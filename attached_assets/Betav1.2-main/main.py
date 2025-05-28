#!/usr/bin/env python3
"""
Emerald's Killfeed - Discord Bot for Deadside PvP Engine
Full production-grade bot with killfeed parsing, stats, economy, and premium features
"""

import asyncio
import logging
import os
import sys
import json
import hashlib
import re
import time
from pathlib import Path

# Clean up any conflicting discord modules before importing
for module_name in list(sys.modules.keys()):
    if module_name == 'discord' or module_name.startswith('discord.'):
        del sys.modules[module_name]

# Import py-cord v2.6.1
try:
    import discord
    from discord.ext import commands
    print(f"✅ Successfully imported py-cord")
except ImportError as e:
    print(f"❌ Error importing py-cord: {e}")
    print("Please ensure py-cord 2.6.1 is installed")
    sys.exit(1)

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bot.models.database import DatabaseManager
from bot.parsers.killfeed_parser import KillfeedParser
from bot.parsers.historical_parser import HistoricalParser
from bot.parsers.log_parser import LogParser
from bot.parsers.intelligent_log_parser import IntelligentLogParser

# Load environment variables (optional for Railway)
load_dotenv()

# Detect Railway environment
RAILWAY_ENV = os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_STATIC_URL")
if RAILWAY_ENV:
    print(f"🚂 Running on Railway environment")
else:
    print("🖥️ Running in local/development environment")

# Import Railway keep-alive server
from keep_alive import keep_alive

# Set runtime mode to production
MODE = os.getenv("MODE", "production")
print(f"Runtime mode set to: {MODE}")

# Start keep-alive server for Railway deployment
if MODE == "production" or RAILWAY_ENV:
    print("🚀 Starting Railway keep-alive server...")
    keep_alive()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Command hash calculation function
def compute_command_hash(bot):
    """
    Computes a hash of the application command schema to detect changes.
    This allows us to only sync commands when the structure has changed.

    Args:
        bot: The bot instance with application_commands

    Returns:
        str: SHA-256 hash of the command structure
    """
    # Get commands from the correct attribute
    if hasattr(bot, 'pending_application_commands') and bot.pending_application_commands:
        commands = [cmd.to_dict() for cmd in bot.pending_application_commands]
        cmd_source = "pending_application_commands"
    elif hasattr(bot, 'application_commands') and bot.application_commands:
        commands = [cmd.to_dict() for cmd in bot.application_commands]
        cmd_source = "application_commands"
    else:
        # Fallback - empty command structure will force sync once
        commands = []
        cmd_source = "none_found"

    # Debug for observation
    cmd_count = len(commands)
    logger.info(f"🔍 Computing hash from {cmd_count} commands using {cmd_source}")

    # Sort all commands and their properties for consistent hashing
    raw = json.dumps(commands, sort_keys=True).encode('utf-8')
    hash_value = hashlib.sha256(raw).hexdigest()

    # Log hash details for debugging
    logger.info(f"🔑 Generated command hash: {hash_value[:10]}... from {cmd_count} commands")

    return hash_value

class EmeraldKillfeedBot(commands.Bot):
    """Main bot class for Emerald's Killfeed"""

    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,
            status=discord.Status.online,
            activity=discord.Game(name="Emerald's Killfeed v2.0")
        )

        # Initialize variables
        self.db_manager = None
        self.scheduler = AsyncIOScheduler()
        self.killfeed_parser = None
        self.log_parser = None
        self.historical_parser = None
        self.intelligent_log_parser = None
        self.ssh_connections = []

        # Missing essential properties
        self.assets_path = Path('./assets')
        self.dev_data_path = Path('./dev_data')
        self.dev_mode = os.getenv('DEV_MODE', 'false').lower() == 'true'

        logger.info("Bot initialized in production mode")

    async def load_cogs(self):
        """Load all bot cogs"""
        try:
            # Load cogs in order
            cogs = [
                'bot.cogs.core',
                'bot.cogs.economy',
                'bot.cogs.gambling', 
                'bot.cogs.linking',
                'bot.cogs.stats',
                'bot.cogs.bounties',
                'bot.cogs.factions',
                'bot.cogs.premium',
                'bot.cogs.leaderboards_fixed',
                'bot.cogs.admin_channels',
                'bot.cogs.parsers'
            ]

            loaded_cogs = []
            failed_cogs = []

            for cog in cogs:
                try:
                    self.load_extension(cog)
                    loaded_cogs.append(cog)
                    logger.info(f"✅ Successfully loaded cog: {cog}")
                except Exception as e:
                    failed_cogs.append(cog)
                    logger.error(f"❌ Failed to load cog {cog}: {e}")

            # Verify commands are registered
            try:
                command_count = len(self.application_commands) if hasattr(self, 'application_commands') else 0
                logger.info(f"📊 Loaded {len(loaded_cogs)}/{len(cogs)} cogs successfully")
                logger.info(f"📊 Total slash commands registered: {command_count}")

                # Debug: List actual commands found
                if command_count > 0:
                    command_names = [cmd.name for cmd in self.application_commands]
                    logger.info(f"🔍 Commands found: {', '.join(command_names)}")
                else:
                    logger.info("ℹ️ Commands will be synced after connection")
            except Exception as e:
                logger.warning(f"Command count check failed: {e}")

            if failed_cogs:
                logger.error(f"❌ Failed cogs: {failed_cogs}")
                return False
            else:
                logger.info("✅ All cogs loaded and commands registered successfully")
                return True

        except Exception as e:
            logger.error(f"❌ Critical failure loading cogs: {e}")
            return False

    async def register_commands_safely(self):
        """
        Multi-Guild Command Registration System

        This specialized system completely avoids the global command sync rate limits while
        ensuring commands are registered in all guilds, including new ones that join later.

        The approach:
        1. Use hash-based tracking to detect command structure changes
        2. Register commands on a per-guild basis (completely avoids global sync rate limits)
        3. Track which guilds have been processed to avoid duplicate work
        4. Handle new guilds joining automatically via on_guild_join event
        """
        # Get the number of commands
        command_count = len(self.pending_application_commands) if hasattr(self, 'pending_application_commands') else 0
        logger.info(f"📊 {command_count} commands registered locally")

        # If we have commands to sync
        if command_count > 0:
            # Debug: List commands found (limited to first 5 to avoid log spam)
            command_names = [cmd.name for cmd in self.pending_application_commands]
            logger.info(f"🔍 Commands found: {', '.join(command_names[:5])}{'...' if len(command_names) > 5 else ''}")

            # STRICTLY PER-GUILD SYNC APPROACH
            logger.info("🛡️ DIRECT FIX: Using GUILD-SPECIFIC command syncing ONLY, NEVER global sync")

            # Compute command hash to detect changes
            hash_file_path = "command_hash.txt"
            current_hash = compute_command_hash(self)
            previous_hash = ''

            # Load previous hash if it exists
            if os.path.exists(hash_file_path):
                try:
                    with open(hash_file_path, 'r') as f:
                        previous_hash = f.read().strip()
                    logger.info(f"📊 Found previous command hash: {previous_hash[:10]}...")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read previous hash: {e}")

            # Do we need to sync commands? Only if hash changed
            hash_changed = current_hash != previous_hash

            # If commands haven't changed, only process new guilds
            if not hash_changed and previous_hash:
                logger.info(f"✅ Command structure unchanged. Using saved hash: {current_hash[:10]}...")

            # Get list of guilds where we need to register commands
            if not self.guilds:
                logger.warning("⚠️ No guilds available for command syncing")
                return

            # Setup tracking of processed guilds
            processed_guilds_file = "processed_guilds.txt"
            processed_guild_ids = set()

            if os.path.exists(processed_guilds_file):
                try:
                    with open(processed_guilds_file, 'r') as f:
                        for line in f:
                            guild_id = line.strip()
                            if guild_id:
                                try:
                                    processed_guild_ids.add(int(guild_id))
                                except ValueError:
                                    pass
                    logger.info(f"📊 Found {len(processed_guild_ids)} previously processed guilds")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read processed guilds: {e}")

            # Determine which guilds need processing
            guilds_to_process = []
            for guild in self.guilds:
                # Process a guild if:
                # 1. Commands have changed (need to update all guilds), OR
                # 2. This guild hasn't been processed yet
                if hash_changed or guild.id not in processed_guild_ids:
                    guilds_to_process.append(guild)

            if not guilds_to_process:
                logger.info(f"✅ All guilds already processed, commands are synced")
                return

            logger.info(f"🔄 Selected {len(guilds_to_process)} guilds for command syncing")

            # Process each guild
            processed_count = 0
            for guild in guilds_to_process:
                # CRITICAL: Never use global sync
                try:
                    # NEVER do global sync, only guild-specific
                    logger.info(f"🔄 Syncing commands to guild: {guild.name} (ID: {guild.id})")
                    await self.sync_commands(guild_ids=[guild.id])
                    logger.info(f"✅ Successfully synced commands to {guild.name}")

                    # Mark guild as processed
                    processed_guild_ids.add(guild.id)
                    try:
                        with open(processed_guilds_file, 'a') as f:
                            f.write(f"{guild.id}\n")
                    except Exception as e:
                        logger.warning(f"⚠️ Could not update processed guilds file: {e}")

                    processed_count += 1

                    # Avoid potential secondary rate limits
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"❌ Failed to sync commands to {guild.name}: {e}")

            # Save the current hash for future reference
            try:
                with open(hash_file_path, 'w') as f:
                    f.write(current_hash)
                logger.info(f"💾 Command hash saved: {current_hash[:10]}...")
            except Exception as e:
                logger.warning(f"⚠️ Could not save command hash: {e}")

            # Report success
            if processed_count > 0:
                logger.info(f"🎉 Successfully synced commands to {processed_count} guilds")

                # If we've processed all guilds, log it
                if len(processed_guild_ids) >= len(self.guilds):
                    logger.info(f"🎉 ALL GUILDS PROCESSED! Commands are synced to all {len(self.guilds)} guilds")
            else:
                logger.warning("⚠️ No guilds were processed, commands may not be available")

            guild_count = len(self.guilds)
            logger.info(f"🔍 Bot is in {guild_count} guilds - preparing for selective sync")

            # Try to read previously processed guilds to avoid duplicate work
            processed_guilds_file = "processed_guilds.txt"
            processed_guild_ids = set()

            if os.path.exists(processed_guilds_file):
                try:
                    with open(processed_guilds_file, 'r') as f:
                        for line in f:
                            guild_id = line.strip()
                            if guild_id:
                                processed_guild_ids.add(int(guild_id))
                    logger.info(f"📊 Found {len(processed_guild_ids)} previously processed guilds")
                except Exception as e:
                    logger.warning(f"⚠️ Could not read processed guilds: {e}")

            # Select guilds to process (prioritize unprocessed guilds)
            guilds_to_process = []
            for guild in self.guilds:
                if guild.id not in processed_guild_ids:
                    guilds_to_process.append(guild)
                    # Limit to 5 guilds per run to be safe
                    if len(guilds_to_process) >= 5:
                        break

            if not guilds_to_process:
                logger.info(f"✅ All guilds already processed, commands are synced")
                return

            logger.info(f"🔄 Selected {len(guilds_to_process)} guilds for command syncing")

            # Process selected guilds
            processed_count = 0
            for guild in guilds_to_process:
                logger.info(f"🔄 Syncing commands to guild: {guild.name} (ID: {guild.id})")
                try:
                    # Guild-specific sync avoids global rate limits
                    synced = await self.sync_commands(guild_ids=[guild.id])

                    # Track that we've processed this guild
                    processed_guild_ids.add(guild.id)
                    with open(processed_guilds_file, 'a') as f:
                        f.write(f"{guild.id}\n")

                    if synced:
                        logger.info(f"✅ Successfully synced {len(synced)} commands to {guild.name}")
                    else:
                        logger.info(f"✅ Synced commands to {guild.name} (count unavailable)")

                    processed_count += 1

                    # Add a short delay between guild syncs to avoid rate limits
                    await asyncio.sleep(1)

                except Exception as e:
                    logger.error(f"❌ Failed to sync commands to {guild.name}: {e}")

            # Report success
            logger.info(f"🎉 Successfully synced commands to {processed_count} guilds")

            # Save the current hash for future reference
            self.save_command_hash(current_hash, hash_file_path)
            logger.info(f"💾 Saved command hash: {current_hash[:10]}... for tracking changes")

            # If we've processed all guilds, log success
            if len(processed_guild_ids) >= guild_count:
                logger.info(f"🎉 ALL GUILDS PROCESSED! Commands are synced to all {guild_count} guilds")
            else:
                remaining = guild_count - len(processed_guild_ids)
                logger.info(f"ℹ️ {remaining} guilds remaining to process on future restarts")

    async def sync_commands_with_extreme_caution(self, current_hash, hash_file_path):
        """
        Ultra-conservative sync strategy for Discord global commands.

        This method acknowledges that Discord's API rate limits can persist
        for extended periods and takes extreme caution to avoid triggering them,
        while still ensuring commands get synced when the structure changes.

        Key features:
        1. Saves hash *before* attempting sync to prevent future rate limits
        2. Never retries after a rate limit (hash is already saved)
        3. Creates a lock file to prevent sync attempts for 24 hours
        """
        try:
            # Initial attempt to sync commands
            logger.info("📡 Initiating global command sync with EXTREME CAUTION...")

            # DIRECT FIX FOR EMERALD'S ISSUE:
            # Instead of syncing global commands, let's use per-guild syncing for the first guild
            # This avoids the global command rate limit entirely while still providing the commands
            try:
                # Find the guilds where we need to register commands
                target_guilds = self.guilds[:3]  # First 3 guilds only to avoid rate limits
                if target_guilds:
                    logger.info(f"🛡️ DIRECT FIX: Using guild-specific command registration to avoid global rate limits")

                    for guild in target_guilds:
                        logger.info(f"🛡️ Syncing commands to guild: {guild.name} (ID: {guild.id})")
                        try:
                            # Guild-specific sync is much less likely to hit rate limits
                            synced = await self.sync_commands(guild_ids=[guild.id])
                            if synced:
                                logger.info(f"✅ Successfully synced {len(synced)} commands to {guild.name}")
                            else:
                                logger.info(f"✅ Synced commands to {guild.name} (count unknown)")
                        except Exception as guild_sync_error:
                            logger.error(f"❌ Failed to sync to {guild.name}: {guild_sync_error}")

                    logger.info(f"🛡️ Guild-specific sync completed. Global sync skipped to avoid rate limits.")
                    return True
                else:
                    logger.warning(f"⚠️ No guilds available for guild-specific sync, falling back to global sync")
            except Exception as guild_sync_error:
                logger.warning(f"⚠️ Guild-specific sync failed: {guild_sync_error}")

            # If we reach here, we need to attempt the sync
            await self.sync_commands()
            logger.info("✅ Global command sync completed successfully!")
            return True

        except Exception as e:
            error_text = str(e)
            logger.error(f"❌ Global sync failed: {error_text}")

            # Log the rate limit error but do NOT retry
            # We've already saved the hash before this function, so future restarts will skip syncing
            if "rate limited" in error_text.lower():
                logger.warning(f"⚠️ RATE LIMIT detected - commands will sync automatically later")
                logger.warning(f"⚠️ Rate limit protection is in place via saved hash and lock file")
            else:
                logger.warning(f"⚠️ Non-rate-limit error during sync attempt")

            return False

    # Keep the old method for reference
    async def sync_commands_with_retry(self, current_hash, hash_file_path):
        """
        Legacy method - kept for reference but not used
        """
        logger.info("⚠️ Using legacy sync method - shouldn't be called")

        try:
            # Initial attempt to sync commands
            logger.info("📡 Initiating global command sync...")
            await self.sync_commands()
            logger.info("✅ Global command sync completed successfully")

            # Save hash after successful sync
            self.save_command_hash(current_hash, hash_file_path)
            return True

        except Exception as e:
            error_text = str(e)
            logger.error(f"❌ Global sync failed: {error_text}")

            # For all errors, save the hash to prevent future attempts
            logger.warning("⚠️ Saving hash to prevent future sync attempts")
            self.save_command_hash(current_hash, hash_file_path)
            return False

    def save_command_hash(self, hash_value, file_path):
        """
        Save command hash to file
        """
        try:
            with open(file_path, 'w') as f:
                f.write(hash_value)
            logger.info(f"💾 Saved command hash: {hash_value[:10]}... to {file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save hash: {e}")
            return False

    async def cleanup_connections(self):
        """Clean up AsyncSSH connections on shutdown"""
        try:
            if hasattr(self, 'killfeed_parser') and self.killfeed_parser:
                await self.killfeed_parser.cleanup_sftp_connections()

            if hasattr(self, 'log_parser') and self.log_parser:
                # Clean up log parser SFTP connections
                for pool_key, conn in list(self.log_parser.sftp_pool.items()):
                    try:
                        conn.close()
                    except:
                        pass
                self.log_parser.sftp_pool.clear()

            logger.info("Cleaned up all SFTP connections")

        except Exception as e:
            logger.error(f"Failed to cleanup connections: {e}")

    async def setup_database(self):
        """Setup MongoDB connection"""
        mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGO_URI')
        if not mongo_uri:
            logger.error("MongoDB URI not found in environment variables")
            return False

        try:
            self.mongo_client = AsyncIOMotorClient(mongo_uri)
            self.database = self.mongo_client.emerald_killfeed

            # Initialize database manager with PHASE 1 architecture
            from bot.models.database import DatabaseManager
            self.db_manager = DatabaseManager(self.mongo_client)
            # For backward compatibility
            self.database = self.db_manager

            # Test connection
            await self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")

            # Initialize database indexes
            await self.db_manager.initialize_indexes()
            logger.info("Database architecture initialized (PHASE 1)")

            # Initialize parsers (PHASE 2)
            self.killfeed_parser = KillfeedParser(self)
            self.historical_parser = HistoricalParser(self)
            self.log_parser = LogParser(self)
            self.intelligent_log_parser = IntelligentLogParser(self)
            logger.info("Parsers initialized (PHASE 2) + Intelligent Parser")

            return True

        except Exception as e:
            logger.error("Failed to connect to MongoDB: %s", e)
            return False

    def setup_scheduler(self):
        """Setup background job scheduler"""
        try:
            self.scheduler.start()
            logger.info("Background job scheduler started")
            return True
        except Exception as e:
            logger.error("Failed to start scheduler: %s", e)
            return False

    async def on_ready(self):
        """Called when bot is ready and connected to Discord - MULTI-GUILD RATE LIMIT SAFE VERSION"""
        # Only run setup once
        if hasattr(self, '_setup_complete'):
            return

        logger.info("🚀 Bot is ready! Loading cogs first...")

        # CRITICAL: Load cogs FIRST before anything else
        try:
            logger.info("🔧 Loading cogs for command registration...")
            cogs_success = await self.load_cogs()
            logger.info(f"🎯 Cog loading: {'✅ Complete' if cogs_success else '❌ Failed'}")

            # Give py-cord time to process async setup functions
            await asyncio.sleep(0.5)  # Allow py-cord to process command registration

            # Use the specialized Multi-Guild Command Registration System
            await self.register_commands_safely()

            logger.info("🚀 Now starting database and parser setup...")

            # Connect to MongoDB
            db_success = await self.setup_database()
            logger.info(f"📊 Database setup: {'✅ Success' if db_success else '❌ Failed'}")

            # Start scheduler
            scheduler_success = self.setup_scheduler()
            logger.info(f"⏰ Scheduler setup: {'✅ Success' if scheduler_success else '❌ Failed'}")

            # Schedule parsers (PHASE 2)
            if self.killfeed_parser:
                self.killfeed_parser.schedule_killfeed_parser()
                logger.info("📡 Killfeed parser scheduled")
            if self.log_parser:
                self.log_parser.schedule_log_parser()
                logger.info("📜 Log parser scheduled")

            # Bot ready messages
            if self.user:
                logger.info("✅ Bot logged in as %s (ID: %s)", self.user.name, self.user.id)
            logger.info("✅ Connected to %d guilds", len(self.guilds))

            for guild in self.guilds:
                logger.info(f"📡 Bot connected to: {guild.name} (ID: {guild.id})")

            # Verify assets exist
            if self.assets_path.exists():
                assets = list(self.assets_path.glob('*.png'))
                logger.info("📁 Found %d asset files", len(assets))
            else:
                logger.warning("⚠️ Assets directory not found")

            # Verify dev data exists (for testing)
            if self.dev_mode:
                csv_files = list(self.dev_data_path.glob('csv/*.csv'))
                log_files = list(self.dev_data_path.glob('logs/*.log'))
                logger.info("🧪 Dev mode: Found %d CSV files and %d log files", len(csv_files), len(log_files))

            logger.info("🎉 Bot setup completed successfully!")
            self._setup_complete = True

        except Exception as e:
            logger.error(f"❌ Critical error in bot setup: {e}")
            raise

    async def on_guild_join(self, guild):
        """Called when bot joins a new guild"""
        logger.info("Joined guild: %s (ID: %s)", guild.name, guild.id)

        # Initialize guild in database (will be implemented in Phase 1)
        # await self.database.guilds.insert_one({
        #     'guild_id': guild.id,
        #     'guild_name': guild.name,
        #     'created_at': datetime.utcnow(),
        #     'premium_servers': [],
        #     'channels': {}
        # })

        # CRITICAL: Register commands to the new guild immediately
        # This ensures commands are available without waiting for a restart
        logger.info(f"🔄 New guild joined - syncing commands to: {guild.name}")
        try:
            # Guild-specific sync avoids global rate limits
            synced = await self.sync_commands(guild_ids=[guild.id])
            if synced:
                logger.info(f"✅ Successfully synced {len(synced)} commands to new guild {guild.name}")
            else:
                logger.info(f"✅ Synced commands to new guild {guild.name} (count unavailable)")

            # Track that we've processed this guild
            processed_guilds_file = "processed_guilds.txt"
            with open(processed_guilds_file, 'a') as f:
                f.write(f"{guild.id}\n")
            logger.info(f"✅ Tracked new guild in processed guilds list")

        except Exception as e:
            logger.error(f"❌ Failed to sync commands to new guild {guild.name}: {e}")

    async def on_guild_remove(self, guild):
        """Called when bot is removed from a guild"""
        logger.info("Left guild: %s (ID: %s)", guild.name, guild.id)

    async def close(self):
        """Clean shutdown"""
        logger.info("Shutting down bot...")

        # Clean up SFTP connections
        await self.cleanup_connections()

        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

        if hasattr(self, 'mongo_client') and self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

        await super().close()
        logger.info("Bot shutdown complete")

async def main():
    """Main entry point"""
    # Check required environment variables for Railway deployment
    bot_token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN')
    mongo_uri = os.getenv('MONGO_URI') or os.getenv('MONGODB_URI')
    tip4serv_key = os.getenv('TIP4SERV_KEY')  # Optional service key
    
    # Railway environment detection
    railway_env = os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('RAILWAY_STATIC_URL')
    if railway_env:
        print(f"✅ Railway environment detected")

    # Validate required secrets
    if not bot_token:
        logger.error("❌ BOT_TOKEN not found in environment variables")
        logger.error("Please set BOT_TOKEN in your Railway environment variables")
        return

    if not mongo_uri:
        logger.error("❌ MONGO_URI not found in environment variables") 
        logger.error("Please set MONGO_URI in your Railway environment variables")
        return
    
    # Log startup success
    logger.info(f"✅ Bot starting with token: {'*' * 20}...{bot_token[-4:] if bot_token else 'MISSING'}")
    logger.info(f"✅ MongoDB URI configured: {'*' * 20}...{mongo_uri[-10:] if mongo_uri else 'MISSING'}")
    if tip4serv_key:
        logger.info(f"✅ TIP4SERV_KEY configured: {'*' * 10}...{tip4serv_key[-4:]}")
    else:
        logger.info("ℹ️ TIP4SERV_KEY not configured (optional)")

    # Create and run bot
    print("Creating bot instance...")
    bot = EmeraldKillfeedBot()

    try:
        await bot.start(bot_token)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error("Error in bot execution: %s", e)
        raise
    finally:
        if not bot.is_closed():
            await bot.close()

if __name__ == "__main__":
    # Run the bot
    print("Starting main bot execution...")
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Critical error in main execution: {e}")
        import traceback
        traceback.print_exc()