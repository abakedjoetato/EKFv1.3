"""
Emerald's Killfeed - Historical Parser (PHASE 2)
Handles full historical data parsing and refresh operations
"""

import asyncio
import logging
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiofiles
import asyncssh
import discord
from discord.ext import commands

from .killfeed_parser import KillfeedParser

logger = logging.getLogger(__name__)

class HistoricalParser:
    """
    HISTORICAL PARSER (FREE)
    - Triggered manually via /server refresh <server_id>
    - Or automatically 30s after /server add
    - Clears PvP data from that server
    - Parses all .csv files in order
    - Updates a single progress embed every 30s in the invoking channel
    - Does not emit killfeed embeds
    """

    def __init__(self, bot):
        self.bot = bot
        self.killfeed_parser = KillfeedParser(bot)
        self.active_refreshes: Dict[str, bool] = {}  # Track active refresh operations

    async def get_all_csv_files(self, server_config: Dict[str, Any]) -> List[str]:
        """Get all CSV files for historical parsing"""
        try:
            if self.bot.dev_mode:
                return await self.get_dev_csv_files()
            else:
                return await self.get_sftp_csv_files(server_config)

        except Exception as e:
            logger.error(f"Failed to get CSV files: {e}")
            return []

    async def get_dev_csv_files(self) -> List[str]:
        """Get all CSV files from dev_data directory"""
        try:
            csv_path = Path('./dev_data/csv')
            csv_files = list(csv_path.glob('*.csv'))

            if not csv_files:
                logger.warning("No CSV files found in dev_data/csv/")
                return []

            all_lines = []

            # Sort files by name (assuming chronological naming)
            csv_files.sort()

            for csv_file in csv_files:
                async with aiofiles.open(csv_file, 'r') as f:
                    content = await f.read()
                    all_lines.extend(content.splitlines())

            return all_lines

        except Exception as e:
            logger.error(f"Failed to read dev CSV files: {e}")
            return []

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create SFTP connection with enhanced error handling and compatibility"""
        try:
            import os

            # Get SFTP credentials with proper fallbacks
            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host') or server_config.get('sftp_host', '')
            sftp_port = int(server_config.get('port') or server_config.get('sftp_port', 22))
            sftp_username = server_config.get('username') or server_config.get('sftp_username', '')
            sftp_password = server_config.get('password') or server_config.get('sftp_password', '')

            # Log connection attempt
            logger.info(f"Attempting SFTP connection to {sftp_host}:{sftp_port} for server {server_id}")

            # Validate credentials
            if not sftp_host:
                logger.error(f"Missing SFTP host for server {server_id}")
                return None

            if not sftp_username or not sftp_password:
                logger.error(f"Missing SFTP credentials for server {server_id}")
                return None

            # Enhanced connection with multiple retry attempts
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    # Configure connection options with legacy support by default
                    options = {
                        'username': sftp_username,
                        'password': sftp_password,
                        'known_hosts': None,  # Skip host key verification
                        'client_keys': None,  # No client keys needed with password auth
                        'preferred_auth': 'password,keyboard-interactive',
                        'kex_algs': [
                            'diffie-hellman-group14-sha256',
                            'diffie-hellman-group16-sha512',
                            'diffie-hellman-group18-sha512',
                            'diffie-hellman-group14-sha1',
                            'diffie-hellman-group1-sha1',
                            'diffie-hellman-group-exchange-sha256',
                            'diffie-hellman-group-exchange-sha1'
                        ],
                        'encryption_algs': [
                            'aes256-ctr', 'aes192-ctr', 'aes128-ctr',
                            'aes256-cbc', 'aes192-cbc', 'aes128-cbc',
                            '3des-cbc', 'blowfish-cbc'
                        ],
                        'mac_algs': [
                            'hmac-sha2-256', 'hmac-sha2-512',
                            'hmac-sha1', 'hmac-md5'
                        ]
                    }

                    # Establish connection with timeout
                    logger.debug(f"Connection attempt {attempt}/{max_retries} to {sftp_host}:{sftp_port}")
                    conn = await asyncio.wait_for(
                        asyncssh.connect(sftp_host, port=sftp_port, **options),
                        timeout=45  # Overall operation timeout
                    )

                    logger.info(f"Successfully connected to SFTP server {sftp_host} for server {server_id}")
                    return conn

                except asyncio.TimeoutError:
                    logger.warning(f"SFTP connection timed out (attempt {attempt}/{max_retries})")
                except asyncssh.DisconnectError as e:
                    logger.warning(f"SFTP server disconnected: {e} (attempt {attempt}/{max_retries})")
                except Exception as e:
                    if 'auth' in str(e).lower():
                        logger.error(f"SFTP authentication failed with provided credentials")
                        # No point retrying with same credentials
                        return None
                    else:
                        logger.warning(f"SFTP connection error: {e} (attempt {attempt}/{max_retries})")
                except asyncssh.Error as e:
                    logger.warning(f"SFTP connection error: {e} (attempt {attempt}/{max_retries})")
                except Exception as e:
                    logger.warning(f"Unexpected error connecting to SFTP: {str(e)} (attempt {attempt}/{max_retries})")

                # Apply exponential backoff between retries
                if attempt < max_retries:
                    delay = 2 ** attempt  # 2, 4, 8 seconds
                    logger.debug(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

            logger.error(f"Failed to connect to SFTP server after {max_retries} attempts")
            return None

        except Exception as e:
            logger.error(f"Failed to get SFTP connection: {e}")
            return None

    async def clear_previous_data(self, guild_id: int, server_id: str):
        """Clear previous entries and reset tracking before historical parsing"""
        try:
            # Clear all PvP data for this server
            await self.bot.db_manager.clear_server_pvp_data(guild_id, server_id)

            # Reset killfeed parser tracking for this server
            server_key = f"{guild_id}_{server_id}"
            if server_key in self.killfeed_parser.parsed_lines:
                self.killfeed_parser.parsed_lines[server_key] = set()
            if server_key in self.killfeed_parser.last_file_position:
                self.killfeed_parser.last_file_position[server_key] = 0

            logger.info(f"Cleared previous data and reset line tracking for server {server_id}")

        except Exception as e:
            logger.error(f"Failed to clear previous data for server {server_id}: {e}")

    async def get_sftp_csv_files(self, server_config: Dict[str, Any]) -> List[str]:
        """Get all CSV files from SFTP server for historical parsing using AsyncSSH"""
        try:
            conn = await self.get_sftp_connection(server_config)
            if not conn:
                return []

            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host')
            # Use consistent path pattern with _id (same as killfeed parser)
            remote_path = f"./{sftp_host}_{server_id}/actual1/deathlogs/"

            all_lines = []

            async with conn.start_sftp_client() as sftp:
                # Enhanced recursive file discovery with robust error handling
                csv_files = []

                pattern = f"{remote_path}**/*.csv"
                logger.info(f"Historical parser searching for CSV files with pattern: {pattern}")

                try:
                    paths = await sftp.glob(pattern)
                    # Use dictionary to track latest version of each unique filename
                    unique_files = {}

                    for path in paths:
                        try:
                            stat_result = await sftp.stat(path)
                            mtime = getattr(stat_result, 'mtime', datetime.now().timestamp())
                            filename = path.split('/')[-1]
                            
                            if filename not in unique_files or mtime > unique_files[filename][1]:
                                unique_files[filename] = (path, mtime)
                                logger.debug(f"Found CSV file: {path}")
                        except Exception as e:
                            logger.warning(f"Error processing CSV file {path}: {e}")
                            
                    # Convert to list
                    csv_files = list(unique_files.values())
                except Exception as e:
                    logger.error(f"Failed to glob files: {e}")

                if not csv_files:
                    logger.warning(f"No CSV files found in {remote_path}")
                    return []

                # Sort by modification time (chronological order for historical parser)
                csv_files.sort(key=lambda x: x[1])

                # Download and read all files with improved robust handling
                logger.info(f"Processing {len(csv_files)} CSV files in chronological order")
                for filepath, timestamp in csv_files:
                    try:
                        # Log file processing start with timestamp
                        readable_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        logger.debug(f"Processing file {filepath} (modified: {readable_time})")

                        # Use chunked reading for large files to prevent memory issues
                        buffer_size = 1024 * 1024  # 1MB buffer
                        file_content = ""

                        async with sftp.open(filepath, 'r') as f:
                            while True:
                                chunk = await f.read(buffer_size)
                                if not chunk:
                                    break

                                # Handle binary data if needed
                                if isinstance(chunk, bytes):
                                    try:
                                        chunk = chunk.decode('utf-8')
                                    except UnicodeDecodeError:
                                        try:
                                            # Try alternative encoding
                                            chunk = chunk.decode('latin-1')
                                        except Exception:
                                            logger.warning(f"Failed to decode content in {filepath}")
                                            continue

                                file_content += chunk

                        # Process file content line by line
                        valid_lines = [line.strip() for line in file_content.splitlines() if line.strip()]
                        logger.debug(f"Found {len(valid_lines)} valid lines in {filepath}")
                        all_lines.extend(valid_lines)

                    except FileNotFoundError:
                        logger.warning(f"CSV file not found: {filepath}")
                    except PermissionError:
                        logger.warning(f"Permission denied reading CSV file: {filepath}")
                    except Exception as e:
                        logger.error(f"Failed to read CSV file {filepath}: {str(e)}")

                logger.info(f"Successfully processed {len(all_lines)} total log lines from {len(csv_files)} files")

                return all_lines

        except Exception as e:
            logger.error(f"Failed to fetch SFTP files for historical parsing: {e}")
            return []

    async def clear_server_data(self, guild_id: int, server_id: str):
        """Clear all PvP data for a server before historical refresh"""
        try:
            # Clear PvP stats
            await self.bot.db_manager.pvp_data.delete_many({
                "guild_id": guild_id,
                "server_id": server_id
            })

            # Clear kill events
            await self.bot.db_manager.kill_events.delete_many({
                "guild_id": guild_id,
                "server_id": server_id
            })

            logger.info(f"Cleared PvP data for server {server_id} in guild {guild_id}")

        except Exception as e:
            logger.error(f"Failed to clear server data: {e}")

    async def update_progress_embed(self, channel: Optional[discord.TextChannel], 
                                   embed_message: discord.Message,
                                   current: int, total: int, server_id: str):
        """Update progress embed every 30 seconds - FIXED INTEGRATION ERROR"""
        try:
            # Safety check - if no channel is provided, just log progress
            if not channel:
                logger.info(f"Progress update for server {server_id}: {current}/{total} events ({(current/total*100) if total > 0 else 0:.1f}%)")
                return

            progress_percent = (current / total * 100) if total > 0 else 0
            progress_bar_length = 20
            filled_length = int(progress_bar_length * current // total) if total > 0 else 0
            progress_bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)

            embed = discord.Embed(
                title="📊 Historical Data Refresh",
                description=f"Refreshing historical data for server **{server_id}**",
                color=0x00FF7F,  # Spring green
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="Progress",
                value=f"```{progress_bar}```\n{current:,} / {total:,} events ({progress_percent:.1f}%)",
                inline=False
            )

            embed.add_field(
                name="Status",
                value="🔄 Processing historical kill events...",
                inline=True
            )

            # FIXED: Remove thumbnail reference to avoid "Unknown Integration" error
            # Thumbnails require file attachments which can't be edited

            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await embed_message.edit(embed=embed)

        except Exception as e:
            logger.error(f"Failed to update progress embed: {e}")

    async def complete_progress_embed(self, embed_message: discord.Message,
                                     server_id: str, processed_count: int, 
                                     duration_seconds: float):
        """Update embed when refresh is complete - FIXED INTEGRATION ERROR"""
        try:
            embed = discord.Embed(
                title="✅ Historical Data Refresh Complete",
                description=f"Successfully refreshed historical data for server **{server_id}**",
                color=0x00FF00,  # Green
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="📈 Results",
                value=f"**{processed_count:,}** kill events processed",
                inline=True
            )

            embed.add_field(
                name="⏱️ Duration", 
                value=f"{duration_seconds:.1f} seconds",
                inline=True
            )

            embed.add_field(
                name="🎯 Status",
                value="Ready for live killfeed tracking",
                inline=False
            )

            # FIXED: Remove thumbnail reference to avoid "Unknown Integration" error
            # Embed edits cannot include file attachments

            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await embed_message.edit(embed=embed)

        except Exception as e:
            logger.error(f"Failed to complete progress embed: {e}")

    async def refresh_server_data(self, guild_id: int, server_config: Dict[str, Any], 
                                 channel: Optional[discord.TextChannel] = None):
        """Refresh historical data for a server"""
        refresh_key = ""
        try:
            server_id = server_config.get('server_id', 'unknown')
            refresh_key = f"{guild_id}_{server_id}"

            # Check if refresh is already running
            if self.active_refreshes.get(refresh_key, False):
                logger.warning(f"Refresh already running for server {server_id}")
                return False

            self.active_refreshes[refresh_key] = True
            start_time = datetime.now()

            logger.info(f"Starting historical refresh for server {server_id} in guild {guild_id}")

            # Send initial progress embed
            embed_message = None
            if channel:
                initial_embed = discord.Embed(
                    title="🚀 Starting Historical Refresh",
                    description=f"Initializing historical data refresh for server **{server_id}**",
                    color=0xFFD700,  # Gold
                    timestamp=datetime.now(timezone.utc)
                )
                initial_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                embed_message = await channel.send(embed=initial_embed)

            # Clear existing data
            await self.clear_server_data(guild_id, server_id)

            # Get all CSV files
            lines = await self.get_all_csv_files(server_config)

            if not lines:
                logger.warning(f"No historical data found for server {server_id}")
                self.active_refreshes[refresh_key] = False
                return False

            total_lines = len(lines)
            processed_count = 0
            last_update_time = datetime.now()

            # Process each line
            for i, line in enumerate(lines):
                if not line.strip():
                    continue

                # Parse kill event (but don't send embeds)
                kill_data = await self.killfeed_parser.parse_csv_line(line)
                if kill_data:
                    # Add to database without sending embeds
                    await self.bot.db_manager.add_kill_event(guild_id, server_id, kill_data)

                    # Update stats using proper MongoDB update syntax
                    # Skip entries with null/empty player names
                    if not kill_data['killer'] or not kill_data['victim']:
                        logger.warning(f"Skipping entry with null player name: {kill_data}")
                        continue

                    if not kill_data['is_suicide']:
                        # Update killer stats atomically
                        await self.bot.db_manager.pvp_data.update_one(
                            {
                                "guild_id": guild_id,
                                "server_id": server_id,
                                "player_name": kill_data['killer']
                            },
                            {
                                "$inc": {"kills": 1},
                                "$setOnInsert": {"deaths": 0, "suicides": 0}
                            },
                            upsert=True
                        )

                    # Update victim stats atomically
                    update_field = "suicides" if kill_data['is_suicide'] else "deaths"
                    await self.bot.db_manager.pvp_data.update_one(
                        {
                            "guild_id": guild_id,
                            "server_id": server_id,
                            "player_name": kill_data['victim']
                        },
                        {
                            "$inc": {update_field: 1},
                            "$setOnInsert": {"kills": 0}
                        },
                        upsert=True
                    )

                    processed_count += 1

                # Update progress embed every 30 seconds
                current_time = datetime.now()
                if embed_message and (current_time - last_update_time).total_seconds() >= 30:
                    await self.update_progress_embed(channel, embed_message, i + 1, total_lines, server_id)
                    last_update_time = current_time

            # Complete the refresh
            duration = (datetime.now() - start_time).total_seconds()

            if embed_message:
                await self.complete_progress_embed(embed_message, server_id, processed_count, duration)

            logger.info(f"Historical refresh completed for server {server_id}: {processed_count} events in {duration:.1f}s")

            self.active_refreshes[refresh_key] = False
            return True

        except Exception as e:
            logger.error(f"Failed to refresh server data: {e}")
            if refresh_key and refresh_key in self.active_refreshes:
                self.active_refreshes[refresh_key] = False
            return False

    async def auto_refresh_after_server_add(self, guild_id: int, server_config: Dict[str, Any]):
        """Automatically refresh data 30 seconds after server is added"""
        try:
            await asyncio.sleep(30)  # Wait 30 seconds
            await self.refresh_server_data(guild_id, server_config)

        except Exception as e:
            logger.error(f"Failed to auto-refresh after server add: {e}")