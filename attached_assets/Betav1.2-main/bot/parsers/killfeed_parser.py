"""
Emerald's Killfeed - Killfeed Parser (PHASE 2)
Parses CSV files for kill events and generates embeds
"""

import asyncio
import csv
import logging
import os
import re
import glob
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Any

import aiofiles
import discord
import asyncssh
from discord.ext import commands

logger = logging.getLogger(__name__)

class KillfeedParser:
    """
    KILLFEED PARSER (FREE)
    - Runs every 300 seconds
    - SFTP path: ./{host}_{serverID}/actual1/deathlogs/*/*.csv
    - Loads most recent file only
    - Tracks and skips previously parsed lines
    - Suicides normalized (killer == victim, Suicide_by_relocation → Menu Suicide)
    - Emits killfeed embeds with distance, weapon, styled headers
    """

    def __init__(self, bot):
        self.bot = bot
        self.parsed_lines: Dict[str, Set[str]] = {}  # Track parsed lines per server
        self.last_file_position: Dict[str, int] = {}  # Track file position per server
        self.sftp_pool: Dict[str, asyncssh.SSHClientConnection] = {}  # SFTP connection pool
        self.pool_cleanup_timeout = 300  # 5 minutes idle timeout

    async def parse_csv_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single CSV line into kill event data"""
        try:
            # Expected CSV format: Timestamp;Killer;KillerID;Victim;VictimID;WeaponOrCause;Distance;KillerPlatform;VictimPlatform
            parts = line.strip().split(';')
            if len(parts) < 9:
                return None

            timestamp_str, killer, killer_id, victim, victim_id, weapon, distance, killer_platform, victim_platform = parts[:9]

            # Validate player names
            if not killer or not killer.strip() or not victim or not victim.strip():
                logger.warning(f"Invalid player names in line: {line}")
                return None

            killer = killer.strip()
            victim = victim.strip()

            # Parse timestamp - handle multiple formats
            try:
                # Try format: 2025.04.30-00.16.49
                timestamp = datetime.strptime(timestamp_str, '%Y.%m.%d-%H.%M.%S')
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            except ValueError:
                try:
                    # Try format: 2025-04-30 00:16:49
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                except ValueError:
                    # Fallback to current time
                    timestamp = datetime.utcnow().replace(tzinfo=timezone.utc)

            # Normalize suicide events
            is_suicide = killer == victim or weapon.lower() == 'suicide_by_relocation'
            if is_suicide:
                if weapon.lower() == 'suicide_by_relocation':
                    weapon = 'Menu Suicide'
                elif weapon.lower() == 'falling':
                    weapon = 'Falling'
                    is_suicide = True  # Falling is treated as suicide
                else:
                    weapon = 'Suicide'

            # Parse distance
            try:
                distance_float = float(distance) if distance and distance != 'N/A' else 0.0
            except ValueError:
                distance_float = 0.0

            return {
                'timestamp': timestamp,
                'killer': killer,
                'killer_id': killer_id,
                'victim': victim,
                'victim_id': victim_id,
                'weapon': weapon,
                'distance': distance_float,
                'killer_platform': killer_platform,
                'victim_platform': victim_platform,
                'is_suicide': is_suicide,
                'raw_line': line.strip()
            }

        except Exception as e:
            logger.error(f"Failed to parse CSV line '{line}': {e}")
            return None

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create SFTP connection with pooling"""
        try:
            sftp_host = server_config.get('host')
            sftp_port = server_config.get('port', 22)
            sftp_username = server_config.get('username')
            sftp_password = server_config.get('password')

            if not all([sftp_host, sftp_username, sftp_password]):
                logger.warning(f"SFTP credentials not configured for server {server_config.get('_id', 'unknown')}")
                return None

            pool_key = f"{sftp_host}:{sftp_port}:{sftp_username}"

            # Check if connection exists and is still valid
            if pool_key in self.sftp_pool:
                conn = self.sftp_pool[pool_key]
                try:
                    if not conn.is_closed():
                        return conn
                    else:
                        del self.sftp_pool[pool_key]
                except Exception:
                    del self.sftp_pool[pool_key]

            # Create new connection with retry/backoff
            for attempt in range(3):
                try:
                    conn = await asyncio.wait_for(
                        asyncssh.connect(
                            sftp_host, 
                            username=sftp_username, 
                            password=sftp_password, 
                            port=sftp_port, 
                            known_hosts=None,
                            server_host_key_algs=['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512'],
                            kex_algs=['diffie-hellman-group14-sha256', 'diffie-hellman-group16-sha512', 'ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521'],
                            encryption_algs=['aes128-ctr', 'aes192-ctr', 'aes256-ctr', 'aes128-gcm@openssh.com', 'aes256-gcm@openssh.com'],
                            mac_algs=['hmac-sha2-256', 'hmac-sha2-512', 'hmac-sha1']
                        ),
                        timeout=30
                    )
                    self.sftp_pool[pool_key] = conn
                    logger.info(f"Created SFTP connection to {sftp_host}")
                    return conn

                except (asyncio.TimeoutError, asyncssh.Error) as e:
                    logger.warning(f"SFTP connection attempt {attempt + 1} failed: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff

            return None

        except Exception as e:
            logger.error(f"Failed to get SFTP connection: {e}")
            return None

    async def get_sftp_csv_files(self, server_config: Dict[str, Any]) -> List[str]:
        """Get CSV files from SFTP server using AsyncSSH with connection pooling"""
        try:
            conn = await self.get_sftp_connection(server_config)
            if not conn:
                return []

            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host')
            # Fix directory resolution logic to correctly combine host and _id into path
            remote_path = f"./{sftp_host}_{server_id}/actual1/deathlogs/"
            logger.info(f"Using SFTP CSV path: {remote_path} for server {server_id} on host {sftp_host}")

            async with conn.start_sftp_client() as sftp:
                csv_files = []
                # Use consistent path pattern
                pattern = f"./{sftp_host}_{server_id}/actual1/deathlogs/**/*.csv"
                logger.info(f"Searching for CSV files with pattern: {pattern}")

                try:
                    paths = await sftp.glob(pattern)
                    # Track unique paths to prevent duplicates
                    seen_paths = set()

                    for path in paths:
                        if path not in seen_paths:
                            try:
                                stat_result = await sftp.stat(path)
                                mtime = getattr(stat_result, 'mtime', datetime.now().timestamp())
                                csv_files.append((path, mtime))
                                seen_paths.add(path)
                                logger.debug(f"Found CSV file: {path}")
                            except Exception as e:
                                logger.warning(f"Error processing CSV file {path}: {e}")
                except Exception as e:
                    logger.error(f"Failed to glob files: {e}")

                if not csv_files:
                    logger.warning(f"No CSV files found in {remote_path}")
                    return []

                # Sort by modification time, get most recent
                csv_files.sort(key=lambda x: x[1], reverse=True)
                most_recent_file = csv_files[0][0]

                # Read file content
                try:
                    async with sftp.open(most_recent_file, 'r') as f:
                        file_content = await f.read()
                        return [line.strip() for line in file_content.splitlines() if line.strip()]
                except Exception as e:
                    logger.error(f"Failed to read CSV file {most_recent_file}: {e}")
                    return []

        except Exception as e:
            logger.error(f"Failed to fetch SFTP CSV files: {e}")
            return []

    async def get_dev_csv_files(self) -> List[str]:
        """Get CSV files from attached_assets and dev_data directories for testing"""
        try:
            # Check attached_assets first
            attached_csv = Path('./attached_assets/2025.04.30-00.00.00.csv')
            if attached_csv.exists():
                async with aiofiles.open(attached_csv, 'r') as f:
                    content = await f.read()
                    return [line.strip() for line in content.splitlines() if line.strip()]

            # Fallback to dev_data
            csv_path = Path('./dev_data/csv')
            if csv_path.exists():
                csv_files = list(csv_path.glob('*.csv'))
                if csv_files:
                    most_recent = max(csv_files, key=lambda f: f.stat().st_mtime)
                    async with aiofiles.open(most_recent, 'r') as f:
                        content = await f.read()
                        return [line.strip() for line in content.splitlines() if line.strip()]

            logger.warning("No CSV files found in attached_assets or dev_data/csv/")
            return []

        except Exception as e:
            logger.error(f"Failed to read dev CSV files: {e}")
            return []

    async def process_kill_event(self, guild_id: int, server_id: str, kill_data: Dict[str, Any]):
        """Process a kill event and update database with proper streak and distance tracking"""
        try:
            # Add kill event to database
            await self.bot.db_manager.add_kill_event(guild_id, server_id, kill_data)

            if kill_data['is_suicide']:
                # Handle suicide - reset streak and increment suicide count
                logger.debug(f"Processing suicide for {kill_data['victim']} in server {server_id}")

                # Reset victim's current streak to 0 and increment suicides
                await self.bot.db_manager.update_pvp_stats(
                    guild_id, server_id, kill_data['victim'],
                    {"suicides": 1}
                )
                # Reset streak separately
                await self.bot.db_manager.reset_player_streak(guild_id, server_id, kill_data['victim'])

            else:
                # Handle actual PvP kill - proper streak and distance tracking
                logger.debug(f"Processing kill: {kill_data['killer']} -> {kill_data['victim']} in server {server_id}")

                # Update killer: increment kills and streak
                await self.bot.db_manager.increment_player_kill(
                    guild_id, server_id, kill_data['killer'], kill_data.get('distance', 0)
                )

                # Update victim: increment deaths and reset streak
                await self.bot.db_manager.increment_player_death(
                    guild_id, server_id, kill_data['victim']
                )

            # Send killfeed embed using EmbedFactory
            await self.send_killfeed_embed(guild_id, kill_data)

        except Exception as e:
            logger.error(f"Failed to process kill event: {e}")

    async def send_killfeed_embed(self, guild_id: int, kill_data: Dict[str, Any]):
        """Send killfeed embed to designated channel using EmbedFactory"""
        try:
            from ..utils.embed_factory import EmbedFactory

            # Get guild configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            killfeed_channel_id = guild_config.get('channels', {}).get('killfeed')
            if not killfeed_channel_id:
                return

            channel = self.bot.get_channel(killfeed_channel_id)
            if not channel:
                return

            # Get player stats for KDR display
            killer_stats = None
            victim_stats = None

            if not kill_data['is_suicide']:
                killer_stats = await self.bot.db_manager.get_pvp_stats(guild_id, "default", kill_data['killer'])
                victim_stats = await self.bot.db_manager.get_pvp_stats(guild_id, "default", kill_data['victim'])

            # Prepare embed data based on death type
            weapon = kill_data['weapon']

            if kill_data['is_suicide']:
                # Check if it's a falling death
                if 'falling' in weapon.lower() or 'fall' in weapon.lower():
                    embed_type = 'fall'
                    embed_data = {
                        'player_name': kill_data['victim'],
                        'faction': None,  # Could be added later if faction data available
                        'thumbnail_url': 'attachment://Falling.png'
                    }
                else:
                    embed_type = 'suicide'
                    embed_data = {
                        'player_name': kill_data['victim'],
                        'cause': weapon,
                        'faction': None,  # Could be added later if faction data available
                        'thumbnail_url': 'attachment://Suicide.png'
                    }
            else:
                # Regular kill embed
                embed_type = 'killfeed'
                embed_data = {
                    'killer_name': kill_data['killer'],
                    'victim_name': kill_data['victim'],
                    'killer_kdr': f"{killer_stats.get('kdr', 0.0):.2f}" if killer_stats else "0.00",
                    'victim_kdr': f"{victim_stats.get('kdr', 0.0):.2f}" if victim_stats else "0.00",
                    'weapon': weapon,
                    'distance': f"{kill_data.get('distance', 0):.0f}",
                    'thumbnail_url': 'attachment://Killfeed.png'
                }

            # Build embed using EmbedFactory
            embed, file_attachment = await EmbedFactory.build(embed_type, embed_data)

            # Send embed with file attachment if needed
            if file_attachment:
                await channel.send(embed=embed, file=file_attachment)
            else:
                await channel.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to send killfeed embed: {e}")

    async def parse_server_killfeed(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse killfeed for a single server"""
        try:
            server_id = str(server_config.get('_id', 'unknown'))
            logger.info(f"Parsing killfeed for server {server_id} in guild {guild_id}")

            # Get CSV lines
            if self.bot.dev_mode:
                lines = await self.get_dev_csv_files()
            else:
                lines = await self.get_sftp_csv_files(server_config)

            if not lines:
                logger.warning(f"No CSV data found for server {server_id}")
                return

            # Track processed lines for this server
            server_key = f"{guild_id}_{server_id}"
            if server_key not in self.parsed_lines:
                self.parsed_lines[server_key] = set()

            new_events = 0

            for line in lines:
                if not line.strip() or line in self.parsed_lines[server_key]:
                    continue

                kill_data = await self.parse_csv_line(line)
                if kill_data:
                    await self.process_kill_event(guild_id, server_id, kill_data)
                    self.parsed_lines[server_key].add(line)
                    new_events += 1

            logger.info(f"Processed {new_events} new kill events for server {server_id}")

        except Exception as e:
            logger.error(f"Failed to parse killfeed for server {server_config}: {e}")

    async def run_killfeed_parser(self):
        """Run killfeed parser for all configured servers"""
        try:
            logger.info("Running killfeed parser...")

            # Get all guilds with configured servers
            guilds_cursor = self.bot.db_manager.guilds.find({})

            async for guild_doc in guilds_cursor:
                guild_id = guild_doc['guild_id']
                servers = guild_doc.get('servers', [])

                for server_config in servers:
                    await self.parse_server_killfeed(guild_id, server_config)

            logger.info("Killfeed parser completed")

        except Exception as e:
            logger.error(f"Failed to run killfeed parser: {e}")

    def schedule_killfeed_parser(self):
        """Schedule killfeed parser to run every 300 seconds"""
        try:
            self.bot.scheduler.add_job(
                self.run_killfeed_parser,
                'interval',
                seconds=300,  # 5 minutes
                id='killfeed_parser',
                replace_existing=True
            )
            logger.info("Killfeed parser scheduled (every 300 seconds)")

        except Exception as e:
            logger.error(f"Failed to schedule killfeed parser: {e}")

    async def cleanup_sftp_connections(self):
        """Clean up idle SFTP connections"""
        try:
            for pool_key, conn in list(self.sftp_pool.items()):
                if conn._transport.is_closing() or not conn.is_client():
                    del self.sftp_pool[pool_key]
                    logger.info(f"Cleaned up stale SFTP connection: {pool_key}")
        except Exception as e:
            logger.error(f"Failed to cleanup SFTP connections: {e}")