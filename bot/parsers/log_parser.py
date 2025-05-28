"""
Emerald's Killfeed - Log Parser (PHASE 2)
Parses Deadside.log files for server events (PREMIUM ONLY)
"""

import asyncio
import logging
import json
import re
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import asyncssh
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

class LogParser:
    """
    Enhanced Log Parser with Performance Optimizations
    - Fast parse mode for cold starts
    - Async batch processing with asyncio.gather()
    - Concurrency control with semaphores
    - Embed backlog buffering
    """

    def __init__(self, bot):
        self.bot = bot
        self.sftp_pool: Dict[str, asyncssh.SSHClientConnection] = {}
        self.last_log_position: Dict[str, int] = {}
        self.file_states: Dict[str, Dict[str, Any]] = {}
        self.persistent_state_file = "log_parser_state.json"

        # Performance enhancement features
        self.fast_parse = False  # Toggle for cold start optimization
        self.parse_semaphore = asyncio.BoundedSemaphore(10)  # Concurrency control
        self.embed_backlog: List[Tuple[int, str, Dict[str, Any]]] = []  # Embed buffering

        # Load persistent state on initialization
        asyncio.create_task(self._load_persistent_state())

        # Player tracking dictionaries
        self.player_connections: Dict[str, Dict[str, Any]] = {}
        self.player_sessions: Dict[str, Dict[str, Any]] = {}

        # Event patterns for log parsing
        self.log_patterns = {
            # Player connection events
            'player_world_joined': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerWorldJoined: (.+?) \(ID:(\d+)\) connected from (.+?):(\d+)'),
            'player_queue_left': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerQueueLeft: (.+?) \(ID:(\d+)\) from (.+?):(\d+)'),
            'player_world_connect': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerWorldConnect: (.+?) \(ID:(\d+)\)'),
            'player_world_spawn': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerWorldSpawn: (.+?) \(ID:(\d+)\) at \((.+?), (.+?)\)'),
            'player_online_status': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerOnlineStatus: (.+?) \(ID:(\d+)\) status: (.+?)'),
            'player_session_start': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] PlayerSessionStart: (.+?) \(ID:(\d+)\)'),

            # Mission events
            'mission_start': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] MissionStart: (.+?) at \((.+?), (.+?)\)'),
            'mission_complete': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] MissionComplete: (.+?) completed by (.+?) \(ID:(\d+)\)'),
            'mission_failed': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] MissionFailed: (.+?) failed'),

            # Trader events
            'trader_spawn': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] TraderSpawn: (.+?) spawned at \((.+?), (.+?)\)'),
            'trader_switched': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] TraderSwitched: (.+?) to (.+?)'),
            'trader_available': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] TraderAvailable: (.+?) available for (.+?) minutes'),

            # Vehicle events
            'vehicle_spawn': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] VehicleSpawn: (.+?) spawned at \((.+?), (.+?)\)'),
            'vehicle_delete': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] VehicleDelete: (.+?) deleted'),

            # Airdrop events
            'airdrop_spawn': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] AirdropSpawn: (.+?) at \((.+?), (.+?)\)'),
            'airdrop_looted': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] AirdropLooted: (.+?) looted by (.+?) \(ID:(\d+)\)'),

            # Heli crash events
            'helicrash_spawn': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] HeliCrashSpawn: (.+?) at \((.+?), (.+?)\)'),
            'helicrash_looted': re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] HeliCrashLooted: (.+?) looted by (.+?) \(ID:(\d+)\)')
        }

    def enable_fast_parse(self):
        """Enable fast parse mode for cold start optimization"""
        self.fast_parse = True
        logger.info("Fast parse mode enabled - Discord embeds will be queued")

    def disable_fast_parse(self):
        """Disable fast parse mode and flush any queued embeds"""
        self.fast_parse = False
        logger.info("Fast parse mode disabled")
        if self.embed_backlog:
            asyncio.create_task(self._flush_embed_backlog())

    async def _flush_embed_backlog(self):
        """Flush all queued embeds from backlog"""
        if not self.embed_backlog:
            return

        logger.info(f"Flushing {len(self.embed_backlog)} queued embeds")

        try:
            # Process embeds in batches to avoid overwhelming Discord API
            batch_size = 5
            for i in range(0, len(self.embed_backlog), batch_size):
                batch = self.embed_backlog[i:i + batch_size]
                tasks = []

                for guild_id, server_id, event_data in batch:
                    task = self.send_log_event_embed(guild_id, server_id, event_data)
                    tasks.append(task)

                # Execute batch with small delay between batches
                await asyncio.gather(*tasks, return_exceptions=True)
                if i + batch_size < len(self.embed_backlog):
                    await asyncio.sleep(1)  # Rate limit protection

        except Exception as e:
            logger.error(f"Error flushing embed backlog: {e}")
        finally:
            self.embed_backlog.clear()
            logger.info("Embed backlog flushed")

    async def _load_persistent_state(self):
        """Load persistent state from file"""
        try:
            if os.path.exists(self.persistent_state_file):
                with open(self.persistent_state_file, 'r') as f:
                    state = json.load(f)
                    self.last_log_position = {k: int(v) for k, v in state.get('last_log_position', {}).items()}
                    self.file_states = state.get('file_states', {})
                    logger.info(f"Loaded persistent state: {len(self.last_log_position)} server positions")
        except Exception as e:
            logger.error(f"Error loading persistent state: {e}")

    async def _save_persistent_state(self):
        """Save persistent state to file"""
        try:
            state = {
                'last_log_position': self.last_log_position,
                'file_states': self.file_states,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            with open(self.persistent_state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving persistent state: {e}")

    def schedule_log_parser(self):
        """Schedule the log parser to run every 30 seconds"""
        if hasattr(self.bot, 'scheduler') and self.bot.scheduler:
            self.bot.scheduler.add_job(
                self.run_log_parser,
                'interval',
                seconds=30,
                id='log_parser',
                replace_existing=True
            )
            logger.info("Log parser scheduled to run every 30 seconds")

    async def shutdown(self):
        """Clean shutdown - save state and close connections"""
        try:
            # Save persistent state
            await self._save_persistent_state()
            
            # Close all SFTP connections
            for pool_key, conn in list(self.sftp_pool.items()):
                try:
                    conn.close()
                    logger.debug(f"Closed SFTP connection: {pool_key}")
                except Exception as e:
                    logger.warning(f"Error closing SFTP connection {pool_key}: {e}")
            
            self.sftp_pool.clear()
            logger.info("Log parser shutdown complete - state saved and connections closed")
            
        except Exception as e:
            logger.error(f"Error during log parser shutdown: {e}")

    def get_server_status_key(self, guild_id: int, server_id: str) -> str:
        """Generate unique key for server status tracking"""
        return f"{guild_id}_{server_id}"

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create SFTP connection with enhanced compatibility for legacy SSH servers"""
        try:
            sftp_host = server_config.get('host')
            sftp_port = server_config.get('port', 22)
            sftp_username = server_config.get('username')
            sftp_password = server_config.get('password')

            pool_key = f"{sftp_host}:{sftp_port}:{sftp_username}"

            # Check if connection exists and is still valid
            if pool_key in self.sftp_pool:
                conn = self.sftp_pool[pool_key]
                try:
                    # Test connection with timeout
                    await asyncio.wait_for(conn.run('echo test', check=True), timeout=5)
                    return conn
                except (asyncio.TimeoutError, Exception) as e:
                    # Connection is dead, remove it
                    logger.debug(f"Removing dead SFTP connection: {e}")
                    try:
                        conn.close()
                    except:
                        pass
                    del self.sftp_pool[pool_key]

            # Enhanced SSH connection options for maximum compatibility
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    logger.debug(f"SFTP connection attempt {attempt}/{max_retries} to {sftp_host}:{sftp_port}")
                    
                    # Configure connection with comprehensive compatibility options
                    connection_options = {
                        'username': sftp_username,
                        'password': sftp_password,
                        'known_hosts': None,
                        'client_keys': None,
                        'preferred_auth': 'password,keyboard-interactive',
                        # Comprehensive key exchange algorithms (including legacy)
                        'kex_algs': [
                            'diffie-hellman-group14-sha256',
                            'diffie-hellman-group16-sha512',
                            'diffie-hellman-group18-sha512',
                            'diffie-hellman-group14-sha1',
                            'diffie-hellman-group1-sha1',
                            'diffie-hellman-group-exchange-sha256',
                            'diffie-hellman-group-exchange-sha1',
                            'ecdh-sha2-nistp256',
                            'ecdh-sha2-nistp384',
                            'ecdh-sha2-nistp521',
                            'curve25519-sha256',
                            'curve25519-sha256@libssh.org'
                        ],
                        # Comprehensive encryption algorithms
                        'encryption_algs': [
                            'aes256-ctr', 'aes192-ctr', 'aes128-ctr',
                            'aes256-gcm@openssh.com', 'aes128-gcm@openssh.com',
                            'aes256-cbc', 'aes192-cbc', 'aes128-cbc',
                            '3des-cbc', 'blowfish-cbc',
                            'arcfour256', 'arcfour128', 'arcfour'
                        ],
                        # Comprehensive MAC algorithms
                        'mac_algs': [
                            'hmac-sha2-256-etm@openssh.com',
                            'hmac-sha2-512-etm@openssh.com',
                            'hmac-sha2-256', 'hmac-sha2-512',
                            'hmac-sha1-etm@openssh.com',
                            'hmac-sha1', 'hmac-md5-etm@openssh.com',
                            'hmac-md5', 'hmac-ripemd160-etm@openssh.com',
                            'hmac-ripemd160'
                        ],
                        # Server host key algorithms
                        'server_host_key_algs': [
                            'rsa-sha2-512', 'rsa-sha2-256', 'ssh-rsa',
                            'ecdsa-sha2-nistp256', 'ecdsa-sha2-nistp384',
                            'ecdsa-sha2-nistp521', 'ssh-ed25519',
                            'ssh-dss'
                        ]
                    }

                    # Establish connection with timeout
                    conn = await asyncio.wait_for(
                        asyncssh.connect(sftp_host, port=sftp_port, **connection_options),
                        timeout=30
                    )

                    self.sftp_pool[pool_key] = conn
                    logger.info(f"Successfully created SFTP connection to {sftp_host}:{sftp_port}")
                    return conn

                except asyncio.TimeoutError:
                    logger.warning(f"SFTP connection timed out (attempt {attempt}/{max_retries})")
                except asyncssh.DisconnectError as e:
                    logger.warning(f"SFTP server disconnected: {e} (attempt {attempt}/{max_retries})")
                except (asyncssh.ProtocolError, asyncssh.KeyExchangeFailed) as e:
                    logger.warning(f"SSH protocol/key exchange error: {e} (attempt {attempt}/{max_retries})")
                except asyncssh.PermissionDenied as e:
                    logger.error(f"SFTP authentication failed: {e}")
                    return None  # Don't retry on auth failures
                except ConnectionError as e:
                    logger.warning(f"SFTP connection error: {e} (attempt {attempt}/{max_retries})")
                except Exception as e:
                    error_str = str(e).lower()
                    if any(term in error_str for term in ['auth', 'permission', 'denied', 'password']):
                        logger.error(f"SFTP authentication failed: {e}")
                        return None  # Don't retry on auth failures
                    elif 'invalid dh' in error_str or 'diffie-hellman' in error_str:
                        logger.warning(f"DH parameter error (attempt {attempt}/{max_retries}): {e}")
                    else:
                        logger.warning(f"SFTP connection error: {e} (attempt {attempt}/{max_retries})")

                # Apply exponential backoff between retries
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logger.debug(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)

            logger.error(f"Failed to connect to SFTP server after {max_retries} attempts")
            return None

        except Exception as e:
            logger.error(f"Failed to get SFTP connection: {e}")
            return None

    async def parse_server_logs(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse logs for a single server with enhanced performance"""
        async with self.parse_semaphore:
            server_id = str(server_config.get('_id', 'unknown'))
            logger.debug(f"Parsing logs for server {server_id} in guild {guild_id}")

            try:
                if self.bot.dev_mode:
                    await self.parse_dev_logs(guild_id, server_id)
                else:
                    await self.parse_sftp_logs(guild_id, server_config)
            except Exception as e:
                logger.error(f"Error parsing logs for server {server_id}: {e}")

    async def parse_dev_logs(self, guild_id: int, server_id: str):
        """Parse development logs with enhanced batch processing"""
        try:
            log_path = Path('./dev_data/logs')
            log_files = list(log_path.glob('*.log'))

            if not log_files:
                logger.warning("No log files found in dev_data/logs/")
                return

            # Sort files by modification time, get most recent
            log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            latest_log = log_files[0]

            server_key = self.get_server_status_key(guild_id, server_id)
            last_position = self.last_log_position.get(server_key, 0)

            # Read file content from last position
            with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(last_position)
                content = f.read()
                new_position = f.tell()

            if not content.strip():
                return

            lines = [line.strip() for line in content.splitlines() if line.strip()]

            # Process lines in batches with async gather
            batch_size = 500
            for i in range(0, len(lines), batch_size):
                batch = lines[i:i + batch_size]

                # Parse batch concurrently
                parse_tasks = [self.parse_log_line(line, server_key, guild_id) for line in batch]
                results = await asyncio.gather(*parse_tasks, return_exceptions=True)

                # Process results
                for line, result in zip(batch, results):
                    if isinstance(result, Exception):
                        logger.error(f"Error parsing line: {result}")
                        continue

                    if result:  # Valid event data
                        # Always process database events
                        await self.process_log_event(guild_id, server_id, result)

                        # Handle embeds based on fast_parse mode
                        if self.fast_parse:
                            # Queue embed for later
                            self.embed_backlog.append((guild_id, server_id, result))
                        else:
                            # Send embed immediately
                            if self.should_output_event(result['type'], result.get('data', {})):
                                await self.send_log_event_embed(guild_id, server_id, result)

            # Update position tracking
            self.last_log_position[server_key] = new_position
            await self._save_persistent_state()

        except Exception as e:
            logger.error(f"Error parsing dev logs: {e}")

    async def parse_sftp_logs(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse SFTP logs with enhanced batch processing"""
        try:
            conn = await self.get_sftp_connection(server_config)
            if not conn:
                return

            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host')
            remote_path = f"./{sftp_host}_{server_id}/actual1/logs/"

            async with conn.start_sftp_client() as sftp:
                # Find most recent log file
                try:
                    log_files = await sftp.glob(f"{remote_path}*.log")
                    if not log_files:
                        logger.warning(f"No log files found in {remote_path}")
                        return

                    # Get file stats and find most recent
                    file_stats = []
                    for log_file in log_files:
                        try:
                            stat = await sftp.stat(log_file)
                            file_stats.append((log_file, stat.mtime, stat.size))
                        except Exception as e:
                            logger.warning(f"Error getting stats for {log_file}: {e}")
                            continue

                    if not file_stats:
                        return

                    # Sort by modification time, get most recent
                    file_stats.sort(key=lambda x: x[1], reverse=True)
                    latest_log, mtime, file_size = file_stats[0]

                    server_key = self.get_server_status_key(guild_id, server_id)

                    # Check if this is a new file or file has been reset
                    file_state = self.file_states.get(server_key, {})
                    last_file_path = file_state.get('file_path')
                    last_file_size = file_state.get('file_size', 0)
                    last_position = self.last_log_position.get(server_key, 0)

                    # Handle file reset detection
                    if (latest_log != last_file_path or 
                        file_size < last_file_size or 
                        last_position > file_size):
                        logger.info(f"File reset detected for server {server_id}, starting from beginning")
                        last_position = 0

                    # Update file state tracking
                    self.file_states[server_key] = {
                        'file_path': latest_log,
                        'file_size': file_size,
                        'last_modified': mtime
                    }

                    # Read new content from last position
                    if last_position >= file_size:
                        return  # No new content

                    try:
                        async with sftp.open(latest_log, 'r') as f:
                            f.seek(last_position)
                            content = f.read()
                            new_position = f.tell()
                    except Exception as e:
                        logger.error(f"Error reading log file {latest_log}: {e}")
                        return

                    if not content.strip():
                        return

                    # Handle potential binary content
                    if isinstance(content, bytes):
                        try:
                            content = content.decode('utf-8')
                        except UnicodeDecodeError:
                            content = content.decode('latin-1')

                    lines = [line.strip() for line in content.splitlines() if line.strip()]

                    # Process lines in batches with async gather
                    batch_size = 500
                    for i in range(0, len(lines), batch_size):
                        batch = lines[i:i + batch_size]

                        # Parse batch concurrently
                        parse_tasks = [self.parse_log_line(line, server_key, guild_id) for line in batch]
                        results = await asyncio.gather(*parse_tasks, return_exceptions=True)

                        # Process results
                        for line, result in zip(batch, results):
                            if isinstance(result, Exception):
                                logger.error(f"Error parsing line: {result}")
                                continue

                            if result:  # Valid event data
                                # Always process database events
                                await self.process_log_event(guild_id, server_id, result)

                                # Handle embeds based on fast_parse mode
                                if self.fast_parse:
                                    # Queue embed for later
                                    self.embed_backlog.append((guild_id, server_id, result))
                                else:
                                    # Send embed immediately
                                    if self.should_output_event(result['type'], result.get('data', {})):
                                        await self.send_log_event_embed(guild_id, server_id, result)

                    # Update position tracking
                    self.last_log_position[server_key] = new_position
                    await self._save_persistent_state()

                except Exception as e:
                    logger.error(f"Error processing SFTP logs: {e}")

        except Exception as e:
            logger.error(f"Error in SFTP log parsing: {e}")

    async def parse_log_line(self, line: str, server_key: str, guild_id: int) -> Optional[Dict[str, Any]]:
        """Parse a single log line and extract event data"""
        try:
            for event_type, pattern in self.log_patterns.items():
                match = pattern.match(line)
                if match:
                    return await self.extract_event_data(event_type, match, server_key, guild_id)
            return None
        except Exception as e:
            logger.error(f"Error parsing log line: {e}")
            return None

    async def extract_event_data(self, event_type: str, match, server_key: str, guild_id: int) -> Dict[str, Any]:
        """Extract event data from regex match"""
        try:
            groups = match.groups()
            timestamp_str = groups[0]
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

            event_data = {
                'type': event_type,
                'timestamp': timestamp,
                'raw_line': match.string,
                'data': {}
            }

            # Extract data based on event type
            if event_type in ['player_world_joined', 'player_queue_left']:
                event_data['data'] = {
                    'player_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'connection_id': groups[2] if len(groups) > 2 else 'Unknown',
                    'ip': groups[3] if len(groups) > 3 else 'Unknown',
                    'port': groups[4] if len(groups) > 4 else 'Unknown'
                }

            elif event_type in ['player_world_connect', 'player_session_start']:
                event_data['data'] = {
                    'player_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'connection_id': groups[2] if len(groups) > 2 else 'Unknown'
                }

            elif event_type == 'player_world_spawn':
                event_data['data'] = {
                    'player_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'connection_id': groups[2] if len(groups) > 2 else 'Unknown',
                    'x': float(groups[3]) if len(groups) > 3 else 0.0,
                    'y': float(groups[4]) if len(groups) > 4 else 0.0,
                    'location': f"({groups[3]}, {groups[4]})" if len(groups) > 4 else "(0, 0)"
                }

            elif event_type == 'player_online_status':
                event_data['data'] = {
                    'player_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'connection_id': groups[2] if len(groups) > 2 else 'Unknown',
                    'status': groups[3] if len(groups) > 3 else 'Unknown'
                }

            elif event_type in ['mission_start']:
                event_data['data'] = {
                    'mission_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'x': float(groups[2]) if len(groups) > 2 else 0.0,
                    'y': float(groups[3]) if len(groups) > 3 else 0.0,
                    'location': f"({groups[2]}, {groups[3]})" if len(groups) > 3 else "(0, 0)"
                }

            elif event_type in ['mission_complete']:
                event_data['data'] = {
                    'mission_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'player_name': groups[2] if len(groups) > 2 else 'Unknown',
                    'connection_id': groups[3] if len(groups) > 3 else 'Unknown'
                }

            elif event_type in ['mission_failed']:
                event_data['data'] = {
                    'mission_name': groups[1] if len(groups) > 1 else 'Unknown'
                }

            elif event_type in ['trader_spawn', 'vehicle_spawn', 'airdrop_spawn', 'helicrash_spawn']:
                event_data['data'] = {
                    'item_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'x': float(groups[2]) if len(groups) > 2 else 0.0,
                    'y': float(groups[3]) if len(groups) > 3 else 0.0,
                    'location': f"({groups[2]}, {groups[3]})" if len(groups) > 3 else "(0, 0)"
                }

            elif event_type in ['trader_switched']:
                event_data['data'] = {
                    'from_trader': groups[1] if len(groups) > 1 else 'Unknown',
                    'to_trader': groups[2] if len(groups) > 2 else 'Unknown'
                }

            elif event_type in ['trader_available']:
                event_data['data'] = {
                    'trader_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'duration': groups[2] if len(groups) > 2 else 'Unknown'
                }

            elif event_type in ['vehicle_delete']:
                event_data['data'] = {
                    'vehicle_name': groups[1] if len(groups) > 1 else 'Unknown'
                }

            elif event_type in ['airdrop_looted', 'helicrash_looted']:
                event_data['data'] = {
                    'item_name': groups[1] if len(groups) > 1 else 'Unknown',
                    'player_name': groups[2] if len(groups) > 2 else 'Unknown',
                    'connection_id': groups[3] if len(groups) > 3 else 'Unknown'
                }

            return event_data

        except Exception as e:
            logger.error(f"Error extracting event data: {e}")
            return None

    def should_output_event(self, event_type: str, event_data: Dict[str, Any]) -> bool:
        """Determine if an event should generate Discord output"""
        # Mission events - always output significant missions
        if event_type in ['mission_start', 'mission_complete', 'mission_failed']:
            return True

        # Trader events - output trader spawns and availability
        if event_type in ['trader_spawn', 'trader_switched', 'trader_available']:
            return True

        # Vehicle spawns - disabled for now
        if event_type in ['vehicle_spawn', 'vehicle_delete']:
            return False

        # Player events - output based on significance (connections and disconnections)
        if event_type in ['player_world_connect', 'player_world_spawn', 'player_online_status', 'player_session_start',
                          'player_world_joined', 'player_queue_left']:
            return True

        # Airdrop and helicrash events
        if event_type in ['airdrop_spawn', 'airdrop_looted', 'helicrash_spawn', 'helicrash_looted']:
            return True

        return False

    async def process_log_event(self, guild_id: int, server_id: str, event_data: Dict[str, Any]):
        """Process log event for database storage and player tracking"""
        try:
            event_type = event_data['type']
            data = event_data['data']
            timestamp = event_data['timestamp']

            # Player connection tracking
            if event_type == 'player_world_joined':
                connection_id = data.get('connection_id')
                if connection_id:
                    self.player_connections[connection_id] = {
                        'player_name': data.get('player_name'),
                        'guild_id': guild_id,
                        'server_id': server_id,
                        'connected_at': timestamp,
                        'ip': data.get('ip'),
                        'port': data.get('port')
                    }

            elif event_type == 'player_queue_left':
                connection_id = data.get('connection_id')
                if connection_id in self.player_connections:
                    connection_info = self.player_connections[connection_id]
                    session_duration = timestamp - connection_info['connected_at']

                    # Store session data
                    session_key = f"{guild_id}_{server_id}_{connection_id}_{timestamp.isoformat()}"
                    self.player_sessions[session_key] = {
                        'player_name': connection_info['player_name'],
                        'guild_id': guild_id,
                        'server_id': server_id,
                        'connected_at': connection_info['connected_at'],
                        'disconnected_at': timestamp,
                        'duration': session_duration.total_seconds(),
                        'ip': connection_info['ip']
                    }

                    # Clean up connection tracking
                    del self.player_connections[connection_id]

            # Store event in database
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                await self.bot.db_manager.store_log_event(guild_id, server_id, event_data)

        except Exception as e:
            logger.error(f"Error processing log event: {e}")

    async def send_log_event_embed(self, guild_id: int, server_id: str, event_data: Dict[str, Any]):
        """Send Discord embed for log event"""
        try:
            # Skip if fast parse mode is enabled
            if self.fast_parse:
                return

            event_type = event_data['type']

            # Check if event should generate output
            if not self.should_output_event(event_type, event_data.get('data', {})):
                return

            # Get guild and channel configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            channels = guild_config.get('channels', {})

            # Import EmbedFactory dynamically to avoid circular imports
            from bot.utils.embed_factory import EmbedFactory

            # Determine target channel and build embed
            channel_id = None
            embed = None
            file = None

            embed_data = {
                'timestamp': event_data['timestamp'],
                'server_id': server_id
            }
            embed_data.update(event_data.get('data', {}))

            if event_type in ['player_world_joined']:
                channel_id = channels.get('connections')
                embed, file = await EmbedFactory.build('player_connection', embed_data)

            elif event_type in ['player_queue_left']:
                channel_id = channels.get('disconnections')
                embed, file = await EmbedFactory.build('player_disconnection', embed_data)

            elif event_type in ['mission_start']:
                channel_id = channels.get('missions')
                embed, file = await EmbedFactory.build('mission_start', embed_data)

            elif event_type in ['mission_complete']:
                channel_id = channels.get('missions')
                embed, file = await EmbedFactory.build('mission_complete', embed_data)

            elif event_type in ['trader_spawn']:
                channel_id = channels.get('traders')
                embed, file = await EmbedFactory.build('trader_spawn', embed_data)

            elif event_type in ['airdrop_spawn']:
                channel_id = channels.get('airdrops')
                embed, file = await EmbedFactory.build('airdrop_spawn', embed_data)

            elif event_type in ['helicrash_spawn']:
                channel_id = channels.get('helicrashes')
                embed, file = await EmbedFactory.build('helicrash_spawn', embed_data)

            # Send embed if we have a valid channel and embed
            if channel_id and embed:
                try:
                    guild = self.bot.get_guild(guild_id)
                    if guild:
                        channel = guild.get_channel(channel_id)
                        if channel:
                            if file:
                                await channel.send(embed=embed, file=file)
                            else:
                                await channel.send(embed=embed)
                except Exception as e:
                    logger.error(f"Error sending embed to channel {channel_id}: {e}")

        except Exception as e:
            logger.error(f"Error sending log event embed: {e}")

    def reset_log_positions(self, guild_id: int = None, server_id: str = None):
        """Reset log position tracking for specific server or all servers"""
        if guild_id and server_id:
            # Reset specific server
            server_key = f"{guild_id}_{server_id}"
            if server_key in self.last_log_position:
                del self.last_log_position[server_key]
            if server_key in self.file_states:
                del self.file_states[server_key]
            logger.info(f"Reset log position and file state for server {server_id} in guild {guild_id}")
        else:
            # Reset all positions
            self.last_log_position.clear()
            self.file_states.clear()
            logger.info("Reset all log position tracking and file states")

        # Save state after reset
        asyncio.create_task(self._save_persistent_state())

    async def run_log_parser(self):
        """Main log parser execution with enhanced concurrency control"""
        try:
            logger.info("Starting enhanced log parser execution")

            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.error("Bot database not available for log parsing")
                return

            # Get servers from database using proper database manager
            guilds_cursor = self.bot.db_manager.guilds.find({})
            total_servers_processed = 0

            # Process all guilds
            async for guild_doc in guilds_cursor:
                guild_id = guild_doc['guild_id']
                servers = guild_doc.get('servers', [])

                if not servers:
                    continue

                # Create tasks for all servers in this guild
                server_tasks = []
                for server_config in servers:
                    task = self.parse_server_logs(guild_id, server_config)
                    server_tasks.append(task)

                # Process servers concurrently with semaphore control
                if server_tasks:
                    await asyncio.gather(*server_tasks, return_exceptions=True)
                    total_servers_processed += len(server_tasks)

            logger.info(f"Enhanced log parser completed - processed {total_servers_processed} servers")

            # If fast parse was enabled and we have backlog, consider flushing
            if self.fast_parse and self.embed_backlog:
                logger.info(f"Fast parse mode active with {len(self.embed_backlog)} queued embeds")
                await self._flush_embed_backlog()

        except Exception as e:
            logger.error(f"Failed to run enhanced log parser: {e}")
        finally:
            # Ensure state is saved even if there's an error
            try:
                await self._save_persistent_state()
            except Exception as save_error:
                logger.error(f"Failed to save state during cleanup: {save_error}")