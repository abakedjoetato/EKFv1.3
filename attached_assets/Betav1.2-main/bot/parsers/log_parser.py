"""
Emerald's Killfeed - Log Parser (PHASE 2)
Parses Deadside.log files for server events (PREMIUM ONLY)
"""

import asyncio
import logging
import os
import re
import glob
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Set

import aiofiles
import discord
import asyncssh
from discord.ext import commands
from .connection_parser import ConnectionLifecycleParser

logger = logging.getLogger(__name__)

class LogParser:
    """
    LOG PARSER (PREMIUM ONLY)
    - Runs every 300 seconds
    - SFTP path: ./{host}_{serverID}/Logs/Deadside.log
    - Detects: Player joins/disconnects, Queue sizes, Airdrops, missions, traders, crashes
    - Detects log rotation
    - Sends styled embeds to respective channels
    """

    def __init__(self, bot):
        self.bot = bot
        self.last_log_position: Dict[str, int] = {}  # Track file position per server
        self.log_patterns = self._compile_log_patterns()
        self.player_sessions: Dict[str, Dict[str, Any]] = {}  # Track player join times for playtime rewards
        self.server_status: Dict[str, Dict[str, Any]] = {}  # Track real-time server status per guild_server
        self.sftp_pool: Dict[str, asyncssh.SSHClientConnection] = {}  # SFTP connection pool
        self.log_file_hashes: Dict[str, str] = {}  # Track log file rotation
        self.player_lifecycle: Dict[str, Dict[str, Any]] = {}  # Track comprehensive player lifecycle
        
        # PLAYER CONNECTION LIFECYCLE TRACKING - Initialize new system
        self.connection_parser = ConnectionLifecycleParser(bot)

    def _compile_log_patterns(self) -> Dict[str, re.Pattern]:
        """Compile robust regex patterns for complete player connection lifecycle tracking"""
        return {
            # PLAYER CONNECTION LIFECYCLE EVENTS (4 Core Events)
            
            # 1. Queue Join (jq) - Player enters queue
            'queue_join': re.compile(r'LogNet: Join request: /Game/Maps/world_0/World_0\?.*\?Name=([^&\s]+).*(?:platformid=PS5:(\w+)|eosid=\|(\w+))', re.IGNORECASE),
            
            # 2. Player Joined (j2) - Player successfully connects
            'player_joined': re.compile(r'LogOnline: Warning: Player \|(\w+) successfully registered!', re.IGNORECASE),
            
            # 3. Disconnect Post-Join (d1) - Standard disconnect after joining
            'disconnect_post_join': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|(\w+)', re.IGNORECASE),
            
            # 4. Disconnect Pre-Join (d2) - Disconnect from queue before joining  
            'disconnect_pre_join': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: (?:PS5|EOS):\|?(\w+)', re.IGNORECASE),
            
            # Phase 4: Disconnection Tracking
            'player_disconnect_cleanup': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*UChannel::CleanUp.*Connection.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_session_end': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*LogOnline.*Session.*(?:ended|closed|terminated).*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_beacon_disconnect': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*UChannel::CleanUp.*Beacon.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_network_disconnect': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*NetConnection.*closed.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            
            # Phase 5: Queue Management & Failures
            'player_queue_timeout': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Connection.*timeout.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_queue_failed': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Failed.*connection.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_auth_failed': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Authentication.*failed.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            
            # Legacy patterns for backward compatibility
            'player_queue_join': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*NotifyAcceptingConnection.*accepted.*from:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_beacon_connected': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*NotifyAcceptedConnection.*RemoteAddr:\s*([\d\.]+):(\d+).*UniqueId:\s*([A-Z]+:\|\w+)', re.IGNORECASE),
            'player_world_connect': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:NotifyAcceptedConnection.*Name:\s*World_0|World_0.*Join).*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_queue_disconnect': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*UChannel::CleanUp.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            
            # ENHANCED CONNECTION PATTERNS - Better detection for player count tracking
            'player_accepted_from': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*NotifyAcceptingConnection.*accepted.*from:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_connection_cleanup': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*UChannel::CleanUp.*Connection.*RemoteAddr:\s*([\d\.]+):(\d+)', re.IGNORECASE),
            'player_beacon_join': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*BeaconHost.*accept.*from:\s*([\d\.]+):(\d+)', re.IGNORECASE),

            # MISSION EVENTS - Comprehensive patterns to catch all mission variations
            'mission_ready': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Mission\s+(GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*switched\s+to\s+READY', re.IGNORECASE),
            'mission_waiting': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Mission\s+(GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*switched\s+to\s+WAITING', re.IGNORECASE),
            'mission_initial': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Mission\s+(GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*switched\s+to\s+INITIAL', re.IGNORECASE),
            'mission_respawn': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Mission\s+(GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*will\s+respawn\s+in\s+(\d+)', re.IGNORECASE),
            
            # Additional mission patterns to catch variations
            'mission_state_any': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Mission\s+(GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*switched\s+to\s+([A-Z_]+)', re.IGNORECASE),

            # ENCOUNTER EVENTS
            'encounter_initial': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Encounter\s+(GA_[A-Za-z0-9_]+).*switched\s+to\s+INITIAL.*respawn\s+in\s+(\d+)', re.IGNORECASE),

            # PATROL POINT EVENTS
            'patrol_switch': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*PatrolPoint\s+([A-Za-z0-9_]+).*switched\s+to\s+([A-Z.]+)(?:.*monsters\s+(\d+))?', re.IGNORECASE),

            # VEHICLE EVENTS - Enhanced detection
            'vehicle_spawn': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:CanSpawnVehicle|NewVehicle|Vehicle.*spawn).*(?:NewVehicles\s+(\d+)|Max\s+(\d+)|spawned|deployed)', re.IGNORECASE),
            'vehicle_delete': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:NewVehicle_Del|Vehicle.*(?:delete|remove|destroy)).*(?:Del\s+vehicle\s+([A-Za-z0-9_]+)|([A-Za-z0-9_]+))', re.IGNORECASE),

            # HELICOPTER CRASH EVENTS - Enhanced patterns
            'helicrash_initial': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:Heli.*crash|Helicopter.*crash|HeliCrash).*(?:INITIAL|initiated|spawned)', re.IGNORECASE),
            'helicrash_spawned': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*HeliCrash.*spawned.*(?:X=([\d\.-]+).*Y=([\d\.-]+))?', re.IGNORECASE),
            'helicrash_switched': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*HeliCrash.*switched.*to.*INITIAL', re.IGNORECASE),

            # AIRDROP EVENTS - Enhanced patterns  
            'airdrop_flying': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:Airdrop|Air.*drop).*(?:flying|in.*air|deployed)', re.IGNORECASE),
            'airdrop_switched': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*AirDrop.*switched.*to.*(?:Flying|Waiting)', re.IGNORECASE),

            # TRADER EVENTS - Enhanced patterns
            'trader_spawn': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Trader.*(?:spawn|appear|initial).*(?:X=([\d\.-]+).*Y=([\d\.-]+))?', re.IGNORECASE),
            'trader_switched': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Trader.*switched.*to.*(?:INITIAL|Active)', re.IGNORECASE),
            'trader_available': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*Trader.*(?:available|ready|active)', re.IGNORECASE),

            # CONSTRUCTION SAVES - Detect but suppress output
            'construction_save': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:LogSFPSConstruction|Construction).*Save.*constructibles\s+(\d+).*([0-9.]+)ms', re.IGNORECASE),

            # SERVER CONFIGURATION
            'server_max_players': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*playersmaxcount=(\d+)', re.IGNORECASE),

            # GENERIC FALLBACK PATTERNS for better coverage
            'generic_mission': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:Mission|GA_[A-Za-z0-9_]*_Mis_?[A-Za-z0-9_]*).*(?:READY|WAITING|INITIAL|respawn)', re.IGNORECASE),
            'generic_vehicle': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:Vehicle|NewVehicle).*(?:spawn|delete|Del)', re.IGNORECASE),
            'generic_player': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\].*(?:NotifyAccept|UChannel|World_0|RemoteAddr)', re.IGNORECASE)
        }

    def normalize_mission_name(self, raw_mission_name: str) -> str:
        """Normalize mission names for consistency with comprehensive mappings"""
        mission_mappings = {
            # Military Bases
            'GA_Military_03_Mis_01': 'Military Base Alpha',
            'GA_Military_04_Mis1': 'Military Base Bravo', 
            'GA_Military_04_Mis_2': 'Military Base Charlie',
            'GA_Military_02_mis1': 'Military Outpost Delta',
            'GA_Military_05_Mis_1': 'Military Base Echo',
            'GA_Military_01_Mis_1': 'Military Base Foxtrot',
            'GA_Military_06_Mis_1': 'Military Base Golf',
            'GA_Military_07_Mis_01': 'Military Base Hotel',
            'GA_Military_Mis_1': 'Military Base India',
            'GA_Military_Mis_01': 'Military Base Juliet',
            'GA_Military_Mis_02': 'Military Base Kilo',
            
            # Industrial Zones
            'GA_Ind_02_Mis_1': 'Industrial Complex Alpha',
            'GA_Ind_01_Mis_1': 'Industrial Complex Beta',
            'GA_Ind_03_Mis_1': 'Industrial Complex Gamma',
            'GA_Ind_Mis_1': 'Industrial Complex Delta',
            'GA_Ind_Mis_01': 'Industrial Complex Echo',
            'GA_PromZone_Mis_01': 'Industrial Zone Beta',
            'GA_PromZone_Mis_02': 'Industrial Zone Gamma',
            'GA_PromZone_Mis_1': 'Industrial Zone Delta',
            'GA_KhimMash_Mis_01': 'Chemical Plant Alpha',
            'GA_KhimMash_Mis_02': 'Chemical Plant Beta',
            'GA_KhimMash_Mis_1': 'Chemical Plant Gamma',
            
            # Settlements
            'GA_Bochki_Mis_1': 'Bochki Settlement',
            'GA_Bochki_Mis_01': 'Bochki Settlement Alpha',
            'GA_Krasnoe_Mis_1': 'Krasnoe Settlement',
            'GA_Krasnoe_Mis_01': 'Krasnoe Settlement Alpha',
            'GA_Dubovoe_0_Mis_1': 'Dubovoe Settlement',
            'GA_Dubovoe_Mis_1': 'Dubovoe Settlement Alpha',
            'GA_Settle_09_Mis_1': 'Northern Settlement',
            'GA_Settle_05_ChernyLog_mis1': 'Cherny Log Settlement',
            'GA_Settle_Mis_1': 'Eastern Settlement',
            'GA_Settle_Mis_01': 'Western Settlement',
            'GA_Beregovoy_mis1': 'Beregovoy Settlement',
            'GA_Beregovoy_Mis_1': 'Beregovoy Settlement Alpha',
            
            # Resource Sites
            'GA_Sawmill_03_Mis_01': 'Sawmill Complex Alpha',
            'GA_Sawmill_01_mis1': 'Sawmill Complex Beta',
            'GA_Sawmill_02_Mis_1': 'Sawmill Complex Gamma',
            'GA_Sawmill_Mis_1': 'Sawmill Complex Delta',
            'GA_Sawmill_Mis_01': 'Sawmill Complex Echo',
            'GA_Lighthouse_02_mis1': 'Lighthouse Compound',
            'GA_Lighthouse_Mis_1': 'Lighthouse Compound Alpha',
            'GA_Lighthouse_Mis_01': 'Lighthouse Compound Beta',
            'GA_Bunker_01_mis1': 'Underground Bunker',
            'GA_Bunker_Mis_1': 'Underground Bunker Alpha',
            'GA_Bunker_Mis_01': 'Underground Bunker Beta',
            
            # Special Locations
            'GA_Airport_mis_01_Enc2': 'Airport Terminal',
            'GA_Airport_Mis_1': 'Airport Terminal Alpha',
            'GA_Airport_Mis_01': 'Airport Terminal Beta',
            'GA_Voron_Enc_1': 'Voron Stronghold',
            'GA_Voron_Mis_1': 'Voron Stronghold Alpha',
            'GA_Hospital_Mis_1': 'Medical Facility',
            'GA_Hospital_Mis_01': 'Medical Facility Alpha',
            'GA_School_Mis_1': 'Abandoned School',
            'GA_School_Mis_01': 'Abandoned School Alpha',
            'GA_Factory_Mis_1': 'Manufacturing Plant',
            'GA_Factory_Mis_01': 'Manufacturing Plant Alpha',
            
            # Additional common patterns
            'GA_Town_Mis_1': 'Town Center',
            'GA_Town_Mis_01': 'Town Center Alpha',
            'GA_Base_Mis_1': 'Forward Base',
            'GA_Base_Mis_01': 'Forward Base Alpha',
            'GA_Outpost_Mis_1': 'Remote Outpost',
            'GA_Outpost_Mis_01': 'Remote Outpost Alpha',
            'GA_Camp_Mis_1': 'Field Camp',
            'GA_Camp_Mis_01': 'Field Camp Alpha'
        }

        # If exact match found, return it
        if raw_mission_name in mission_mappings:
            return mission_mappings[raw_mission_name]
        
        # Try to extract meaningful parts for fallback
        clean_name = raw_mission_name.replace('GA_', '').replace('_Mis_', ' ').replace('_mis', ' ')
        clean_name = clean_name.replace('_01', '').replace('_02', '').replace('_03', '')
        clean_name = clean_name.replace('_1', '').replace('_2', '').replace('_3', '')
        clean_name = clean_name.replace('_Enc', ' Encounter')
        
        # Convert to title case and clean up
        return clean_name.replace('_', ' ').title()

    def normalize_vehicle_name(self, raw_vehicle_name: str) -> str:
        """Normalize vehicle names for better display"""
        if not raw_vehicle_name or raw_vehicle_name == 'Unknown':
            return 'Military Vehicle'
            
        vehicle_mappings = {
            'BP_Vehicle_Car_01_C': 'Civilian Car',
            'BP_Vehicle_Car_02_C': 'Sports Car',
            'BP_Vehicle_Car_03_C': 'Off-Road Vehicle',
            'BP_Vehicle_Truck_01_C': 'Cargo Truck',
            'BP_Vehicle_Truck_02_C': 'Military Truck',
            'BP_Vehicle_APC_01_C': 'Armored Personnel Carrier',
            'BP_Vehicle_Helicopter_01_C': 'Transport Helicopter',
            'BP_Vehicle_Helicopter_02_C': 'Attack Helicopter',
            'BP_Vehicle_Bike_01_C': 'Motorcycle',
            'BP_Vehicle_Quad_01_C': 'ATV Quad Bike',
            'BP_Vehicle_Boat_01_C': 'Patrol Boat',
            'BP_Vehicle_Boat_02_C': 'Speed Boat'
        }
        
        # Check for exact match
        if raw_vehicle_name in vehicle_mappings:
            return vehicle_mappings[raw_vehicle_name]
        
        # Extract meaningful parts for fallback
        clean_name = raw_vehicle_name.replace('BP_Vehicle_', '').replace('_C', '').replace('_01', '').replace('_02', '')
        return clean_name.replace('_', ' ').title() if clean_name else 'Military Vehicle'

    def get_connection_key(self, guild_id: int, server_id: str, ip: str, port: str) -> str:
        """Generate unique key for tracking connection lifecycle"""
        return f"{guild_id}_{server_id}_{ip}_{port}"

    async def track_player_lifecycle_event(self, guild_id: int, server_id: str, ip: str, port: str, 
                                         event_type: str, timestamp: datetime, additional_data: Dict = None):
        """Track comprehensive player lifecycle events"""
        connection_key = self.get_connection_key(guild_id, server_id, ip, port)
        
        # Initialize connection tracking if not exists
        if connection_key not in self.player_lifecycle:
            self.player_lifecycle[connection_key] = {
                'guild_id': guild_id,
                'server_id': server_id,
                'ip': ip,
                'port': port,
                'connection_id': f"{ip}:{port}",
                'current_state': None,
                'state_history': [],
                'first_seen': timestamp,
                'last_updated': timestamp,
                'unique_id': None,
                'session_duration': None,
                'is_active': False
            }

        lifecycle = self.player_lifecycle[connection_key]
        
        # Determine new state based on event type
        new_state = self._map_event_to_state(event_type)
        
        if new_state:
            # Update lifecycle state
            old_state = lifecycle['current_state']
            lifecycle['current_state'] = new_state
            lifecycle['last_updated'] = timestamp
            lifecycle['state_history'].append({
                'state': new_state,
                'timestamp': timestamp,
                'event_type': event_type,
                'duration_from_previous': (timestamp - lifecycle['last_updated']).total_seconds() if old_state else 0
            })

            # Handle specific state transitions
            if new_state == 'BEACON_AUTHENTICATED' and additional_data and 'unique_id' in additional_data:
                lifecycle['unique_id'] = additional_data['unique_id']
            
            elif new_state == 'WORLD_CONNECTED':
                lifecycle['is_active'] = True
                # Start session tracking for playtime rewards
                await self.track_player_join(guild_id, server_id, f"{ip}:{port}", timestamp)
                # Update server player count
                await self.track_player_successful_join(guild_id, server_id, f"{ip}:{port}", timestamp)
                
            elif new_state in ['DISCONNECTED', 'FAILED', 'TIMEOUT']:
                lifecycle['is_active'] = False
                lifecycle['session_duration'] = (timestamp - lifecycle['first_seen']).total_seconds()
                # End session tracking and award playtime
                if old_state == 'WORLD_CONNECTED' or lifecycle.get('was_online', False):
                    await self.track_player_disconnect(guild_id, server_id, f"{ip}:{port}", timestamp)
                # Update server player count
                await self.track_player_disconnect_or_failed_join(guild_id, server_id, f"{ip}:{port}", timestamp)
                
            logger.debug(f"Player lifecycle {connection_key}: {old_state} -> {new_state} ({event_type})")

    def _map_event_to_state(self, event_type: str) -> Optional[str]:
        """Map log event types to lifecycle states"""
        state_mapping = {
            'player_queue_request': 'QUEUE_REQUESTED',
            'player_queue_accepted': 'QUEUE_ACCEPTED',
            'player_beacon_handshake': 'BEACON_HANDSHAKE',
            'player_beacon_auth': 'BEACON_AUTHENTICATED',
            'player_world_auth': 'WORLD_AUTHENTICATING',
            'player_world_spawn': 'WORLD_CONNECTED',
            'player_online_status': 'ONLINE_ACTIVE',
            'player_session_start': 'ONLINE_ACTIVE',
            'player_character_spawn': 'ONLINE_ACTIVE',
            'player_disconnect_cleanup': 'DISCONNECTING',
            'player_session_end': 'DISCONNECTED',
            'player_beacon_disconnect': 'DISCONNECTED',
            'player_network_disconnect': 'DISCONNECTED',
            'player_queue_timeout': 'TIMEOUT',
            'player_queue_failed': 'FAILED',
            'player_auth_failed': 'FAILED',
            # Legacy mappings
            'player_queue_join': 'QUEUE_ACCEPTED',
            'player_beacon_connected': 'BEACON_AUTHENTICATED',
            'player_world_connect': 'WORLD_CONNECTED',
            'player_queue_disconnect': 'DISCONNECTED'
        }
        return state_mapping.get(event_type)

    async def get_active_players_count(self, guild_id: int, server_id: str) -> int:
        """Get accurate count of active players using lifecycle tracking"""
        active_count = 0
        current_time = datetime.now(timezone.utc)
        
        for connection_key, lifecycle in self.player_lifecycle.items():
            if (lifecycle['guild_id'] == guild_id and 
                lifecycle['server_id'] == server_id and 
                lifecycle['is_active'] and 
                lifecycle['current_state'] in ['WORLD_CONNECTED', 'ONLINE_ACTIVE']):
                
                # Check if connection is still considered active (within last 10 minutes)
                time_since_update = (current_time - lifecycle['last_updated']).total_seconds()
                if time_since_update < 600:  # 10 minutes
                    active_count += 1
                else:
                    # Mark as inactive if no activity for 10+ minutes
                    lifecycle['is_active'] = False
                    
        return active_count

    async def cleanup_old_lifecycle_data(self, max_age_hours: int = 24):
        """Clean up old lifecycle tracking data"""
        current_time = datetime.now(timezone.utc)
        keys_to_remove = []
        
        for connection_key, lifecycle in self.player_lifecycle.items():
            age_hours = (current_time - lifecycle['last_updated']).total_seconds() / 3600
            if age_hours > max_age_hours:
                keys_to_remove.append(connection_key)
        
        for key in keys_to_remove:
            del self.player_lifecycle[key]
            
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} old player lifecycle entries")

    async def track_player_join(self, guild_id: int, server_id: str, player_name: str, timestamp: datetime):
        """Track player join for playtime rewards"""
        session_key = f"{guild_id}_{server_id}_{player_name}"
        self.player_sessions[session_key] = {
            'join_time': timestamp,
            'guild_id': guild_id,
            'server_id': server_id,
            'player_name': player_name
        }

    async def track_player_disconnect(self, guild_id: int, server_id: str, player_name: str, timestamp: datetime):
        """Track player disconnect and award playtime economy points"""
        session_key = f"{guild_id}_{server_id}_{player_name}"

        if session_key in self.player_sessions:
            join_time = self.player_sessions[session_key]['join_time']
            playtime_minutes = (timestamp - join_time).total_seconds() / 60

            # Award economy points (1 point per minute, minimum 5 minutes)
            if playtime_minutes >= 5:
                points_earned = int(playtime_minutes)

                # Find Discord user by character name
                discord_id = await self._find_discord_user_by_character(guild_id, player_name)
                if discord_id:
                    # Get currency name for this guild
                    currency_name = await self._get_guild_currency_name(guild_id)

                    # Award playtime points
                    await self.bot.get_cog('Economy').add_wallet_event(
                        guild_id, discord_id, points_earned, 
                        'playtime', f'Online time: {int(playtime_minutes)} minutes'
                    )

            # Remove from tracking
            del self.player_sessions[session_key]

    async def _find_discord_user_by_character(self, guild_id: int, character_name: str) -> Optional[int]:
        """Find Discord user ID by character name"""
        try:
            # Search through all players in the guild for this character
            cursor = self.bot.db_manager.players.find({'guild_id': guild_id})
            async for player_doc in cursor:
                if character_name in player_doc.get('linked_characters', []):
                    return player_doc.get('discord_id')
            return None
        except Exception:
            return None

    async def _get_guild_currency_name(self, guild_id: int) -> str:
        """Get custom currency name for guild or default"""
        try:
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                return 'Emeralds'
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            return guild_config.get('currency_name', 'Emeralds') if guild_config else 'Emeralds'
        except Exception:
            return 'Emeralds'

    def get_server_status_key(self, guild_id: int, server_id: str) -> str:
        """Generate server status tracking key"""
        return f"{guild_id}_{server_id}"

    async def init_server_status(self, guild_id: int, server_id: str, server_name: Optional[str] = None):
        """Initialize server status tracking"""
        status_key = self.get_server_status_key(guild_id, server_id)
        self.server_status[status_key] = {
            'guild_id': guild_id,
            'server_id': server_id,
            'server_name': server_name or server_id,
            'current_players': 0,
            'max_players': 50,  # Default, will be updated from log
            'queue_count': 0,
            'queued_players': set(),
            'online_players': set(),
            'last_updated': datetime.now(timezone.utc)
        }

    async def update_server_max_players(self, guild_id: int, server_id: str, max_players: int):
        """Update server max player count from log"""
        status_key = self.get_server_status_key(guild_id, server_id)

        if status_key not in self.server_status:
            await self.init_server_status(guild_id, server_id)

        self.server_status[status_key]['max_players'] = max_players
        await self.update_voice_channel_name(guild_id, server_id)

    async def track_player_queued(self, guild_id: int, server_id: str, player_name: str, queue_position: int):
        """Track player entering queue"""
        status_key = self.get_server_status_key(guild_id, server_id)

        if status_key not in self.server_status:
            await self.init_server_status(guild_id, server_id)

        # Add to queued players
        self.server_status[status_key]['queued_players'].add(player_name)
        self.server_status[status_key]['queue_count'] = len(self.server_status[status_key]['queued_players'])
        self.server_status[status_key]['last_updated'] = datetime.now(timezone.utc)

        await self.update_voice_channel_name(guild_id, server_id)

    async def track_player_successful_join(self, guild_id: int, server_id: str, player_name: str, timestamp: datetime):
        """Track successful player join (from queue to online)"""
        status_key = self.get_server_status_key(guild_id, server_id)

        if status_key not in self.server_status:
            await self.init_server_status(guild_id, server_id)

        # Remove from queue, add to online
        self.server_status[status_key]['queued_players'].discard(player_name)
        self.server_status[status_key]['online_players'].add(player_name)

        # Update counts
        self.server_status[status_key]['current_players'] = len(self.server_status[status_key]['online_players'])
        self.server_status[status_key]['queue_count'] = len(self.server_status[status_key]['queued_players'])
        self.server_status[status_key]['last_updated'] = datetime.now(timezone.utc)

        # Start playtime tracking
        await self.track_player_join(guild_id, server_id, player_name, timestamp)

        await self.update_voice_channel_name(guild_id, server_id)

    async def track_player_disconnect_or_failed_join(self, guild_id: int, server_id: str, player_name: str, timestamp: datetime):
        """Track player disconnect or failed join"""
        status_key = self.get_server_status_key(guild_id, server_id)

        if status_key not in self.server_status:
            await self.init_server_status(guild_id, server_id)

        # Remove from both queue and online (handles both disconnect and failed join)
        was_online = player_name in self.server_status[status_key]['online_players']

        self.server_status[status_key]['queued_players'].discard(player_name)
        self.server_status[status_key]['online_players'].discard(player_name)

        # Update counts
        self.server_status[status_key]['current_players'] = len(self.server_status[status_key]['online_players'])
        self.server_status[status_key]['queue_count'] = len(self.server_status[status_key]['queued_players'])
        self.server_status[status_key]['last_updated'] = datetime.now(timezone.utc)

        # Award playtime if they were online
        if was_online:
            await self.track_player_disconnect(guild_id, server_id, player_name, timestamp)

        await self.update_voice_channel_name(guild_id, server_id)

    async def get_comprehensive_server_stats(self, guild_id: int, server_id: str) -> Dict[str, Any]:
        """Get comprehensive server statistics using lifecycle tracking"""
        current_time = datetime.now(timezone.utc)
        stats = {
            'active_players': 0,
            'queued_players': 0,
            'connecting_players': 0,
            'total_connections_today': 0,
            'failed_connections_today': 0,
            'average_session_duration': 0,
            'peak_players_today': 0,
            'connection_success_rate': 0
        }
        
        # Calculate statistics from lifecycle data
        active_connections = []
        queued_connections = []
        connecting_connections = []
        todays_connections = []
        failed_connections = []
        session_durations = []
        
        for connection_key, lifecycle in self.player_lifecycle.items():
            if lifecycle['guild_id'] == guild_id and lifecycle['server_id'] == server_id:
                # Check if connection is from today
                is_today = lifecycle['first_seen'].date() == current_time.date()
                if is_today:
                    todays_connections.append(lifecycle)
                
                # Categorize current state
                current_state = lifecycle['current_state']
                if current_state in ['WORLD_CONNECTED', 'ONLINE_ACTIVE'] and lifecycle['is_active']:
                    # Check if still considered active
                    time_since_update = (current_time - lifecycle['last_updated']).total_seconds()
                    if time_since_update < 600:  # 10 minutes
                        active_connections.append(lifecycle)
                elif current_state in ['QUEUE_REQUESTED', 'QUEUE_ACCEPTED']:
                    queued_connections.append(lifecycle)
                elif current_state in ['BEACON_HANDSHAKE', 'BEACON_AUTHENTICATED', 'WORLD_AUTHENTICATING']:
                    connecting_connections.append(lifecycle)
                elif current_state in ['FAILED', 'TIMEOUT'] and is_today:
                    failed_connections.append(lifecycle)
                
                # Collect session durations for completed sessions
                if lifecycle.get('session_duration'):
                    session_durations.append(lifecycle['session_duration'])
        
        # Calculate final statistics
        stats['active_players'] = len(active_connections)
        stats['queued_players'] = len(queued_connections)
        stats['connecting_players'] = len(connecting_connections)
        stats['total_connections_today'] = len(todays_connections)
        stats['failed_connections_today'] = len(failed_connections)
        
        if session_durations:
            stats['average_session_duration'] = sum(session_durations) / len(session_durations)
        
        if stats['total_connections_today'] > 0:
            successful_connections = stats['total_connections_today'] - stats['failed_connections_today']
            stats['connection_success_rate'] = (successful_connections / stats['total_connections_today']) * 100
        
        return stats

    async def update_voice_channel_name(self, guild_id: int, server_id: str):
        """Update voice channel name with current server status"""
        try:
            status_key = self.get_server_status_key(guild_id, server_id)

            if status_key not in self.server_status:
                return

            status = self.server_status[status_key]

            # Get guild config to find voice channel - FIX: Use proper database manager
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.warning("Bot database not available for voice channel update")
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            # Look for playercountvc channel (set by /setchannel playercountvc command)
            channels = guild_config.get('channels', {})
            voice_channel_id = channels.get('playercountvc')

            if not voice_channel_id:
                logger.debug(f"No playercountvc channel configured for guild {guild_id}")
                return

            # Get the voice channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                return

            voice_channel = guild.get_channel(voice_channel_id)
            if not voice_channel:
                logger.warning(f"Voice channel {voice_channel_id} not found for guild {guild_id}")
                return

            # PHASE 2 FIX: Use server name instead of server_id for voice channel updates
            server_name = status.get('server_name', 'Unnamed Server')
            current = status['current_players']
            max_players = status['max_players']
            queue = status['queue_count']

            # Format: "📈 ServerName: count/max" or "📈 ServerName: count/max (queue in queue)"
            if queue > 0:
                new_name = f"📈 {server_name}: {current}/{max_players} ({queue} in queue)"
            else:
                new_name = f"📈 {server_name}: {current}/{max_players}"

            # Update channel name if different
            if voice_channel.name != new_name:
                await voice_channel.edit(name=new_name)
                logger.info(f"Updated voice channel name to: {new_name}")

        except Exception as e:
            logger.error(f"Failed to update voice channel name: {e}")

    async def get_sftp_log_content(self, server_config: Dict[str, Any]) -> Optional[str]:
        """Get log content from SFTP server using AsyncSSH with rotation detection"""
        try:
            conn = await self.get_sftp_connection(server_config)
            if not conn:
                return None

            server_id = str(server_config.get('_id', 'unknown'))
            sftp_host = server_config.get('host')
            # Try multiple possible log paths
            possible_paths = [
                f"./{sftp_host}_{server_id}/Logs/Deadside.log",
                f"./{sftp_host}_{server_id}/logs/Deadside.log",
                f"./Logs/Deadside.log",
                f"./logs/Deadside.log"
            ]

            async with conn.start_sftp_client() as sftp:
                # Try each possible path until we find the log file
                for remote_path in possible_paths:
                    try:
                        logger.info(f"Trying SFTP log path: {remote_path} for server {server_id} on host {sftp_host}")

                        # Check file stats for rotation detection
                        file_stat = await sftp.stat(remote_path)
                        file_size = file_stat.size if hasattr(file_stat, 'size') else 0

                        server_key = f"{sftp_host}_{server_id}"

                        # Detect log rotation by checking if file size decreased
                        if server_key in self.last_log_position:
                            if file_size is not None and file_size < self.last_log_position[server_key]:
                                logger.info(f"Log rotation detected for {server_key}")
                                self.last_log_position[server_key] = 0  # Reset position

                        # Read from last position
                        start_position = self.last_log_position.get(server_key, 0)

                        async with sftp.open(remote_path, 'r') as f:
                            # Read entire file content
                            full_content = await f.read()

                            # Split into lines to track by line count instead of byte position
                            all_lines = full_content.splitlines()
                            total_lines = len(all_lines)

                            # Get last processed line count
                            last_line_count = self.last_log_position.get(server_key, 0)

                            # Detect rotation if line count decreased significantly
                            if last_line_count > total_lines and total_lines > 0:
                                logger.info(f"Log rotation detected for {server_key} (line count: {last_line_count} -> {total_lines})")
                                last_line_count = 0

                            # Get new lines only
                            new_lines = all_lines[last_line_count:]
                            new_content = '\n'.join(new_lines)

                            # Update position to current line count
                            self.last_log_position[server_key] = total_lines

                            logger.info(f"Successfully read log file from: {remote_path} ({len(new_lines)} new lines from total {total_lines})")
                            return new_content

                    except FileNotFoundError:
                        logger.debug(f"Log file not found at: {remote_path}")
                        continue
                    except Exception as e:
                        logger.warning(f"Error reading log file at {remote_path}: {e}")
                        continue

                logger.warning(f"No log file found at any of the attempted paths for server {server_id}")
                return None

        except Exception as e:
            logger.error(f"Failed to fetch SFTP log file: {e}")
            return None

    async def get_sftp_connection(self, server_config: Dict[str, Any]) -> Optional[asyncssh.SSHClientConnection]:
        """Get or create SFTP connection with pooling and timeout handling"""
        try:
            sftp_host = server_config.get('host')
            sftp_port = server_config.get('port', 22)
            sftp_username = server_config.get('username')
            sftp_password = server_config.get('password')

            if not all([sftp_host, sftp_username, sftp_password]):
                return None

            pool_key = f"{sftp_host}:{sftp_port}:{sftp_username}"

            # Check existing connection with improved validation
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
                    # Use exact format specified in diagnostic for asyncssh connection
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
                    return conn

                except (asyncio.TimeoutError, asyncssh.Error) as e:
                    logger.warning(f"SFTP connection attempt {attempt + 1} failed: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)

            return None

        except Exception as e:
            logger.error(f"Failed to get SFTP connection: {e}")
            return None

    async def get_dev_log_content(self) -> Optional[str]:
        """Get log content from attached_assets and dev_data directories"""
        try:
            # Check attached_assets first
            attached_log = Path('./attached_assets/Deadside.log')
            if attached_log.exists():
                try:
                    async with aiofiles.open(attached_log, 'r', encoding='utf-8', errors='ignore') as f:
                        content = await f.read()
                        return content
                except Exception as e:
                    logger.error(f"Failed to read log file {attached_log}: {e}")
                    return None

            # Fallback to dev_data
            log_path = Path('./dev_data/logs/Deadside.log')
            if log_path.exists():
                try:
                    async with aiofiles.open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = await f.read()
                        return content
                except Exception as e:
                    logger.error(f"Failed to read log file {log_path}: {e}")
                    return None

            logger.warning("No log file found in attached_assets or dev_data/logs/")
            return None

        except Exception as e:
            logger.error(f"Failed to read dev log file: {e}")
            return None

    async def parse_log_line(self, line: str, server_key: str, guild_id: int) -> Optional[Dict[str, Any]]:
        """Parse a single log line and extract event data with enhanced patterns"""
        line = line.strip()
        if not line:
            return None
        
        # FIRST: Check for player connection lifecycle events using new parser
        lifecycle_result = await self.connection_parser.parse_lifecycle_event(line, server_key, guild_id)
        if lifecycle_result:
            return lifecycle_result

        # Try each pattern - prioritize specific patterns over generic ones
        for event_type, pattern in self.log_patterns.items():
            match = pattern.search(line)
            if match:
                try:
                    # Handle different timestamp formats
                    timestamp_str = match.group(1) if match.groups() else None

                    if timestamp_str:
                        try:
                            # Try the expected format first
                            timestamp = datetime.strptime(timestamp_str, '%Y.%m.%d-%H.%M.%S:%f')
                        except ValueError:
                            try:
                                # Try without microseconds
                                timestamp = datetime.strptime(timestamp_str, '%Y.%m.%d-%H.%M.%S')
                            except ValueError:
                                # Fallback to current time
                                timestamp = datetime.now(timezone.utc)
                                logger.debug(f"Could not parse timestamp '{timestamp_str}', using current time")
                    else:
                        timestamp = datetime.now(timezone.utc)

                    timestamp = timestamp.replace(tzinfo=timezone.utc)

                    event_data = {
                        'type': event_type,
                        'timestamp': timestamp,
                        'raw_line': line
                    }

                    # Extract specific data based on event type with comprehensive lifecycle handling
                    try:
                        # COMPREHENSIVE PLAYER LIFECYCLE EVENT HANDLING
                        if event_type in ['player_queue_request', 'player_queue_accepted', 'player_beacon_handshake'] and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type == 'player_beacon_auth' and len(match.groups()) >= 4:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'unique_id': match.group(4),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type in ['player_world_auth', 'player_world_spawn', 'player_online_status', 'player_session_start', 'player_character_spawn'] and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type in ['player_disconnect_cleanup', 'player_session_end', 'player_beacon_disconnect', 'player_network_disconnect', 'player_queue_timeout', 'player_queue_failed', 'player_auth_failed'] and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        # Legacy pattern support for backward compatibility
                        elif event_type == 'player_queue_join' and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type == 'player_beacon_connected' and len(match.groups()) >= 4:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'unique_id': match.group(4),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type == 'player_world_connect' and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type == 'player_queue_disconnect' and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type in ['player_accepted_from', 'player_beacon_join'] and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type == 'player_connection_cleanup' and len(match.groups()) >= 3:
                            event_data.update({
                                'ip': match.group(2),
                                'port': match.group(3),
                                'connection_id': f"{match.group(2)}:{match.group(3)}"
                            })
                        elif event_type in ['mission_ready', 'mission_waiting', 'mission_initial'] and len(match.groups()) >= 2:
                            mission_name = match.group(2)
                            event_data.update({
                                'mission_name': mission_name,
                                'normalized_name': self.normalize_mission_name(mission_name),
                                'state': event_type.replace('mission_', '').upper()
                            })
                        elif event_type == 'mission_respawn' and len(match.groups()) >= 3:
                            mission_name = match.group(2)
                            respawn_time = int(match.group(3))
                            event_data.update({
                                'mission_name': mission_name,
                                'normalized_name': self.normalize_mission_name(mission_name),
                                'respawn_time': respawn_time
                            })
                        elif event_type == 'mission_state_any' and len(match.groups()) >= 3:
                            mission_name = match.group(2)
                            state = match.group(3)
                            # Convert to specific event type based on state
                            if state == 'READY':
                                event_data['type'] = 'mission_ready'
                            elif state == 'WAITING':
                                event_data['type'] = 'mission_waiting'
                            elif state == 'INITIAL':
                                event_data['type'] = 'mission_initial'
                            
                            event_data.update({
                                'mission_name': mission_name,
                                'normalized_name': self.normalize_mission_name(mission_name),
                                'state': state
                            })
                        elif event_type == 'encounter_initial' and len(match.groups()) >= 3:
                            encounter_name = match.group(2)
                            respawn_time = int(match.group(3))
                            event_data.update({
                                'encounter_name': encounter_name,
                                'respawn_time': respawn_time
                            })
                        elif event_type == 'patrol_switch' and len(match.groups()) >= 3:
                            patrol_name = match.group(2)
                            state = match.group(3)
                            monsters = match.group(4) if len(match.groups()) >= 4 and match.group(4) else None
                            event_data.update({
                                'patrol_name': patrol_name,
                                'state': state,
                                'monsters': int(monsters) if monsters else None
                            })
                        elif event_type == 'vehicle_spawn' and len(match.groups()) >= 2:
                            # Enhanced vehicle spawn detection
                            current_vehicles = None
                            max_vehicles = None
                            vehicle_type = 'Unknown'
                            
                            if len(match.groups()) >= 3:
                                try:
                                    current_vehicles = int(match.group(2)) if match.group(2) else None
                                    max_vehicles = int(match.group(3)) if match.group(3) else None
                                except (ValueError, TypeError):
                                    pass
                            
                            # Try to extract vehicle type from the line
                            if 'BP_Vehicle_' in line:
                                vehicle_match = re.search(r'BP_Vehicle_[A-Za-z0-9_]+', line)
                                if vehicle_match:
                                    vehicle_type = self.normalize_vehicle_name(vehicle_match.group())
                            
                            event_data.update({
                                'current_vehicles': current_vehicles,
                                'max_vehicles': max_vehicles,
                                'vehicle_type': vehicle_type
                            })
                        elif event_type == 'vehicle_delete' and len(match.groups()) >= 2:
                            vehicle_type = match.group(2) or match.group(3) if len(match.groups()) >= 3 else 'Unknown'
                            
                            # Try to extract vehicle type from the line if not found in groups
                            if vehicle_type == 'Unknown' and 'BP_Vehicle_' in line:
                                vehicle_match = re.search(r'BP_Vehicle_[A-Za-z0-9_]+', line)
                                if vehicle_match:
                                    vehicle_type = self.normalize_vehicle_name(vehicle_match.group())
                            else:
                                vehicle_type = self.normalize_vehicle_name(vehicle_type)
                            
                            event_data.update({
                                'vehicle_type': vehicle_type
                            })
                        elif event_type in ['helicrash_initial', 'helicrash_spawned', 'helicrash_switched']:
                            location = 'Unknown'
                            if len(match.groups()) >= 4 and match.group(2) and match.group(3):
                                try:
                                    x_coord = float(match.group(2))
                                    y_coord = float(match.group(3))
                                    location = f"Grid {x_coord:.0f},{y_coord:.0f}"
                                except (ValueError, TypeError):
                                    pass
                            event_data.update({
                                'crash_type': 'helicopter',
                                'state': 'INITIAL',
                                'location': location
                            })
                        elif event_type in ['airdrop_flying', 'airdrop_switched']:
                            event_data.update({
                                'airdrop_state': 'flying'
                            })
                        elif event_type in ['trader_spawn', 'trader_switched', 'trader_available']:
                            location = 'Unknown'
                            if len(match.groups()) >= 4 and match.group(2) and match.group(3):
                                try:
                                    x_coord = float(match.group(2))
                                    y_coord = float(match.group(3))
                                    location = f"Grid {x_coord:.0f},{y_coord:.0f}"
                                except (ValueError, TypeError):
                                    pass
                            event_data.update({
                                'trader_state': 'available',
                                'location': location
                            })
                        elif event_type == 'construction_save' and len(match.groups()) >= 3:
                            count = int(match.group(2))
                            duration = float(match.group(3))
                            event_data.update({
                                'constructibles_count': count,
                                'save_duration_ms': duration
                            })
                        elif event_type == 'server_max_players' and len(match.groups()) >= 2:
                            event_data['max_players'] = int(match.group(2))
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Error extracting data from event {event_type}: {e}")

                    return event_data

                except Exception as e:
                    logger.debug(f"Failed to parse event type {event_type} from line: {e}")
                    continue

        # Log unmatched lines occasionally for debugging
        if len(line) > 50:  # Only log substantial lines
            logger.debug(f"No pattern matched for line: {line[:100]}...")

        return None

    def should_output_event(self, event_data: Dict[str, Any]) -> bool:
        """Determine if event should be output based on dispatch rules"""
        event_type = event_data['type']

        # Mission events - output only when READY
        if event_type in ['mission_ready', 'mission_waiting', 'mission_initial']:
            return event_type == 'mission_ready'

        # Airdrop events - output when flying or switched
        if event_type in ['airdrop_flying', 'airdrop_switched']:
            return True

        # Encounters - do not output
        if event_type == 'encounter_initial':
            return False

        # Construction saves - do not output
        if event_type == 'construction_save':
            return False

        # Heli crashes - output all types
        if event_type in ['helicrash_initial', 'helicrash_spawned', 'helicrash_switched']:
            return True

        # Trader events - output all types
        if event_type in ['trader_spawn', 'trader_switched', 'trader_available']:
            return True

        # Vehicle spawns - output on valid spawn/initiate line
        if event_type in ['vehicle_spawn', 'vehicle_delete']:
            return True

        # Player events - output based on significance (connections and disconnections)
        if event_type in ['player_world_connect', 'player_world_spawn', 'player_online_status', 'player_session_start',
                         'player_queue_disconnect', 'player_disconnect_cleanup', 'player_session_end', 
                         'player_beacon_disconnect', 'player_network_disconnect', 'player_accepted_from', 
                         'player_connection_cleanup', 'player_beacon_join']:
            return True

        # Queue events - output for visibility into queue activity
        if event_type in ['player_queue_accepted', 'player_queue_timeout', 'player_queue_failed', 'player_auth_failed']:
            return True

        return False

    async def send_log_event_embed(self, guild_id: int, server_id: str, event_data: Dict[str, Any]):
        """Send log event embed to appropriate channel using EmbedFactory"""
        try:
            # Check if event should be output
            if not self.should_output_event(event_data):
                logger.debug(f"Event {event_data['type']} suppressed per dispatch rules")
                return

            # Get guild configuration - FIX: Use proper database manager
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.warning("Bot database not available for sending embeds")
                return

            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return

            channels = guild_config.get('channels', {})
            event_type = event_data['type']

            # Map event types to channel types
            channel_mapping = {
                # Player connection events
                'player_world_connect': 'connections',
                'player_world_spawn': 'connections', 
                'player_online_status': 'connections',
                'player_session_start': 'connections',
                'player_queue_accepted': 'connections',
                'player_accepted_from': 'connections',
                'player_beacon_join': 'connections',
                # Player disconnection events
                'player_queue_disconnect': 'connections',
                'player_disconnect_cleanup': 'connections',
                'player_session_end': 'connections',
                'player_beacon_disconnect': 'connections',
                'player_network_disconnect': 'connections',
                'player_connection_cleanup': 'connections',
                'player_queue_timeout': 'connections',
                'player_queue_failed': 'connections',
                'player_auth_failed': 'connections',
                # Game events
                'mission_ready': 'events',
                'airdrop_flying': 'events',
                'airdrop_switched': 'events',
                'helicrash_initial': 'events',
                'helicrash_spawned': 'events',
                'helicrash_switched': 'events',
                'trader_spawn': 'events',
                'trader_switched': 'events',
                'trader_available': 'events',
                'vehicle_spawn': 'events',
                'vehicle_delete': 'events'
            }

            # Get the appropriate channel for this event type
            channel_type = channel_mapping.get(event_type, 'events')  # Default to events
            channel_id = channels.get(channel_type)

            # If specific channel not set, try fallback channels
            if not channel_id:
                # Try 'logs' as fallback for backward compatibility
                channel_id = channels.get('logs')

            if not channel_id:
                logger.debug(f"No channel configured for event type '{event_type}' (needs '{channel_type}' channel)")
                return

            channel = self.bot.get_channel(channel_id)
            if not channel:
                logger.warning(f"Channel {channel_id} not found for event type '{event_type}'")
                return

            # Create event-specific embed using EmbedFactory with file attachment
            embed_result = await self._create_event_embed_via_factory(event_data)
            if embed_result:
                if isinstance(embed_result, tuple):
                    embed, file = embed_result
                    await channel.send(embed=embed, file=file)
                else:
                    embed = embed_result
                    await channel.send(embed=embed)
                logger.debug(f"Sent {event_type} embed to {channel_type} channel: {channel.name}")

        except Exception as e:
            logger.error(f"Failed to send log event embed: {e}")

    async def _create_event_embed_via_factory(self, event_data: Dict[str, Any]):
        """Create styled embed for log event using EmbedFactory"""
        try:
            from bot.utils.embed_factory import EmbedFactory

            event_type = event_data['type']
            timestamp = event_data['timestamp']

            # Map event types to EmbedFactory keys and create appropriate embeds
            if event_type == 'player_world_connect':
                embed_data = {
                    'connection_id': event_data.get('connection_id', 'Unknown'),
                    'server_id': event_data.get('server_id', 'Unknown'),
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('player_connection', embed_data)

            elif event_type == 'player_queue_disconnect':
                embed_data = {
                    'connection_id': event_data.get('connection_id', 'Unknown'),
                    'server_id': event_data.get('server_id', 'Unknown'),
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('player_disconnection', embed_data)

            elif event_type == 'mission_ready':
                mission_name = event_data.get('normalized_name', 'Unknown Mission')
                embed_data = {
                    'mission_name': mission_name,
                    'state': 'READY',
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('mission_event', embed_data)

            elif event_type in ['airdrop_flying', 'airdrop_switched']:
                embed_data = {
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('airdrop_event', embed_data)

            elif event_type in ['helicrash_initial', 'helicrash_spawned', 'helicrash_switched']:
                embed_data = {
                    'location': event_data.get('location', 'Unknown'),
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('helicrash_event', embed_data)

            elif event_type in ['trader_spawn', 'trader_switched', 'trader_available']:
                embed_data = {
                    'location': event_data.get('location', 'Unknown'),
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('trader_event', embed_data)

            elif event_type == 'vehicle_spawn':
                vehicle_type = event_data.get('vehicle_type', 'Military Vehicle')
                embed_data = {
                    'vehicle_type': vehicle_type,
                    'action': 'spawn',
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('vehicle_event', embed_data)

            elif event_type == 'vehicle_delete':
                vehicle_type = event_data.get('vehicle_type', 'Military Vehicle')
                embed_data = {
                    'vehicle_type': vehicle_type,
                    'action': 'delete',
                    'timestamp': timestamp
                }
                return await EmbedFactory.build('vehicle_event', embed_data)

            else:
                return None

        except Exception as e:
            logger.error(f"Failed to create event embed via factory: {e}")
            return None

    async def parse_logs_for_server(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse logs for a specific server"""
        try:
            # Check if server has premium access - for now, allow all servers to use log parser
            # Premium check can be re-enabled later if needed
            # if not await self.bot.database.is_premium_server(guild_id, str(server_config.get('_id', 'unknown'))):
            #     return

            # Parse logs using SSH/SFTP
            if self.bot.dev_mode:
                await self.parse_dev_logs(guild_id, server_config)
            else:
                await self.parse_sftp_logs(guild_id, server_config)

        except Exception as e:
            logger.error(f"Failed to parse logs for server {server_config}: {e}")

    async def parse_sftp_logs(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse logs from SFTP server"""
        try:
            host = server_config.get('host', server_config.get('hostname'))
            port = server_config.get('port', 22)
            username = server_config.get('username')
            password = server_config.get('password')
            server_id = str(server_config.get('_id', 'unknown'))

            if not all([host, username, password]):
                logger.warning(f"Missing SFTP credentials for server {server_id}")
                return

            # Create SSH connection
            async with asyncssh.connect(
                host, port=port, username=username, password=password,
                known_hosts=None, server_host_key_algs=['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512']
            ) as conn:

                async with conn.start_sftp_client() as sftp:
                    # Get log files
                    log_path = f"./{host}_{server_id}/actual1/logs/"

                    try:
                        files = await sftp.glob(f"{log_path}**/*.log")

                        # Get current time with timezone awareness
                        current_time = datetime.now(timezone.utc)

                        for file_path in files:
                            try:
                                # Check file modification time
                                stat = await sftp.stat(file_path)
                                # Make file_mtime timezone-aware
                                file_mtime = datetime.fromtimestamp(getattr(stat, 'st_mtime', datetime.now().timestamp()), tz=timezone.utc)

                                # Only process recent files (last 24 hours)
                                if (current_time - file_mtime).total_seconds() > 86400:
                                    continue

                                # Read and parse file
                                async with sftp.open(file_path, 'r') as f:
                                    content = await f.read()
                                    await self.process_log_content(guild_id, server_id, content)

                            except Exception as e:
                                logger.error(f"Failed to process log file {file_path}: {e}")
                                continue

                    except Exception as e:
                        logger.warning(f"No log files found at {log_path}: {e}")

        except Exception as e:
            logger.error(f"Failed SFTP log parsing: {e}")

    async def parse_dev_logs(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse logs in development mode from local files"""
        try:
            server_id = str(server_config.get('_id', 'dev_server'))

            # Get log content from dev files
            log_content = await self.get_dev_log_content()

            if not log_content:
                logger.warning(f"No dev log content found for server {server_id}")
                return

            lines = log_content.splitlines()

            # Track position for incremental parsing
            server_key = f"{guild_id}_{server_id}"
            last_position = self.last_log_position.get(server_key, 0)

            # Process new lines only
            new_lines = lines[last_position:]
            new_events = 0

            for line in new_lines:
                server_key = self.get_server_status_key(guild_id, server_id)
                event_data = await self.parse_log_line(line, server_key, guild_id)
                if event_data:
                    # Process player tracking events
                    await self.process_log_event(guild_id, server_id, event_data)
                    # Send embed
                    await self.send_log_event_embed(guild_id, server_id, event_data)
                    new_events += 1

            # Update position
            self.last_log_position[server_key] = len(lines)

            if new_events > 0:
                logger.info(f"Processed {new_events} new dev log events for server {server_id}")

        except Exception as e:
            logger.error(f"Failed to parse dev logs for server {server_config}: {e}")

    async def parse_server_logs(self, guild_id: int, server_config: Dict[str, Any]):
        """Parse logs for a single server (PREMIUM ONLY) with enhanced event detection"""
        try:
            server_id = str(server_config.get('_id', 'unknown'))

            logger.info(f"Parsing logs for premium server {server_id} in guild {guild_id}")

            # Get log content
            if self.bot.dev_mode:
                log_content = await self.get_dev_log_content()
            else:
                log_content = await self.get_sftp_log_content(server_config)

            if not log_content:
                logger.warning(f"No log content found for server {server_id}")
                return

            if not log_content.strip():
                logger.warning(f"Log content is empty for server {server_id}")
                return

            lines = log_content.splitlines()
            logger.info(f"Processing {len(lines)} new lines from log content")

            # Process in larger batches for better performance with 100+ servers
            batch_size = 500
            new_events = 0
            processed_lines = 0
            total_batches = (len(lines) + batch_size - 1) // batch_size

            logger.info(f"Starting enhanced batch processing: {len(lines)} lines in {total_batches} batches")

            for i in range(0, len(lines), batch_size):
                batch = lines[i:i + batch_size]
                batch_events = 0
                batch_number = (i // batch_size) + 1

                logger.debug(f"Processing batch {batch_number}/{total_batches} (lines {i+1}-{min(i+batch_size, len(lines))})")

                for line_num, line in enumerate(batch, start=i+1):
                    processed_lines += 1
                    server_key = self.get_server_status_key(guild_id, server_id)
                    event_data = await self.parse_log_line(line, server_key, guild_id)
                    if event_data:
                        logger.debug(f"Line {line_num}: Parsed event: {event_data['type']}")
                        # Process player tracking events
                        await self.process_log_event(guild_id, server_id, event_data)
                        # Send embed (with dispatch rule filtering)
                        await self.send_log_event_embed(guild_id, server_id, event_data)
                        new_events += 1
                        batch_events += 1

                # Log progress for every batch
                logger.info(f"Batch {batch_number}/{total_batches}: Found {batch_events} events from {len(batch)} lines (total processed: {processed_lines}/{len(lines)})")

                # Small delay between batches to prevent overwhelming Discord API
                if i + batch_size < len(lines):
                    await asyncio.sleep(0.05)

            logger.info(f"Completed enhanced processing {processed_lines} lines, found {new_events} events for server {server_id}")

        except Exception as e:
            logger.error(f"Failed to parse logs for server {server_config}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

    async def run_log_parser(self):
        """Run log parser for all premium servers"""
        try:
            logger.info("Running enhanced log parser for premium servers...")

            # Get all guilds with configured servers
            guilds_cursor = self.bot.database.guilds.find({})

            async for guild_doc in guilds_cursor:
                guild_id = guild_doc['guild_id']
                servers = guild_doc.get('servers', [])

                for server_config in servers:
                    await self.parse_server_logs(guild_id, server_config)

            logger.info("Enhanced log parser completed")

        except Exception as e:
            logger.error(f"Failed to run enhanced log parser: {e}")

    async def process_log_content(self, guild_id: int, server_id: str, content: str):
        """Process log content and extract events"""
        try:
            lines = content.splitlines()
            for line in lines:
                if not line.strip():
                    continue

                server_key = self.get_server_status_key(guild_id, server_id)
                event_data = await self.parse_log_line(line, server_key, guild_id)
                if event_data:
                    # Process player tracking events
                    await self.process_log_event(guild_id, server_id, event_data)
                    # Send embed
                    await self.send_log_event_embed(guild_id, server_id, event_data)

        except Exception as e:
            logger.error(f"Failed to process log content: {e}")

    def schedule_log_parser(self):
        """Schedule log parser to run every 300 seconds"""
        try:
            self.bot.scheduler.add_job(
                self.run_log_parser,
                'interval',
                seconds=300,  # 5 minutes
                id='log_parser',
                replace_existing=True
            )
            logger.info("Enhanced log parser scheduled (every 300 seconds)")

        except Exception as e:
            logger.error(f"Failed to schedule enhanced log parser: {e}")

    async def process_log_event(self, guild_id: int, server_id: str, event_data: Dict[str, Any]):
        """Process a parsed log event, handling comprehensive player lifecycle tracking"""
        try:
            event_type = event_data['type']
            timestamp = event_data['timestamp']

            # Handle comprehensive player lifecycle tracking
            if event_type in ['player_queue_request', 'player_queue_accepted', 'player_beacon_handshake', 
                            'player_beacon_auth', 'player_world_auth', 'player_world_spawn', 
                            'player_online_status', 'player_session_start', 'player_character_spawn',
                            'player_disconnect_cleanup', 'player_session_end', 'player_beacon_disconnect',
                            'player_network_disconnect', 'player_queue_timeout', 'player_queue_failed',
                            'player_auth_failed', 'player_queue_join', 'player_beacon_connected',
                            'player_world_connect', 'player_queue_disconnect']:
                
                ip = event_data.get('ip')
                port = event_data.get('port')
                
                if ip and port:
                    additional_data = {}
                    if 'unique_id' in event_data:
                        additional_data['unique_id'] = event_data['unique_id']
                    
                    await self.track_player_lifecycle_event(
                        guild_id, server_id, ip, port, event_type, timestamp, additional_data
                    )
                    
            elif event_type == 'server_max_players':
                max_players = event_data.get('max_players', 50)
                await self.update_server_max_players(guild_id, server_id, max_players)

            # Periodic cleanup of old lifecycle data
            if hasattr(self, '_last_cleanup'):
                if (timestamp - self._last_cleanup).total_seconds() > 3600:  # Cleanup every hour
                    await self.cleanup_old_lifecycle_data()
                    self._last_cleanup = timestamp
            else:
                self._last_cleanup = timestamp

        except Exception as e:
            logger.error(f"Failed to process log event: {e}")

    def reset_log_positions(self, guild_id: int = None, server_id: str = None):
        """Reset log position tracking for specific server or all servers"""
        if guild_id and server_id:
            # Reset specific server
            server_key = f"{guild_id}_{server_id}"
            if server_key in self.last_log_position:
                del self.last_log_position[server_key]
                logger.info(f"Reset log position for server {server_id} in guild {guild_id}")
            else:
                logger.info(f"No position tracking found for server {server_id} in guild {guild_id}")
        else:
            # Reset all positions
            self.last_log_position.clear()
            logger.info("Reset all log position tracking")

    async def run_log_parser(self):
        """Main log parser execution with enhanced event detection"""
        try:
            logger.info("Starting enhanced log parser execution")

            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.error("Bot database not available for log parsing")
                return

            # Get servers from database using proper database manager
            guilds_cursor = self.bot.db_manager.guilds.find({})
            total_servers_processed = 0

            async for guild_doc in guilds_cursor:
                guild_id = guild_doc['guild_id']
                servers = guild_doc.get('servers', [])
                if not servers:
                    logger.warning(f"No servers found for guild {guild_id}")
                    continue
                logger.info(f"Found {len(servers)} servers for guild {guild_id}")

                for server in servers:
                    server_name = server.get('name', 'Unknown')
                    server_id = str(server.get('_id', 'unknown'))
                    try:
                        logger.info(f"Processing logs for server: {server_name} (ID: {server_id})")

                        await self.parse_server_logs(guild_id, server)

                        total_servers_processed += 1
                        logger.info(f"Successfully processed logs for server: {server_name}")

                    except Exception as server_error:
                        logger.error(f"Failed to process server {server_name}: {server_error}")
                        import traceback
                        logger.error(f"Server error traceback: {traceback.format_exc()}")

            logger.info(f"Enhanced log parser execution completed - processed {total_servers_processed} servers")

        except Exception as e:
            logger.error(f"Enhanced log parser execution failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")