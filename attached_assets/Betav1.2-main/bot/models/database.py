# Applying the provided changes to the original code to fix datetime errors and add faction functionality.
"""
Emerald's Killfeed - Database Models and Architecture
Implements PHASE 1 data architecture requirements
"""

import logging
import asyncio
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorDatabase

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Database manager implementing PHASE 1 architecture:
    - All PvP data stored per game server
    - Linking, wallet, factions stored per guild
    - Players linked to one account spanning all servers in guild
    - Premium tracked per game server, not user or guild
    """

    def __init__(self, mongo_client: AsyncIOMotorClient):
        self.client = mongo_client
        self.db: AsyncIOMotorDatabase = mongo_client.emerald_killfeed

        # Collections
        self.guilds = self.db.guilds
        self.pvp_data = self.db.pvp_data
        self.players = self.db.players
        self.economy = self.db.economy
        self.bounties = self.db.bounties
        self.factions = self.db.factions
        self.kill_events = self.db.kill_events
        self.premium = self.db.premium_servers

    async def initialize_indexes(self):
        """Create database indexes for optimal performance"""
        try:
            # Guild indexes
            await self.guilds.create_index("guild_id", unique=True)

            # Player indexes (guild-scoped)
            await self.players.create_index([("guild_id", 1), ("discord_id", 1)], unique=True)
            await self.players.create_index([("guild_id", 1), ("linked_characters", 1)])

            # PvP data indexes (server-scoped)
            await self.pvp_data.create_index([("guild_id", 1), ("server_id", 1), ("player_name", 1)], unique=True)
            await self.pvp_data.create_index([("guild_id", 1), ("server_id", 1), ("kills", -1)])
            await self.pvp_data.create_index([("guild_id", 1), ("server_id", 1), ("kdr", -1)])

            # Kill events indexes (server-scoped)
            await self.kill_events.create_index([("guild_id", 1), ("server_id", 1), ("timestamp", -1)])
            await self.kill_events.create_index([("guild_id", 1), ("server_id", 1), ("killer", 1)])
            await self.kill_events.create_index([("guild_id", 1), ("server_id", 1), ("victim", 1)])

            # Economy indexes (guild-scoped)
            await self.economy.create_index([("guild_id", 1), ("discord_id", 1)], unique=True)

            # Faction indexes (guild-scoped)
            await self.factions.create_index([("guild_id", 1), ("faction_name", 1)], unique=True)

            # Premium indexes (server-scoped)
            await self.premium.create_index([("guild_id", 1), ("_id", 1)], unique=True)
            await self.premium.create_index("expires_at")

            # Bounty indexes (guild-scoped)
            await self.bounties.create_index([("guild_id", 1), ("target_player", 1)])
            await self.bounties.create_index("expires_at")

            # Factions collection indexes
            await self.factions.create_index([("guild_id", 1), ("faction_name", 1)], unique=True)
            await self.factions.create_index([("guild_id", 1)])

            # Kill events collection indexes
            await self.kill_events.create_index([("guild_id", 1), ("server_id", 1)])
            await self.kill_events.create_index([("timestamp", -1)])
            await self.kill_events.create_index([("killer", 1)])
            await self.kill_events.create_index([("victim", 1)])

            logger.info("Database indexes created successfully")

        except Exception as e:
            logger.error(f"Failed to create database indexes: {e}")

    # GUILD MANAGEMENT
    async def create_guild(self, guild_id: int, guild_name: str) -> Dict[str, Any]:
        """Create guild configuration"""
        guild_doc = {
            "guild_id": guild_id,
            "guild_name": guild_name,
            "created_at": datetime.now(timezone.utc),
            "last_updated": datetime.now(timezone.utc),
            "servers": [],  # List of connected game servers
            "channels": {
                "killfeed": None,
                "leaderboard": None,
                "logs": None
            },
            "settings": {
                "prefix": "!",
                "timezone": "UTC"
            }
        }

        await self.guilds.insert_one(guild_doc)
        logger.info(f"Created guild: {guild_name} ({guild_id})")
        return guild_doc

    async def get_guild(self, guild_id: int) -> Optional[Dict[str, Any]]:
        """Get guild configuration"""
        try:
            return await self.guilds.find_one({"guild_id": guild_id})
        except Exception as e:
            logger.error(f"Failed to get guild {guild_id}: {e}")
            return None

    async def add_server_to_guild(self, guild_id: int, server_config: Dict[str, Any]) -> bool:
        """Add game server to guild"""
        try:
            result = await self.guilds.update_one(
                {"guild_id": guild_id},
                {"$addToSet": {"servers": server_config}}
            )
            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to add server to guild {guild_id}: {e}")
            return False

    async def remove_server_from_guild(self, guild_id: int, server_id: str) -> bool:
        """Remove game server from guild"""
        try:
            # Try removing by _id first (new format)
            result = await self.guilds.update_one(
                {"guild_id": guild_id},
                {"$pull": {"servers": {"_id": server_id}}}
            )

            # If no match, try removing by server_id (old format)
            if result.modified_count == 0:
                result = await self.guilds.update_one(
                    {"guild_id": guild_id},
                    {"$pull": {"servers": {"server_id": server_id}}}
                )

            return result.modified_count > 0
        except Exception as e:
            logger.error(f"Failed to remove server from guild {guild_id}: {e}")
            return False

    # PLAYER LINKING (Guild-scoped)
    async def find_player_in_pvp_data(self, guild_id: int, character_name: str) -> Optional[str]:
        """Find player in PvP data with case-insensitive search, returns actual player name if found"""
        try:
            # Search for player with case-insensitive regex that handles spaces
            # Remove extra spaces and normalize the search term
            normalized_search = ' '.join(character_name.strip().split())

            # Create case-insensitive regex pattern
            escaped_search = normalized_search.replace(' ', r'\s+')
            pattern = f"^{escaped_search}$"

            player_doc = await self.pvp_data.find_one({
                "guild_id": guild_id,
                "player_name": {"$regex": pattern, "$options": "i"}
            })

            if player_doc:
                return player_doc["player_name"]  # Return the actual player name from database

            return None

        except Exception as e:
            logger.error(f"Failed to find player in PvP data: {e}")
            return None

    async def link_player(self, guild_id: int, discord_id: int, character_name: str) -> bool:
        """Link Discord user to character (guild-scoped)"""
        try:
            # Check if player already exists
            existing_player = await self.players.find_one({
                "guild_id": guild_id, 
                "discord_id": discord_id
            })

            if existing_player:
                # Player exists, just add the character if not already linked
                if character_name not in existing_player.get('linked_characters', []):
                    await self.players.update_one(
                        {"guild_id": guild_id, "discord_id": discord_id},
                        {"$addToSet": {"linked_characters": character_name}}
                    )
            else:
                # New player, create document
                player_doc = {
                    "guild_id": guild_id,
                    "discord_id": discord_id,
                    "linked_characters": [character_name],
                    "primary_character": character_name,
                    "linked_at": datetime.now(timezone.utc)
                }
                await self.players.insert_one(player_doc)

            logger.info(f"Linked player {character_name} to Discord {discord_id} in guild {guild_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to link player: {e}")
            return False

    async def get_linked_player(self, guild_id: int, discord_id: int) -> Optional[Dict[str, Any]]:
        """Get linked player data"""
        try:
            player_doc = await self.players.find_one({
                'guild_id': guild_id,
                'discord_id': discord_id
            })

            if player_doc:
                # Ensure we return a proper dict, not a tuple
                if isinstance(player_doc, dict):
                    # Validate required fields
                    if 'linked_characters' not in player_doc or not player_doc['linked_characters']:
                        logger.error(f"Player doc missing linked_characters: {player_doc}")
                        # Try to repair by deleting corrupt document
                        try:
                            await self.players.delete_one({
                                'guild_id': guild_id,
                                'discord_id': discord_id
                            })
                            logger.info(f"Deleted corrupt player document for guild {guild_id}, discord {discord_id}")
                        except Exception as cleanup_error:
                            logger.error(f"Failed to cleanup corrupt player document: {cleanup_error}")
                        return None

                    if 'primary_character' not in player_doc:
                        # Set primary_character if missing and update document
                        primary_char = player_doc['linked_characters'][0]
                        player_doc['primary_character'] = primary_char
                        try:
                            await self.players.update_one(
                                {'guild_id': guild_id, 'discord_id': discord_id},
                                {'$set': {'primary_character': primary_char}}
                            )
                            logger.info(f"Set missing primary_character for guild {guild_id}, discord {discord_id}")
                        except Exception as update_error:
                            logger.error(f"Failed to set primary_character: {update_error}")

                    # Ensure linked_at field exists
                    if 'linked_at' not in player_doc:
                        player_doc['linked_at'] = datetime.now(timezone.utc)
                        try:
                            await self.players.update_one(
                                {'guild_id': guild_id, 'discord_id': discord_id},
                                {'$set': {'linked_at': player_doc['linked_at']}}
                            )
                        except Exception as update_error:
                            logger.error(f"Failed to set linked_at: {update_error}")

                    return player_doc
                else:
                    logger.error(f"Unexpected player_doc type: {type(player_doc)} - value: {player_doc}")
                    return None

            return None

        except Exception as e:
            logger.error(f"Failed to get linked player: {e}")
            raise  # Re-raise to allow calling code to handle appropriately

    # PVP DATA (Server-scoped)
    async def update_pvp_stats(self, guild_id: int, server_id: str, player_name: str, 
                              stats_update: Dict[str, Any]) -> bool:
        """Update PvP statistics for player on specific server"""
        try:
            # Define all possible stat fields that could be incremented
            incrementable_fields = {
                "kills", "deaths", "suicides", "longest_streak", "current_streak", "total_distance"
            }

            # Handle atomic increment operations
            if isinstance(stats_update, dict) and len(stats_update) == 1:
                # Simple single field update - use atomic increment
                field_name = list(stats_update.keys())[0]
                field_value = list(stats_update.values())[0]

                if field_name in incrementable_fields:
                    # Create safe defaults without any incrementable fields or timestamps
                    safe_defaults = {
                        "guild_id": guild_id,
                        "server_id": server_id,
                        "player_name": player_name,
                        "created_at": datetime.now(timezone.utc),
                        "kdr": 0.0,
                        "favorite_weapon": None,
                        "best_streak": 0,
                        "personal_best_distance": 0.0
                    }

                    # Only add non-incrementable stat defaults
                    for field in ["kills", "deaths", "suicides", "longest_streak", "current_streak", "total_distance"]:
                        if field != field_name:  # Don't set default for field we're incrementing
                            safe_defaults[field] = 0 if field != "total_distance" else 0.0

                    # Single atomic operation without conflicts
                    result = await self.pvp_data.update_one(
                        {
                            "guild_id": guild_id,
                            "server_id": server_id,
                            "player_name": player_name
                        },
                        {
                            "$inc": {field_name: field_value},
                            "$setOnInsert": safe_defaults,
                            "$currentDate": {"last_updated": True}
                        },
                        upsert=True
                    )

                    # Handle KDR calculation separately if needed
                    if field_name in ["kills", "deaths"] and result.acknowledged:
                        await self._update_kdr(guild_id, server_id, player_name)

                else:
                    # Non-incrementable field, use simple set
                    await self.pvp_data.update_one(
                        {
                            "guild_id": guild_id,
                            "server_id": server_id,
                            "player_name": player_name
                        },
                        {
                            "$set": stats_update,
                            "$currentDate": {"last_updated": True}
                        },
                        upsert=True
                    )
            else:
                # Complex update - get current doc first to avoid conflicts
                current_doc = await self.pvp_data.find_one({
                    "guild_id": guild_id,
                    "server_id": server_id,
                    "player_name": player_name
                })

                # Calculate KDR if kills or deaths are being updated
                if "kills" in stats_update or "deaths" in stats_update:
                    kills = stats_update.get("kills", current_doc.get("kills", 0) if current_doc else 0)
                    deaths = stats_update.get("deaths", current_doc.get("deaths", 0) if current_doc else 0)
                    stats_update["kdr"] = kills / max(deaths, 1) if deaths > 0 else float(kills)

                if not current_doc:
                    # Create new document
                    new_doc = {
                        "guild_id": guild_id,
                        "server_id": server_id,
                        "player_name": player_name,
                        "created_at": datetime.now(timezone.utc),
                        "last_updated": datetime.now(timezone.utc),
                        "kills": 0,
                        "deaths": 0,
                        "suicides": 0,
                        "kdr": 0.0,
                        "total_distance": 0.0,
                        "favorite_weapon": None,
                        "longest_streak": 0,
                        "current_streak": 0,
                        "personal_best_distance": 0.0,
                        **stats_update
                    }
                    await self.pvp_data.insert_one(new_doc)
                else:
                    # Update existing document
                    await self.pvp_data.update_one(
                        {"guild_id": guild_id, "server_id": server_id, "player_name": player_name},
                        {
                            "$set": {
                                **stats_update,
                                "last_updated": datetime.now(timezone.utc)
                            }
                        }
                    )

            logger.debug(f"Successfully updated PvP stats for {player_name} in server {server_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to update PvP stats: {e}")
            return False

    async def _update_kdr(self, guild_id: int, server_id: str, player_name: str):
        """Helper method to update KDR calculation"""
        try:
            current_doc = await self.pvp_data.find_one({
                "guild_id": guild_id,
                "server_id": server_id,
                "player_name": player_name
            })

            if current_doc:
                kills = current_doc.get("kills", 0)
                deaths = current_doc.get("deaths", 0)
                kdr = kills / max(deaths, 1) if deaths > 0 else float(kills)

                await self.pvp_data.update_one(
                    {"guild_id": guild_id, "server_id": server_id, "player_name": player_name},
                    {"$set": {"kdr": kdr}}
                )
        except Exception as e:
            logger.error(f"Failed to update KDR: {e}")

    async def get_pvp_stats(self, guild_id: int, server_id: str, player_name: str) -> Optional[Dict[str, Any]]:
        """Get PvP statistics for player on specific server"""
        return await self.pvp_data.find_one({
            "guild_id": guild_id,
            "server_id": server_id,
            "player_name": player_name
        })

    async def get_guild_currency_name(self, guild_id: int) -> str:
        """Get custom currency name for guild or default"""
        try:
            guild_doc = await self.guilds.find_one({'guild_id': guild_id})
            return guild_doc.get('currency_name', 'Emeralds') if guild_doc else 'Emeralds'
        except Exception:
            return 'Emeralds'

    async def reset_player_streak(self, guild_id: int, server_id: str, player_name: str):
        """Reset a player's current streak to 0"""
        try:
            await self.pvp_data.update_one(
                {
                    "guild_id": guild_id,
                    "server_id": server_id,
                    "player_name": player_name
                },
                {
                    "$set": {"current_streak": 0}
                }
            )
        except Exception as e:
            logger.error(f"Failed to reset player streak: {e}")

    async def add_kill_event(self, guild_id: int, server_id: str, kill_data: Dict[str, Any]):
        """Add a kill event to the database with enhanced distance validation"""
        try:
            # PHASE 1 FIX: Ensure distance is properly validated before DB insertion
            distance = kill_data.get("distance", 0)
            if isinstance(distance, str):
                try:
                    distance = float(distance) if distance else 0.0
                except (ValueError, TypeError):
                    distance = 0.0
            elif not isinstance(distance, (int, float)):
                distance = 0.0
            
            # Ensure distance is within reasonable bounds
            distance = max(0.0, min(distance, 5000.0))
            
            kill_event = {
                "guild_id": guild_id,
                "server_id": server_id,
                "timestamp": kill_data.get("timestamp", datetime.now(timezone.utc)),
                "killer": kill_data.get("killer", ""),
                "killer_id": kill_data.get("killer_id", ""),
                "victim": kill_data.get("victim", ""),
                "victim_id": kill_data.get("victim_id", ""),
                "weapon": kill_data.get("weapon", ""),
                "distance": distance,  # Now properly validated numeric value
                "killer_platform": kill_data.get("killer_platform", ""),
                "victim_platform": kill_data.get("victim_platform", ""),
                "is_suicide": kill_data.get("is_suicide", False),
                "raw_line": kill_data.get("raw_line", "")
            }

            await self.kill_events.insert_one(kill_event)
            logger.debug(f"Added kill event: {kill_data['killer']} -> {kill_data['victim']} (distance: {distance}m)")

        except Exception as e:
            logger.error(f"Failed to add kill event: {e}")

    async def increment_player_kill(self, guild_id: int, server_id: str, player_name: str, distance: float = 0.0):
        """Increment player kill count and update streak/distance stats with enhanced distance tracking"""
        try:
            # PHASE 1 FIX: Ensure distance is properly validated and tracked
            if isinstance(distance, str):
                try:
                    distance = float(distance) if distance else 0.0
                except (ValueError, TypeError):
                    distance = 0.0
            distance = max(0.0, min(distance, 5000.0))  # Validate range
            # Get current stats to calculate new longest distance and streak
            current_stats = await self.get_pvp_stats(guild_id, server_id, player_name)

            # Calculate new streak and longest distance
            current_streak = current_stats.get('current_streak', 0) if current_stats else 0
            new_streak = current_streak + 1
            longest_streak = max(current_stats.get('longest_streak', 0) if current_stats else 0, new_streak)
            personal_best_distance = max(current_stats.get('personal_best_distance', 0.0) if current_stats else 0.0, distance)

            # Use atomic increment for kills and total_distance
            await self.update_pvp_stats(guild_id, server_id, player_name, {"kills": 1})

            # Add distance to total_distance (accumulated)
            if distance > 0:
                await self.update_pvp_stats(guild_id, server_id, player_name, {"total_distance": distance})

            # Update streak and personal best separately
            await self.update_pvp_stats(
                guild_id, server_id, player_name,
                {
                    "current_streak": new_streak,
                    "longest_streak": longest_streak,
                    "personal_best_distance": personal_best_distance
                }
            )

        except Exception as e:
            logger.error(f"Failed to increment player kill: {e}")

    async def increment_player_death(self, guild_id: int, server_id: str, player_name: str):
        """Increment player death count and reset streak"""
        try:
            await self.update_pvp_stats(
                guild_id, server_id, player_name,
                {"deaths": 1}
            )
            # Reset streak separately
            await self.reset_player_streak(guild_id, server_id, player_name)

        except Exception as e:
            logger.error(f"Failed to increment player death: {e}")

    async def find_player_by_character_name(self, guild_id: int, character_name: str) -> Optional[Dict]:
        """Find a player document by searching linked character names (case-insensitive, space-normalized)"""
        try:
            # Normalize the search term
            normalized_search = ' '.join(character_name.strip().split())

            # Create case-insensitive regex pattern
            escaped_search = normalized_search.replace(' ', r'\s+')
            pattern = f"^{escaped_search}$"

            player_doc = await self.pvp_data.find_one({
                "guild_id": guild_id,
                "player_name": {
                    "$regex": pattern,
                    "$options": "i"
                }
            })

            return player_doc

        except Exception as e:
            logger.error(f"Failed to find player by character name: {e}")
            return None

    async def get_recent_kills(self, guild_id: int, server_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent kill events for server"""
        cursor = self.kill_events.find(
            {"guild_id": guild_id, "server_id": server_id}
        ).sort("timestamp", -1).limit(limit)

        return await cursor.to_list(length=limit)

    # ECONOMY (Guild-scoped)
    async def get_wallet(self, guild_id: int, discord_id: int) -> Dict[str, Any]:
        """Get user wallet (guild-scoped)"""
        wallet = await self.economy.find_one({"guild_id": guild_id, "discord_id": discord_id})

        if not wallet:
            wallet = {
                "guild_id": guild_id,
                "discord_id": discord_id,
                "balance": 0,
                "total_earned": 0,
                "total_spent": 0,
                "created_at": datetime.now(timezone.utc)
            }
            await self.economy.insert_one(wallet)

        return wallet

    async def update_wallet(self, guild_id: int, discord_id: int, amount: int, 
                           transaction_type: str) -> bool:
        """Update user wallet balance"""
        try:
            inc_updates = {"balance": amount}
            if amount > 0:
                inc_updates["total_earned"] = amount
            else:
                inc_updates["total_spent"] = abs(amount)

            update_query = {
                "$inc": inc_updates,
                "$set": {"last_updated": datetime.now(timezone.utc)}
            }

            result = await self.economy.update_one(
                {"guild_id": guild_id, "discord_id": discord_id},
                update_query,
                upsert=True
            )

            return result.acknowledged

        except Exception as e:
            logger.error(f"Failed to update wallet: {e}")
            return False

    # PREMIUM (Server-scoped)
    async def set_premium_status(self, guild_id: int, server_id: str, 
                                expires_at: Optional[datetime] = None) -> bool:
        """Set premium status for specific server"""
        try:
            # Ensure expires_at is timezone-aware if provided
            if expires_at and expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            premium_doc = {
                "guild_id": guild_id,
                "server_id": server_id,
                "active": expires_at is not None,
                "expires_at": expires_at,
                "updated_at": datetime.now(timezone.utc)
            }

            await self.premium.update_one(
                {"guild_id": guild_id, "server_id": server_id},
                {"$set": premium_doc},
                upsert=True
            )

            return True

        except Exception as e:
            logger.error(f"Failed to set premium status: {e}")
            return False

    async def is_premium_server(self, guild_id: int, server_id: str) -> bool:
        """Check if server has active premium"""
        premium_doc = await self.premium.find_one({"guild_id": guild_id, "server_id": server_id})

        if not premium_doc or not premium_doc.get("active"):
            return False

        expires_at = premium_doc.get("expires_at")
        if expires_at:
            # Ensure both datetimes are timezone-aware for comparison
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)

            current_time = datetime.now(timezone.utc)

            if expires_at < current_time:
                # Premium expired, update status
                await self.set_premium_status(guild_id, server_id, None)
                return False

        return True

    # LEADERBOARDS
    async def get_leaderboard(self, guild_id: int, server_id: str, stat: str = "kills", 
                             limit: int = 10) -> List[Dict[str, Any]]:
        """Get leaderboard for specific stat"""
        sort_order = -1 if stat in ["kills", "kdr", "longest_streak"] else 1

        cursor = self.pvp_data.find(
            {"guild_id": guild_id, "server_id": server_id}
        ).sort(stat, sort_order).limit(limit)

        return await cursor.to_list(length=limit)

    # LOG PARSER SUPPORT METHODS
    async def get_active_premium_servers(self) -> List[Dict[str, Any]]:
        """Get all active premium servers for log parser"""
        try:
            # Find all premium servers that are active and not expired
            current_time = datetime.now(timezone.utc)
            
            premium_servers = await self.premium.find({
                "active": True,
                "$or": [
                    {"expires_at": {"$gt": current_time}},
                    {"expires_at": None}
                ]
            }).to_list(length=None)

            # Get server names from guild configurations
            result = []
            for premium_doc in premium_servers:
                guild_id = premium_doc.get("guild_id")
                server_id = premium_doc.get("server_id")
                
                # Find the guild config to get server name
                guild_config = await self.guilds.find_one({"guild_id": guild_id})
                if guild_config:
                    servers = guild_config.get("servers", [])
                    for server in servers:
                        if str(server.get("_id")) == str(server_id):
                            result.append({
                                "server_id": server_id,
                                "server_name": server.get("name", f"Server {server_id}"),
                                "guild_id": guild_id,
                                "expires_at": premium_doc.get("expires_at")
                            })
                            break

            return result

        except Exception as e:
            logger.error(f"Failed to get active premium servers: {e}")
            return []

    async def get_recent_log_events(self, server_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent log events for a server"""
        try:
            cursor = self.kill_events.find(
                {"server_id": server_id}
            ).sort("timestamp", -1).limit(limit)

            return await cursor.to_list(length=limit)

        except Exception as e:
            logger.error(f"Failed to get recent log events: {e}")
            return []

    async def get_current_online_count(self, server_id: str) -> int:
        """Get current online player count for a server"""
        try:
            # This would typically come from a separate online players collection
            # For now, return a placeholder based on recent activity
            recent_events = await self.get_recent_log_events(server_id, 10)
            
            # Count unique players from recent events (last hour)
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            recent_players = set()
            
            for event in recent_events:
                event_time = event.get("timestamp")
                if event_time and event_time > one_hour_ago:
                    if event.get("killer"):
                        recent_players.add(event.get("killer"))
                    if event.get("victim"):
                        recent_players.add(event.get("victim"))

            return len(recent_players)

        except Exception as e:
            logger.error(f"Failed to get current online count: {e}")
            return 0