"""
Emerald's Killfeed - Fixed Leaderboard System
Properly themed leaderboards using EmbedFactory
"""

import discord
from discord.ext import commands
import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
# Removed autocomplete import to fix loading issues
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class LeaderboardsFixed(commands.Cog):
    """Fixed leaderboard commands that actually use the themed factory"""

    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(
        name="leaderboard",
        description="View properly themed leaderboards"
    )
    async def leaderboard(self, ctx: discord.ApplicationContext,
                         stat: discord.Option(str, "Statistic to display", 
                                            choices=['kills', 'deaths', 'kdr', 'distance', 'weapons', 'factions']),
                         server: discord.Option(str, "Server to view stats for", required=False)):
        """Display properly themed leaderboard"""
        await ctx.defer()

        try:
            guild_id = ctx.guild.id if ctx.guild else None
            if not guild_id:
                await ctx.followup.send("This command can only be used in a server!", ephemeral=True)
                return

            # Get guild configuration
            guild_doc = await self.bot.db_manager.get_guild(guild_id)
            if not guild_doc or not guild_doc.get('servers'):
                await ctx.followup.send("No servers configured for this guild. Use `/addserver` first!", ephemeral=True)
                return

            # Select server
            if server:
                selected_server = None
                for server_config in guild_doc['servers']:
                    if server_config.get('name', '').lower() == server.lower() or server_config.get('server_id', '') == server:
                        selected_server = server_config
                        break

                if not selected_server:
                    await ctx.followup.send(f"Server '{server}' not found!", ephemeral=True)
                    return
            else:
                selected_server = guild_doc['servers'][0]

            server_id = selected_server.get('server_id', selected_server.get('_id', 'default'))
            server_name = selected_server.get('name', f'Server {server_id}')

            # Create themed leaderboard using EmbedFactory
            embed, file = await self.create_themed_leaderboard(guild_id, server_id, server_name, stat)

            if embed:
                await ctx.followup.send(embed=embed, files=[file] if file else None)
            else:
                await ctx.followup.send(f"No data available for {stat} leaderboard on {server_name}!", ephemeral=True)

        except Exception as e:
            logger.error(f"Failed to show leaderboard: {e}")
            await ctx.followup.send("Failed to load leaderboard. Please try again later.", ephemeral=True)

    async def get_player_faction(self, guild_id: int, player_name: str) -> Optional[str]:
        """Get player's faction tag if they have one"""
        try:
            # Look up player in factions collection
            faction_doc = await self.bot.db_manager.factions.find_one({
                "guild_id": guild_id,
                "members": player_name
            })
            return faction_doc.get('faction_name') if faction_doc else None
        except Exception:
            return None

    async def format_leaderboard_line(self, rank: int, player: Dict[str, Any], stat_type: str, guild_id: int) -> str:
        """Format a single leaderboard line with faction tags and proper styling"""
        player_name = player.get('player_name', 'Unknown')

        # Get faction tag
        faction = await self.get_player_faction(guild_id, player_name)
        faction_tag = f" [{faction}]" if faction else ""

        # Format rank with emoji styling for top 3
        if rank == 1:
            rank_display = "🥇"
        elif rank == 2:
            rank_display = "🥈" 
        elif rank == 3:
            rank_display = "🥉"
        else:
            rank_display = f"**{rank}.**"

        # Format value based on stat type - show only the relevant stat
        if stat_type == 'kills':
            kills = player.get('kills', 0)
            value = f"{kills:,} Kills"

        elif stat_type == 'deaths':
            deaths = player.get('deaths', 0)
            value = f"{deaths:,} Deaths"

        elif stat_type == 'kdr':
            kdr = player.get('kdr', 0.0)
            kills = player.get('kills', 0)
            deaths = player.get('deaths', 0)

            # Recalculate KDR if it's 0 but we have kills/deaths data
            if kdr == 0.0 and kills > 0:
                kdr = kills / max(deaths, 1)

            value = f"KDR: {kdr:.2f} ({kills:,}/{deaths:,})"

        elif stat_type == 'distance':
            total_distance = player.get('total_distance', 0)
            best_distance = player.get('personal_best_distance', 0)

            # Show best distance as primary stat with total in parentheses
            if best_distance > 1000:
                best_str = f"{best_distance/1000:.1f}km"
            else:
                best_str = f"{best_distance:.1f}m"

            if total_distance > 1000:
                total_str = f"{total_distance/1000:.1f}km"
            else:
                total_str = f"{total_distance:.1f}m"

            value = f"Best: {best_str} (Total: {total_str})"

        else:
            value = str(player.get(stat_type, 0))

        return f"{rank_display} {player_name}{faction_tag} — {value}"

    async def create_themed_leaderboard(self, guild_id: int, server_id: str, stat_type: str, server_name: str) -> Tuple[Optional[discord.Embed], Optional[discord.File]]:
        """Create properly themed leaderboard using EmbedFactory"""
        try:
            # Themed title pools for each stat type
            title_pools = {
                'kills': ["Top Operators", "Elite Eliminators", "Death Dealers", "Blood Money Rankings"],
                'deaths': ["Most Fallen", "Casualty Reports", "Frequent Respawners", "Battle Casualties"],
                'kdr': ["Combat Efficiency", "Kill/Death Masters", "Survival Experts", "Tactical Legends"],
                'distance': ["Precision Masters", "Long Range Snipers", "Distance Champions", "Eagle Eyes"],
                'weapons': ["Arsenal Analysis", "Weapon Mastery", "Combat Tools", "Death Dealers"],
                'factions': ["Faction Dominance", "Alliance Power", "Territory Control", "War Machine"]
            }

            # Themed descriptions
            descriptions = {
                'kills': "Most eliminations on the battlefield",
                'deaths': "Those who've fallen in the line of duty",
                'kdr': "Elite warriors with the highest efficiency",
                'distance': "Snipers who strike from afar",
                'weapons': "Most lethal tools of war",
                'factions': "Dominant forces in the wasteland"
            }

            if stat_type == 'kills':
                # Custom aggregation for kills leaderboard with proper sorting
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id}},
                    {"$group": {
                        "_id": "$player_name",
                        "player_name": {"$first": "$player_name"},
                        "kills": {"$sum": "$kills"},
                        "deaths": {"$sum": "$deaths"},
                        "total_distance": {"$sum": "$total_distance"}
                    }},
                    {"$addFields": {
                        "kdr": {"$cond": {
                            "if": {"$gt": ["$deaths", 0]},
                            "then": {"$divide": ["$kills", "$deaths"]},
                            "else": "$kills"
                        }}
                    }},
                    {"$sort": {"kills": -1}},
                    {"$limit": 10}
                ]
                players = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)
                title = f"{random.choice(title_pools['kills'])} - {server_name}"
                description = descriptions['kills']

            elif stat_type == 'deaths':
                # Custom aggregation for deaths leaderboard with proper sorting
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id}},
                    {"$group": {
                        "_id": "$player_name",
                        "player_name": {"$first": "$player_name"},
                        "kills": {"$sum": "$kills"},
                        "deaths": {"$sum": "$deaths"},
                        "total_distance": {"$sum": "$total_distance"}
                    }},
                    {"$addFields": {
                        "kdr": {"$cond": {
                            "if": {"$gt": ["$deaths", 0]},
                            "then": {"$divide": ["$kills", "$deaths"]},
                            "else": "$kills"
                        }}
                    }},
                    {"$sort": {"deaths": -1}},
                    {"$limit": 10}
                ]
                players = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)
                title = f"{random.choice(title_pools['deaths'])} - {server_name}"
                description = descriptions['deaths']

            elif stat_type == 'kdr':
                # Custom aggregation for KDR leaderboard with minimum kills requirement
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id}},
                    {"$group": {
                        "_id": "$player_name",
                        "player_name": {"$first": "$player_name"},
                        "kills": {"$sum": "$kills"},
                        "deaths": {"$sum": "$deaths"},
                        "total_distance": {"$sum": "$total_distance"}
                    }},
                    {"$match": {"kills": {"$gte": 1}}},  # Minimum 1 kill for KDR leaderboard
                    {"$addFields": {
                        "kdr": {"$cond": {
                            "if": {"$gt": ["$deaths", 0]},
                            "then": {"$divide": ["$kills", "$deaths"]},
                            "else": "$kills"
                        }}
                    }},
                    {"$sort": {"kdr": -1}},
                    {"$limit": 10}
                ]
                players = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)
                title = f"{random.choice(title_pools['kdr'])} - {server_name}"
                description = descriptions['kdr']

            elif stat_type == 'distance':
                # PHASE 1 FIX: Enhanced distance leaderboard with proper formatting
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id, "distance": {"$gt": 0}}},
                    {"$group": {
                        "_id": "$killer",
                        "best_distance": {"$max": "$distance"},
                        "total_long_range_kills": {"$sum": {"$cond": [{"$gt": ["$distance", 100]}, 1, 0]}}
                    }},
                    {"$sort": {"best_distance": -1}},
                    {"$limit": 10}
                ]
                
                cursor = self.bot.db_manager.kill_events.aggregate(pipeline)
                results = await cursor.to_list(length=10)
                
                if results:
                    leaderboard_text = ""
                    for i, entry in enumerate(results, 1):
                        player_name = entry['_id']
                        distance = entry['best_distance']
                        long_range_kills = entry['total_long_range_kills']
                        
                        # PHASE 1 FIX: Proper distance formatting
                        if distance >= 1000:
                            distance_str = f"{distance/1000:.1f}km"
                        else:
                            distance_str = f"{distance:.0f}m"
                        
                        leaderboard_text += f"**{i}.** {player_name} - {distance_str}\n"
                        leaderboard_text += f"    Long range kills: {long_range_kills}\n\n"
                else:
                    leaderboard_text = "No distance data available yet"
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id}},
                    {"$group": {
                        "_id": "$player_name",
                        "player_name": {"$first": "$player_name"},
                        "kills": {"$sum": "$kills"},
                        "deaths": {"$sum": "$deaths"},
                        "total_distance": {"$sum": "$total_distance"},
                        "personal_best_distance": {"$max": "$personal_best_distance"}
                    }},
                    {"$match": {"total_distance": {"$gt": 0}}},  # Only players with distance kills
                    {"$addFields": {
                        "kdr": {"$cond": {
                            "if": {"$gt": ["$deaths", 0]},
                            "then": {"$divide": ["$kills", "$deaths"]},
                            "else": "$kills"
                        }}
                    }},
                    {"$sort": {"personal_best_distance": -1}},  # Sort by longest single kill distance
                    {"$limit": 10}
                ]
                players = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)
                title = f"{random.choice(title_pools['distance'])} - {server_name}"
                description = descriptions['distance']

            elif stat_type == 'weapons':
                # Get weapon stats with enhanced aggregation - exclude suicide weapons
                pipeline = [
                    {"$match": {
                        "guild_id": guild_id, 
                        "server_id": server_id,
                        "is_suicide": False,  # Only actual PvP kills
                        "weapon": {"$nin": ["Menu Suicide", "Suicide", "Falling", "suicide_by_relocation"]}  # Exclude suicide methods
                    }},
                    {"$group": {
                        "_id": "$weapon",
                        "kills": {"$sum": 1},
                        "top_killer": {"$first": "$killer"}
                    }},
                    {"$sort": {"kills": -1}},
                    {"$limit": 10}
                ]
                weapons_data = await self.bot.db_manager.kill_events.aggregate(pipeline).to_list(length=None)

                if not weapons_data:
                    return None, None

                leaderboard_text = []
                for i, weapon in enumerate(weapons_data, 1):
                    weapon_name = weapon['_id'] or 'Unknown'
                    kills = weapon['kills']
                    top_killer = weapon['top_killer'] or 'Unknown'

                    # Enhanced weapon formatting with emoji ranks
                    if i == 1:
                        rank_display = "🥇"
                    elif i == 2:
                        rank_display = "🥈" 
                    elif i == 3:
                        rank_display = "🥉"
                    else:
                        rank_display = f"**{i}.**"

                    # Get faction for top killer
                    faction = await self.get_player_faction(guild_id, top_killer)
                    faction_tag = f" [{faction}]" if faction else ""

                    # Clean weapon name formatting
                    if weapon_name and weapon_name != 'Unknown':
                        leaderboard_text.append(f"{rank_display} {weapon_name} — {kills:,} Kills | Top: {top_killer}{faction_tag}")
                    else:
                        leaderboard_text.append(f"{rank_display} Unknown Weapon — {kills:,} Kills | Top: {top_killer}{faction_tag}")

                title = f"{random.choice(title_pools['weapons'])} - {server_name}"
                embed_data = {
                    'title': title,
                    'description': descriptions['weapons'],
                    'rankings': "\n".join(leaderboard_text),
                    'total_kills': sum(w['kills'] for w in weapons_data),
                    'total_deaths': 0,
                    'stat_type': 'weapons',
                    'style_variant': 'weapons',
                    'server_name': server_name,
                    'thumbnail_url': 'attachment://WeaponStats.png'
                }

                embed, file = await EmbedFactory.build('leaderboard', embed_data)
                return embed, file

            elif stat_type == 'factions':
                # Get faction stats aggregated across all players
                pipeline = [
                    {"$match": {"guild_id": guild_id, "server_id": server_id}},
                    {"$group": {
                        "_id": "$player_name",
                        "kills": {"$sum": "$kills"},
                        "deaths": {"$sum": "$deaths"}
                    }}
                ]
                player_stats = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)

                # Group by faction
                faction_stats = {}
                for player_stat in player_stats:
                    player_name = player_stat['_id']
                    faction = await self.get_player_faction(guild_id, player_name)

                    if faction:
                        if faction not in faction_stats:
                            faction_stats[faction] = {'kills': 0, 'deaths': 0, 'members': 0}

                        faction_stats[faction]['kills'] += player_stat.get('kills', 0)
                        faction_stats[faction]['deaths'] += player_stat.get('deaths', 0)
                        faction_stats[faction]['members'] += 1

                if not faction_stats:
                    return None, None

                # Sort by kills
                sorted_factions = sorted(faction_stats.items(), key=lambda x: x[1]['kills'], reverse=True)[:10]

                leaderboard_text = []
                for i, (faction_name, stats) in enumerate(sorted_factions, 1):
                    kills = stats['kills']
                    deaths = stats['deaths']
                    members = stats['members']
                    kdr = kills / max(deaths, 1) if deaths > 0 else kills

                    if i == 1:
                        rank_display = "🥇"
                    elif i == 2:
                        rank_display = "🥈" 
                    elif i == 3:
                        rank_display = "🥉"
                    else:
                        rank_display = f"**{i}.**"

                    # Format faction line with bracket notation
                    parts = [f"{kills:,} Kills"]
                    if kdr > 0 and deaths > 0:
                        parts.append(f"KDR: {kdr:.2f}")
                    parts.append(f"{members} Members")

                    leaderboard_text.append(f"{rank_display} [{faction_name}] — {' | '.join(parts)}")

                title = f"{random.choice(title_pools['factions'])} - {server_name}"
                embed_data = {
                    'title': title,
                    'description': descriptions['factions'],
                    'rankings': "\n".join(leaderboard_text),
                    'total_kills': sum(f[1]['kills'] for f in sorted_factions),
                    'total_deaths': sum(f[1]['deaths'] for f in sorted_factions),
                    'stat_type': 'factions',
                    'style_variant': 'factions',
                    'server_name': server_name,
                    'thumbnail_url': 'attachment://Faction.png'
                }

                embed, file = await EmbedFactory.build('leaderboard', embed_data)
                return embed, file

            else:
                return None, None

            if not players:
                return None, None

            # Create professional leaderboard text with advanced formatting
            leaderboard_text = []
            for i, player in enumerate(players, 1):
                formatted_line = await self.format_leaderboard_line(i, player, stat_type, guild_id)
                leaderboard_text.append(formatted_line)

            # All leaderboards use Leaderboard.png
            thumbnail_map = {
                'kills': 'attachment://Leaderboard.png',
                'deaths': 'attachment://Leaderboard.png',
                'kdr': 'attachment://Leaderboard.png',
                'distance': 'attachment://Leaderboard.png'
            }

            # Use EmbedFactory for proper theming with dynamic styling
            embed_data = {
                'title': title,
                'description': description,
                'rankings': "\n".join(leaderboard_text),
                'total_kills': sum(p.get('kills', 0) for p in players),
                'total_deaths': sum(p.get('deaths', 0) for p in players),
                'stat_type': stat_type,
                'style_variant': stat_type,
                'server_name': server_name,
                'thumbnail_url': thumbnail_map.get(stat_type, 'attachment://Leaderboard.png')
            }

            embed, file = await EmbedFactory.build('leaderboard', embed_data)
            return embed, file

        except Exception as e:
            logger.error(f"Failed to create themed leaderboard: {e}")
            return None, None

def setup(bot):
    bot.add_cog(LeaderboardsFixed(bot))