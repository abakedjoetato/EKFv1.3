"""
Emerald's Killfeed - Bounty System (PHASE 7)
Manual bounties via /bounty set <target> <amount> (24h lifespan)
AI auto-bounties based on hourly kill performance
Must match linked killer to claimed target
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

import discord
from discord.ext import commands

logger = logging.getLogger(__name__)

class Bounties(commands.Cog):
    """
    BOUNTIES (PREMIUM)
    - Manual bounties via /bounty set <target> <amount> (24h lifespan)
    - AI auto-bounties based on hourly kill performance
    - Must match linked killer to claimed target
    """
    
    def __init__(self, bot):
        self.bot = bot
        
    async def check_premium_server(self, guild_id: int) -> bool:
        """Check if guild has premium access for bounty features"""
        guild_doc = await self.bot.db_manager.get_guild(guild_id)
        if not guild_doc:
            return False
        
        servers = guild_doc.get('servers', [])
        for server_config in servers:
            server_id = server_config.get('server_id', server_config.get('_id', 'default'))
            if await self.bot.db_manager.is_premium_server(guild_id, server_id):
                return True
        
        return False
    
    async def get_player_character_names(self, guild_id: int, discord_id: int) -> List[str]:
        """Get all character names for a Discord user"""
        player_data = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
        return player_data['linked_characters'] if player_data else []
    
    async def find_discord_user_by_character(self, guild_id: int, character_name: str) -> Optional[int]:
        """Find Discord user ID by character name"""
        player_data = await self.bot.db_manager.players.find_one({
            'guild_id': guild_id,
            'linked_characters': character_name
        })
        return player_data['discord_id'] if player_data else None
    
    async def add_wallet_event(self, guild_id: int, discord_id: int, 
                              amount: int, event_type: str, description: str):
        """Add wallet transaction event for tracking"""
        try:
            event_doc = {
                "guild_id": guild_id,
                "discord_id": discord_id,
                "amount": amount,
                "event_type": event_type,
                "description": description,
                "timestamp": datetime.now(timezone.utc)
            }
            
            await self.bot.db_manager.db.wallet_events.insert_one(event_doc)
            
        except Exception as e:
            logger.error(f"Failed to add wallet event: {e}")
    
    bounty = discord.SlashCommandGroup("bounty", "Bounty system commands")
    
    @bounty.command(name="set", description="Set a bounty on a player")
    async def bounty_set(self, ctx: discord.ApplicationContext, target: str, amount: int):
        """Set a bounty on a target character"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Bounty system requires premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Validate amount
            if amount <= 0:
                await ctx.respond("❌ Bounty amount must be positive!", ephemeral=True)
                return
            
            if amount < 100:
                await ctx.respond("❌ Minimum bounty amount is $100!", ephemeral=True)
                return
            
            if amount > 50000:
                await ctx.respond("❌ Maximum bounty amount is $50,000!", ephemeral=True)
                return
            
            # Check if user has enough money
            wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
            if wallet['balance'] < amount:
                await ctx.respond(
                    f"❌ Insufficient funds! You have **${wallet['balance']:,}** but need **${amount:,}**",
                    ephemeral=True
                )
                return
            
            # Validate target character name
            target = target.strip()
            if not target:
                await ctx.respond("❌ Target character name cannot be empty!", ephemeral=True)
                return
            
            # Check if target is linked to someone
            target_discord_id = await self.find_discord_user_by_character(guild_id, target)
            if not target_discord_id:
                await ctx.respond(f"❌ Character **{target}** is not linked to any Discord account!", ephemeral=True)
                return
            
            # Prevent self-bounties
            user_characters = await self.get_player_character_names(guild_id, discord_id)
            if target in user_characters:
                await ctx.respond("❌ You cannot set a bounty on yourself!", ephemeral=True)
                return
            
            # Check if bounty already exists
            existing_bounty = await self.bot.db_manager.bounties.find_one({
                'guild_id': guild_id,
                'target_character': target,
                'active': True,
                'expires_at': {'$gt': datetime.now(timezone.utc)}
            })
            
            if existing_bounty:
                await ctx.respond(f"❌ There is already an active bounty on **{target}**!", ephemeral=True)
                return
            
            # Deduct money from user
            success = await self.bot.db_manager.update_wallet(guild_id, discord_id, -amount, "bounty_set")
            
            if not success:
                await ctx.respond("❌ Failed to process payment. Please try again.", ephemeral=True)
                return
            
            # Create bounty
            bounty_doc = {
                'guild_id': guild_id,
                'target_character': target,
                'target_discord_id': target_discord_id,
                'issuer_discord_id': discord_id,
                'amount': amount,
                'active': True,
                'claimed': False,
                'created_at': datetime.now(timezone.utc),
                'expires_at': datetime.now(timezone.utc) + timedelta(hours=24),
                'auto_generated': False
            }
            
            await self.bot.db_manager.bounties.insert_one(bounty_doc)
            
            # Add wallet event
            await self.add_wallet_event(
                guild_id, discord_id, -amount, "bounty_set",
                f"Set bounty on {target} for ${amount:,}"
            )
            
            # Create bounty embed
            embed = discord.Embed(
                title="🎯 Bounty Set",
                description=f"A bounty has been placed on **{target}**!",
                color=0xFF4500,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="💰 Reward",
                value=f"**${amount:,}**",
                inline=True
            )
            
            embed.add_field(
                name="⏰ Expires",
                value=f"<t:{int((datetime.now(timezone.utc) + timedelta(hours=24)).timestamp())}:R>",
                inline=True
            )
            
            embed.add_field(
                name="👤 Target",
                value=target,
                inline=True
            )
            
            embed.add_field(
                name="📋 Instructions",
                value="Kill the target to claim the bounty!\nBounty expires in 24 hours.",
                inline=False
            )
            
            embed.set_thumbnail(url="attachment://Bounty.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to set bounty: {e}")
            await ctx.respond("❌ Failed to set bounty.", ephemeral=True)
    
    @bounty.command(name="list", description="List active bounties")
    async def bounty_list(self, ctx: discord.ApplicationContext):
        """List all active bounties"""
        try:
            guild_id = ctx.guild.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Bounty system requires premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Get active bounties
            cursor = self.bot.db_manager.bounties.find({
                'guild_id': guild_id,
                'active': True,
                'expires_at': {'$gt': datetime.now(timezone.utc)}
            }).sort('amount', -1)
            
            bounties = await cursor.to_list(length=20)
            
            if not bounties:
                embed = discord.Embed(
                    title="🎯 Active Bounties",
                    description="No active bounties found!",
                    color=0x808080,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                await ctx.respond(embed=embed)
                return
            
            # Create bounty list embed
            embed = discord.Embed(
                title="🎯 Active Bounties",
                description=f"**{len(bounties)}** active bounties",
                color=0xFF4500,
                timestamp=datetime.now(timezone.utc)
            )
            
            bounty_list = []
            for i, bounty in enumerate(bounties[:10], 1):  # Show top 10
                target = bounty['target_character']
                amount = bounty['amount']
                expires = bounty['expires_at']
                auto = " 🤖" if bounty.get('auto_generated', False) else ""
                
                bounty_list.append(
                    f"**{i}.** {target} - **${amount:,}**{auto}\n"
                    f"    Expires <t:{int(expires.timestamp())}:R>"
                )
            
            embed.add_field(
                name="💰 Bounty List",
                value="\n".join(bounty_list),
                inline=False
            )
            
            if len(bounties) > 10:
                embed.add_field(
                    name="📊 Note",
                    value=f"Showing top 10 of {len(bounties)} bounties",
                    inline=False
                )
            
            embed.set_thumbnail(url="attachment://Bounty.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers • 🤖 = Auto-generated")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to list bounties: {e}")
            await ctx.respond("❌ Failed to retrieve bounties.", ephemeral=True)
    
    async def check_bounty_claims(self, guild_id: int, killer_character: str, victim_character: str):
        """Check if a kill claims any bounties"""
        try:
            # Find active bounties on the victim
            active_bounties = await self.bot.db_manager.bounties.find({
                'guild_id': guild_id,
                'target_character': victim_character,
                'active': True,
                'claimed': False,
                'expires_at': {'$gt': datetime.now(timezone.utc)}
            }).to_list(length=None)
            
            if not active_bounties:
                return
            
            # Find Discord ID of killer
            killer_discord_id = await self.find_discord_user_by_character(guild_id, killer_character)
            if not killer_discord_id:
                return  # Killer not linked, can't claim bounty
            
            # Process each bounty
            for bounty in active_bounties:
                await self._claim_bounty(guild_id, bounty, killer_discord_id, killer_character)
            
        except Exception as e:
            logger.error(f"Failed to check bounty claims: {e}")
    
    async def _claim_bounty(self, guild_id: int, bounty: Dict[str, Any], 
                           killer_discord_id: int, killer_character: str):
        """Process a bounty claim"""
        try:
            bounty_amount = bounty['amount']
            target_character = bounty['target_character']
            
            # Mark bounty as claimed
            await self.bot.db_manager.bounties.update_one(
                {'_id': bounty['_id']},
                {
                    '$set': {
                        'claimed': True,
                        'active': False,
                        'claimed_at': datetime.now(timezone.utc),
                        'claimer_discord_id': killer_discord_id,
                        'claimer_character': killer_character
                    }
                }
            )
            
            # Award money to killer
            await self.bot.db_manager.update_wallet(
                guild_id, killer_discord_id, bounty_amount, "bounty_claim"
            )
            
            # Add wallet event
            await self.add_wallet_event(
                guild_id, killer_discord_id, bounty_amount, "bounty_claim",
                f"Claimed bounty on {target_character} for ${bounty_amount:,}"
            )
            
            # Send bounty claimed notification
            await self._send_bounty_claimed_embed(guild_id, bounty, killer_discord_id, killer_character)
            
            logger.info(f"Bounty claimed: {killer_character} killed {target_character} for ${bounty_amount:,}")
            
        except Exception as e:
            logger.error(f"Failed to claim bounty: {e}")
    
    async def _send_bounty_claimed_embed(self, guild_id: int, bounty: Dict[str, Any], 
                                        killer_discord_id: int, killer_character: str):
        """Send bounty claimed notification"""
        try:
            # Get guild channels
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return
            
            killfeed_channel_id = guild_config.get('channels', {}).get('killfeed')
            if not killfeed_channel_id:
                return
            
            channel = self.bot.get_channel(killfeed_channel_id)
            if not channel:
                return
            
            embed = discord.Embed(
                title="💰 Bounty Claimed!",
                description=f"**{killer_character}** has claimed the bounty on **{bounty['target_character']}**!",
                color=0x00FF00,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="💰 Reward",
                value=f"**${bounty['amount']:,}**",
                inline=True
            )
            
            embed.add_field(
                name="🎯 Hunter",
                value=f"<@{killer_discord_id}>",
                inline=True
            )
            
            embed.add_field(
                name="💀 Target",
                value=bounty['target_character'],
                inline=True
            )
            
            if bounty.get('auto_generated', False):
                embed.add_field(
                    name="🤖 Type",
                    value="Auto-generated bounty",
                    inline=False
                )
            
            embed.set_thumbnail(url="attachment://Bounty.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to send bounty claimed embed: {e}")
    
    async def generate_auto_bounties(self, guild_id: int):
        """Generate automatic bounties based on kill performance"""
        try:
            # Get top killers from the last hour
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
            
            pipeline = [
                {
                    '$match': {
                        'guild_id': guild_id,
                        'timestamp': {'$gte': one_hour_ago},
                        'is_suicide': False
                    }
                },
                {
                    '$group': {
                        '_id': '$killer',
                        'kill_count': {'$sum': 1}
                    }
                },
                {
                    '$match': {
                        'kill_count': {'$gte': 5}  # Minimum 5 kills in an hour
                    }
                },
                {
                    '$sort': {'kill_count': -1}
                },
                {
                    '$limit': 3  # Top 3 performers
                }
            ]
            
            top_killers = await self.bot.db_manager.kill_events.aggregate(pipeline).to_list(length=None)
            
            for killer_data in top_killers:
                killer_name = killer_data['_id']
                kill_count = killer_data['kill_count']
                
                # Check if there's already a bounty on this player
                existing_bounty = await self.bot.db_manager.bounties.find_one({
                    'guild_id': guild_id,
                    'target_character': killer_name,
                    'active': True,
                    'expires_at': {'$gt': datetime.now(timezone.utc)}
                })
                
                if existing_bounty:
                    continue  # Skip if already has bounty
                
                # Calculate bounty amount based on performance
                base_amount = 1000
                performance_bonus = (kill_count - 4) * 500  # Extra $500 per kill above 5
                bounty_amount = min(base_amount + performance_bonus, 10000)  # Cap at $10k
                
                # Find target Discord ID
                target_discord_id = await self.find_discord_user_by_character(guild_id, killer_name)
                if not target_discord_id:
                    continue  # Skip if not linked
                
                # Create auto-bounty
                bounty_doc = {
                    'guild_id': guild_id,
                    'target_character': killer_name,
                    'target_discord_id': target_discord_id,
                    'issuer_discord_id': None,  # System-generated
                    'amount': bounty_amount,
                    'active': True,
                    'claimed': False,
                    'created_at': datetime.now(timezone.utc),
                    'expires_at': datetime.now(timezone.utc) + timedelta(hours=2),  # 2 hour lifespan
                    'auto_generated': True,
                    'trigger_kills': kill_count
                }
                
                await self.bot.db_manager.bounties.insert_one(bounty_doc)
                
                # Send auto-bounty notification
                await self._send_auto_bounty_embed(guild_id, killer_name, bounty_amount, kill_count)
                
                logger.info(f"Auto-bounty generated: {killer_name} (${bounty_amount:,}) for {kill_count} kills")
                
        except Exception as e:
            logger.error(f"Failed to generate auto-bounties: {e}")
    
    async def _send_auto_bounty_embed(self, guild_id: int, target_name: str, 
                                     amount: int, kill_count: int):
        """Send auto-bounty notification"""
        try:
            # Get guild channels
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return
            
            killfeed_channel_id = guild_config.get('channels', {}).get('killfeed')
            if not killfeed_channel_id:
                return
            
            channel = self.bot.get_channel(killfeed_channel_id)
            if not channel:
                return
            
            embed = discord.Embed(
                title="🤖 Auto-Bounty Generated",
                description=f"The system has placed an automatic bounty on **{target_name}**!",
                color=0xFF8C00,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="💰 Reward",
                value=f"**${amount:,}**",
                inline=True
            )
            
            embed.add_field(
                name="🔥 Trigger",
                value=f"{kill_count} kills in 1 hour",
                inline=True
            )
            
            embed.add_field(
                name="⏰ Expires",
                value="<t:" + str(int((datetime.now(timezone.utc) + timedelta(hours=2)).timestamp())) + ":R>",
                inline=True
            )
            
            embed.add_field(
                name="🎯 Target",
                value=target_name,
                inline=False
            )
            
            embed.set_thumbnail(url="attachment://Bounty.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers • Auto-generated bounty")
            
            await channel.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to send auto-bounty embed: {e}")

def setup(bot):
    bot.add_cog(Bounties(bot))