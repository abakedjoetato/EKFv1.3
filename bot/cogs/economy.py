"""
Emerald's Killfeed - Economy System (PHASE 3)
Currency stored per Discord user, scoped to guild
Earned via /work, PvP, bounties, online time
Admin control: /eco give, /eco take, /eco reset
"""

import asyncio
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any

import discord
from discord.ext import commands
from bot.cogs.autocomplete import ServerAutocomplete

logger = logging.getLogger(__name__)

class Economy(commands.Cog):
    """
    ECONOMY (PREMIUM)
    - Currency stored per Discord user, scoped to guild
    - Earned via /work, PvP, bounties, online time
    - Admin control: /eco give, /eco take, /eco reset
    - Tracked in wallet_events
    """
    
    def __init__(self, bot):
        self.bot = bot
        self.work_cooldowns: Dict[str, datetime] = {}
        self.user_locks: Dict[str, asyncio.Lock] = {}  # Prevent concurrent transactions
        
    def get_user_lock(self, user_key: str) -> asyncio.Lock:
        """Get or create a lock for a user to prevent concurrent transactions"""
        if user_key not in self.user_locks:
            self.user_locks[user_key] = asyncio.Lock()
        return self.user_locks[user_key]
    
    async def check_premium_server(self, guild_id: int, server_id: str = "default") -> bool:
        """Check if guild has premium access for economy features"""
        # Economy is premium-only, check any premium server in guild
        guild_doc = await self.bot.db_manager.get_guild(guild_id)
        if not guild_doc:
            return False
        
        servers = guild_doc.get('servers', [])
        for server_config in servers:
            server_id = server_config.get('server_id', server_config.get('_id', 'default'))
            if await self.bot.db_manager.is_premium_server(guild_id, server_id):
                return True
        
        return False
    
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
    
    @discord.slash_command(name="balance", description="Check your wallet balance")
    async def balance(self, ctx: discord.ApplicationContext):
        """Check user's wallet balance"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Economy features require premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Get wallet data
            wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
            
            embed = discord.Embed(
                title="💰 Wallet Balance",
                description=f"<@{discord_id}>'s financial status",
                color=0x00FF7F,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="💵 Current Balance",
                value=f"**${wallet['balance']:,}**",
                inline=True
            )
            
            embed.add_field(
                name="📈 Total Earned",
                value=f"${wallet['total_earned']:,}",
                inline=True
            )
            
            embed.add_field(
                name="📉 Total Spent",
                value=f"${wallet['total_spent']:,}",
                inline=True
            )
            
            # Add thumbnail
            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to show balance: {e}")
            await ctx.respond("❌ Failed to retrieve balance. Please try again.", ephemeral=True)
    
    @discord.slash_command(name="work", description="Work to earn money")
    async def work(self, ctx: discord.ApplicationContext):
        """Work command to earn money"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            user_key = f"{guild_id}_{discord_id}"
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Economy features require premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Check cooldown (1 hour)
            now = datetime.now(timezone.utc)
            if user_key in self.work_cooldowns:
                time_remaining = self.work_cooldowns[user_key] - now
                if time_remaining.total_seconds() > 0:
                    minutes_left = int(time_remaining.total_seconds() / 60)
                    embed = discord.Embed(
                        title="⏱️ Work Cooldown",
                        description=f"You must wait **{minutes_left}** minutes before working again!",
                        color=0xFFD700
                    )
                    await ctx.respond(embed=embed, ephemeral=True)
                    return
            
            # Use lock to prevent concurrent work commands
            async with self.get_user_lock(user_key):
                # Respond immediately to prevent timeout
                await ctx.defer()
                
                # Random work scenarios and earnings
                work_scenarios = [
                    ("🔧 Fixed a generator", random.randint(50, 150)),
                    ("🎯 Completed a bounty mission", random.randint(100, 300)),
                    ("🛡️ Defended a safe zone", random.randint(75, 200)),
                    ("📦 Delivered supplies", random.randint(60, 180)),
                    ("🔍 Scouted enemy territory", random.randint(80, 250)),
                    ("🏗️ Repaired base defenses", random.randint(90, 220)),
                    ("⚡ Restored power grid", random.randint(120, 280)),
                    ("🚑 Rescued survivors", random.randint(150, 350))
                ]
                
                scenario, earnings = random.choice(work_scenarios)
                
                # Add random bonus chance (10% chance for 2x earnings)
                if random.random() < 0.1:
                    earnings *= 2
                    scenario += " **[CRITICAL SUCCESS!]**"
                
                # Update wallet
                success = await self.bot.db_manager.update_wallet(
                    guild_id, discord_id, earnings, "work"
                )
                
                if success:
                    # Set cooldown (1 hour)
                    self.work_cooldowns[user_key] = now + timedelta(hours=1)
                    
                    # Add wallet event
                    await self.add_wallet_event(
                        guild_id, discord_id, earnings, "work", scenario
                    )
                    
                    # Create success embed
                    embed = discord.Embed(
                        title="💼 Work Completed",
                        description=scenario,
                        color=0x00FF00,
                        timestamp=now
                    )
                    
                    embed.add_field(
                        name="💰 Earnings",
                        value=f"**+${earnings:,}**",
                        inline=True
                    )
                    
                    embed.add_field(
                        name="⏰ Next Work",
                        value="Available in 1 hour",
                        inline=True
                    )
                    
                    embed.set_thumbnail(url="attachment://main.png")
                    embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                    
                    await ctx.followup.send(embed=embed)
                else:
                    await ctx.followup.send("❌ Failed to process work payment. Please try again.")
            
        except Exception as e:
            logger.error(f"Failed to process work command: {e}")
            await ctx.respond("❌ Work failed. Please try again.", ephemeral=True)
    
    # Admin economy commands
    eco = discord.SlashCommandGroup("eco", "Economy administration commands")
    
    @eco.command(name="give", description="Give money to a user")
    @discord.default_permissions(administrator=True)
    async def eco_give(self, ctx: discord.ApplicationContext, 
                       user: discord.Member, amount: int):
        """Give money to a user (admin only)"""
        try:
            guild_id = ctx.guild.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Economy features require premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            if amount <= 0:
                await ctx.respond("❌ Amount must be positive!", ephemeral=True)
                return
            
            # Update wallet
            success = await self.bot.db_manager.update_wallet(
                guild_id, user.id, amount, "admin_give"
            )
            
            if success:
                await self.add_wallet_event(
                    guild_id, user.id, amount, "admin_give", 
                    f"Given by {ctx.user.mention}"
                )
                
                embed = discord.Embed(
                    title="💰 Money Given",
                    description=f"Successfully gave **${amount:,}** to {user.mention}",
                    color=0x00FF00,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to give money. Please try again.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to give money: {e}")
            await ctx.respond("❌ Failed to give money.", ephemeral=True)
    
    @eco.command(name="take", description="Take money from a user")
    @discord.default_permissions(administrator=True)
    async def eco_take(self, ctx: discord.ApplicationContext, 
                       user: discord.Member, amount: int):
        """Take money from a user (admin only)"""
        try:
            guild_id = ctx.guild.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Economy features require premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            if amount <= 0:
                await ctx.respond("❌ Amount must be positive!", ephemeral=True)
                return
            
            # Check if user has enough money
            wallet = await self.bot.db_manager.get_wallet(guild_id, user.id)
            if wallet['balance'] < amount:
                await ctx.respond(
                    f"❌ {user.mention} only has **${wallet['balance']:,}** in their wallet!",
                    ephemeral=True
                )
                return
            
            # Update wallet (negative amount)
            success = await self.bot.db_manager.update_wallet(
                guild_id, user.id, -amount, "admin_take"
            )
            
            if success:
                await self.add_wallet_event(
                    guild_id, user.id, -amount, "admin_take", 
                    f"Taken by {ctx.user.mention}"
                )
                
                embed = discord.Embed(
                    title="💸 Money Taken",
                    description=f"Successfully took **${amount:,}** from {user.mention}",
                    color=0xFF4500,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to take money. Please try again.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to take money: {e}")
            await ctx.respond("❌ Failed to take money.", ephemeral=True)
    
    @eco.command(name="reset", description="Reset a user's wallet")
    @discord.default_permissions(administrator=True)
    async def eco_reset(self, ctx: discord.ApplicationContext, user: discord.Member):
        """Reset a user's wallet (admin only)"""
        try:
            guild_id = ctx.guild.id
            
            # Check premium access
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🔒 Premium Feature",
                    description="Economy features require premium subscription!",
                    color=0xFF6B6B
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return
            
            # Get current balance
            wallet = await self.bot.db_manager.get_wallet(guild_id, user.id)
            current_balance = wallet['balance']
            
            if current_balance == 0:
                await ctx.respond(f"❌ {user.mention}'s wallet is already empty!", ephemeral=True)
                return
            
            # Reset wallet
            await self.bot.db_manager.economy.update_one(
                {"guild_id": guild_id, "discord_id": user.id},
                {"$set": {"balance": 0, "total_earned": 0, "total_spent": 0}}
            )
            
            await self.add_wallet_event(
                guild_id, user.id, -current_balance, "admin_reset", 
                f"Wallet reset by {ctx.user.mention}"
            )
            
            embed = discord.Embed(
                title="🔄 Wallet Reset",
                description=f"Successfully reset {user.mention}'s wallet\nPrevious balance: **${current_balance:,}**",
                color=0xFFA500,
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to reset wallet: {e}")
            await ctx.respond("❌ Failed to reset wallet.", ephemeral=True)

def setup(bot):
    bot.add_cog(Economy(bot))