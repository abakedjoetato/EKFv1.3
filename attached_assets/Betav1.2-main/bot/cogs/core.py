"""
Emerald's Killfeed - Core System Commands
Basic bot management, info, and utility commands
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands

logger = logging.getLogger(__name__)

class Core(commands.Cog):
    """
    CORE SYSTEM
    - Basic bot information and utility commands
    - Server management and configuration
    - General purpose commands
    """

    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="info", description="Show bot information")
    async def info(self, ctx: discord.ApplicationContext):
        """Display bot information and statistics"""
        try:
            # Create bot info embed manually for reliability
            embed = discord.Embed(
                title="🤖 Emerald's Killfeed Bot",
                description="Advanced Discord bot for Deadside server monitoring",
                color=0x00d38a,
                timestamp=datetime.now(timezone.utc)
            )

            # Add bot information fields
            embed.add_field(
                name="📊 Server Statistics",
                value=f"**Guilds:** {len(self.bot.guilds)}\n**Users:** {sum(guild.member_count for guild in self.bot.guilds if guild.member_count):,}",
                inline=True
            )

            embed.add_field(
                name="⚙️ System Status", 
                value="**Status:** Online\n**Version:** v2.0",
                inline=True
            )

            embed.add_field(
                name="🔗 Links",
                value="[Discord Server](https://discord.gg/EmeraldServers)\n[Support](https://discord.gg/EmeraldServers)",
                inline=False
            )

            embed.set_thumbnail(url=self.bot.user.avatar.url if self.bot.user and self.bot.user.avatar else None)
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)
        except Exception as e:
            logger.error(f"Failed to show bot info: {e}")
            await ctx.respond("❌ Failed to retrieve bot information.", ephemeral=True)

    @discord.slash_command(name="ping", description="Check bot latency")
    async def ping(self, ctx):
        """Check bot response time and latency"""
        try:
            latency = round(self.bot.latency * 1000)

            embed = discord.Embed(
                title="🏓 Pong!",
                description=f"Bot latency: **{latency}ms**",
                color=0x00FF00 if latency < 100 else 0xFFD700 if latency < 300 else 0xFF6B6B,
                timestamp=datetime.now(timezone.utc)
            )

            # Status indicator
            if latency < 100:
                status = "🟢 Excellent"
            elif latency < 300:
                status = "🟡 Good"
            else:
                status = "🔴 Poor"

            embed.add_field(
                name="📡 Connection Status",
                value=status,
                inline=True
            )

            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to ping: {e}")
            await ctx.respond("❌ Failed to check latency.", ephemeral=True)

    @discord.slash_command(name="help", description="Show help information")
    async def help(self, ctx):
        """Display help information and command categories"""
        try:
            embed = discord.Embed(
                title="❓ Help & Commands",
                description="Complete guide to Emerald's Killfeed Bot",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc)
            )

            # Free commands
            embed.add_field(
                name="🆓 Free Commands",
                value="• `/info` - Bot information\n• `/ping` - Check latency\n• `/link` - Link characters\n• `/linked` - View linked characters\n• `/stats` - Player statistics",
                inline=False
            )

            # Premium commands
            embed.add_field(
                name="⭐ Premium Commands",
                value="• `/balance` - Check wallet\n• `/work` - Earn money\n• `/bounty` - Bounty system\n• `/faction` - Faction management\n• `/gambling` - Casino games",
                inline=False
            )

            # Admin commands
            embed.add_field(
                name="🛠️ Admin Commands",
                value="• `/server` - Server management\n• `/premium` - Premium management\n• `/eco` - Economy administration",
                inline=False
            )

            embed.add_field(
                name="🎯 Getting Started",
                value="1. Link your character with `/link <name>`\n2. Check stats with `/stats`\n3. Upgrade to premium for full features!",
                inline=False
            )

            embed.set_thumbnail(url=self.bot.user.avatar.url if self.bot.user and self.bot.user.avatar else None)
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to show help: {e}")
            await ctx.respond("❌ Failed to show help information.", ephemeral=True)

    @discord.slash_command(name="status", description="Check bot and system status")
    async def status(self, ctx: discord.ApplicationContext):
        """Display comprehensive bot status information"""
        try:
            # Check database connection
            db_status = "🟢 Connected"
            try:
                await self.bot.mongo_client.admin.command('ping')
            except:
                db_status = "🔴 Disconnected"

            # Check scheduler status
            scheduler_status = "🟢 Running" if self.bot.scheduler.running else "🔴 Stopped"

            embed = discord.Embed(
                title="📊 System Status",
                description="Current bot and system status",
                color=0x00FF7F,
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="🤖 Bot Status",
                value=f"• Status: **🟢 Online**\n• Uptime: **{self._format_uptime()}**\n• Latency: **{round(self.bot.latency * 1000)}ms**",
                inline=True
            )

            embed.add_field(
                name="🔗 Connections",
                value=f"• Database: **{db_status}**\n• Scheduler: **{scheduler_status}**\n• Discord: **🟢 Connected**",
                inline=True
            )

            embed.add_field(
                name="📈 Statistics",
                value=f"• Guilds: **{len(self.bot.guilds)}**\n• Users: **{len(self.bot.users)}**\n• Commands: **{len(self.bot.pending_application_commands)}**",
                inline=True
            )

            embed.set_thumbnail(url=self.bot.user.avatar.url if self.bot.user and self.bot.user.avatar else None)
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to show status: {e}")
            await ctx.respond("❌ Failed to retrieve status information.", ephemeral=True)

    def _format_uptime(self) -> str:
        """Format bot uptime in human readable format"""
        try:
            import psutil
            import os

            # Get process uptime
            process = psutil.Process(os.getpid())
            uptime_seconds = process.create_time()
            current_time = datetime.now().timestamp()
            uptime = int(current_time - uptime_seconds)

            hours, remainder = divmod(uptime, 3600)
            minutes, seconds = divmod(remainder, 60)

            if hours > 0:
                return f"{hours}h {minutes}m {seconds}s"
            elif minutes > 0:
                return f"{minutes}m {seconds}s"
            else:
                return f"{seconds}s"

        except:
            return "Unknown"

def setup(bot):
    bot.add_cog(Core(bot))