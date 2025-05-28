"""
Emerald's Killfeed - Premium Management System (PHASE 9)
/sethome by BOT_OWNER_ID
/premium assign, /premium revoke, /premium status
Premium is assigned per server, not user
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

import discord
from discord.ext import commands
from bot.cogs.autocomplete import ServerAutocomplete

logger = logging.getLogger(__name__)

class Premium(commands.Cog):
    """
    PREMIUM MGMT (PHASE 9)
    - /sethome by BOT_OWNER_ID
    - /premium assign, /premium revoke, /premium status
    - Premium is assigned per server, not user
    """

    def __init__(self, bot):
        self.bot = bot
        self.bot_owner_id = int(os.getenv('BOT_OWNER_ID', 0))

    def is_bot_owner(self, user_id: int) -> bool:
        """Check if user is the bot owner"""
        import os
        bot_owner_id = int(os.getenv('BOT_OWNER_ID', 0))
        return user_id == bot_owner_id

    @discord.slash_command(name="sethome", description="Set this server as the bot's home server")
    async def sethome(self, ctx: discord.ApplicationContext):
        """Set this server as the bot's home server (BOT_OWNER_ID only)"""
        try:
            # Check if user is bot owner
            if not self.is_bot_owner(ctx.user.id):
                await ctx.respond("❌ Only the bot owner can use this command!", ephemeral=True)
                return

            guild_id = ctx.guild.id

            # Update or create guild as home server
            await self.bot.database.guilds.update_one(
                {"guild_id": guild_id},
                {
                    "$set": {
                        "guild_name": ctx.guild.name,
                        "is_home_server": True,
                        "updated_at": datetime.now(timezone.utc)
                    },
                    "$setOnInsert": {
                        "created_at": datetime.now(timezone.utc),
                        "servers": [],
                        "channels": {}
                    }
                },
                upsert=True
            )

            # Remove home server status from other guilds
            await self.bot.database.guilds.update_many(
                {"guild_id": {"$ne": guild_id}},
                {"$unset": {"is_home_server": ""}}
            )

            embed = discord.Embed(
                title="🏠 Home Server Set",
                description=f"**{ctx.guild.name}** has been set as the bot's home server!",
                color=0x00FF00,
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="🎯 Benefits",
                value="• Full access to all premium features\n• Administrative controls\n• Premium management commands",
                inline=False
            )

            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to set home server: {e}")
            await ctx.respond("❌ Failed to set home server.", ephemeral=True)

    premium = discord.SlashCommandGroup("premium", "Premium management commands")

    @premium.command(name="assign", description="Assign premium to a server")
    @discord.default_permissions(administrator=True)
    @discord.option(
        name="server",
        description="Select a server",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def premium_assign(self, ctx: discord.ApplicationContext, server: str, duration_days: int = 30):
        """Assign premium status to a server"""
        try:
            guild_id = ctx.guild.id
            server_id = server  # Use the server parameter which contains the server_id

            # Check if user is bot owner or in home server
            is_owner = self.is_bot_owner(ctx.user.id)

            # Check if current guild is home server
            home_guild = await self.bot.db_manager.guilds.find_one({
                "guild_id": guild_id,
                "is_home_server": True
            })

            if not is_owner and not home_guild:
                await ctx.respond("❌ Premium management is only available to bot owners or in the home server!", ephemeral=True)
                return

            # Validate duration
            if duration_days <= 0 or duration_days > 365:
                await ctx.respond("❌ Duration must be between 1 and 365 days!", ephemeral=True)
                return

            # Calculate expiration date
            expires_at = datetime.now(timezone.utc) + timedelta(days=duration_days)

            # Set premium status
            success = await self.bot.db_manager.set_premium_status(guild_id, server_id, expires_at)

            if success:
                embed = discord.Embed(
                    title="⭐ Premium Assigned",
                    description=f"Premium status assigned to server **{server_id}**!",
                    color=0xFFD700,
                    timestamp=datetime.now(timezone.utc)
                )

                embed.add_field(
                    name="⏰ Duration",
                    value=f"{duration_days} days",
                    inline=True
                )

                embed.add_field(
                    name="📅 Expires",
                    value=f"<t:{int(expires_at.timestamp())}:F>",
                    inline=True
                )

                embed.add_field(
                    name="🎯 Features Unlocked",
                    value="• Economy System\n• Gambling Games\n• Bounty System\n• Faction System\n• Log Parser\n• Leaderboards",
                    inline=False
                )

                embed.set_thumbnail(url="attachment://main.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to assign premium status.", ephemeral=True)

        except Exception as e:
            logger.error(f"Failed to assign premium: {e}")
            await ctx.respond("❌ Failed to assign premium.", ephemeral=True)

    @premium.command(name="revoke", description="Revoke premium from a server")
    @discord.default_permissions(administrator=True)
    @discord.option(
        name="server",
        description="Select a server",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def premium_revoke(self, ctx: discord.ApplicationContext, server: str):
        """Revoke premium status from a server"""
        try:
            guild_id = ctx.guild.id
            server_id = server  # Use the server parameter which contains the server_id

            # Check if user is bot owner or in home server
            is_owner = self.is_bot_owner(ctx.user.id)

            # Check if current guild is home server
            home_guild = await self.bot.db_manager.guilds.find_one({
                "guild_id": guild_id,
                "is_home_server": True
            })

            if not is_owner and not home_guild:
                await ctx.respond("❌ Premium management is only available to bot owners or in the home server!", ephemeral=True)
                return

            # Check if server has premium
            is_premium = await self.bot.db_manager.is_premium_server(guild_id, server_id)

            if not is_premium:
                await ctx.respond(f"❌ Server **{server_id}** does not have premium status!", ephemeral=True)
                return

            # Revoke premium
            success = await self.bot.db_manager.set_premium_status(guild_id, server_id, None)

            if success:
                embed = discord.Embed(
                    title="❌ Premium Revoked",
                    description=f"Premium status revoked from server **{server_id}**.",
                    color=0xFF6B6B,
                    timestamp=datetime.now(timezone.utc)
                )

                embed.add_field(
                    name="⚠️ Note",
                    value="Premium features are now disabled for this server.",
                    inline=False
                )

                embed.set_thumbnail(url="attachment://main.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to revoke premium status.", ephemeral=True)

        except Exception as e:
            logger.error(f"Failed to revoke premium: {e}")
            await ctx.respond("❌ Failed to revoke premium.", ephemeral=True)

    @premium.command(name="status", description="Check premium status for servers")
    async def premium_status(self, ctx: discord.ApplicationContext):
        """Check premium status for all servers in the guild"""
        try:
            guild_id = ctx.guild.id

            # Get guild configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)

            if not guild_config:
                await ctx.respond("❌ This guild is not configured!", ephemeral=True)
                return

            servers = guild_config.get('servers', [])

            if not servers:
                embed = discord.Embed(
                    title="⭐ Premium Status",
                    description="No game servers configured for this guild.",
                    color=0x808080,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(
                    name="🎯 Next Steps",
                    value="Use `/server add` to add game servers first.",
                    inline=False
                )
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                await ctx.respond(embed=embed)
                return

            # Check premium status for each server
            premium_servers = []
            free_servers = []

            for server_config in servers:
                server_id = str(server_config.get('_id', 'unknown'))
                server_name = server_config.get('name', f'Server {server_id}')
                is_premium = await self.bot.db_manager.is_premium_server(guild_id, server_id)

                if is_premium:
                    # Get expiration info
                    premium_doc = await self.bot.db_manager.premium.find_one({
                        "guild_id": guild_id,
                        "server_id": server_id
                    })

                    if premium_doc and premium_doc.get('expires_at'):
                        expires_text = f"<t:{int(premium_doc['expires_at'].timestamp())}:R>"
                    else:
                        expires_text = "Never"

                    premium_servers.append(f"**{server_name}** - Expires {expires_text}")
                else:
                    free_servers.append(f"**{server_name}** - Free tier")

            # Create status embed
            embed = discord.Embed(
                title="⭐ Premium Status",
                description=f"Premium status for **{ctx.guild.name}**",
                color=0xFFD700 if premium_servers else 0x808080,
                timestamp=datetime.now(timezone.utc)
            )

            if premium_servers:
                embed.add_field(
                    name="✅ Premium Servers",
                    value="\n".join(premium_servers),
                    inline=False
                )

            if free_servers:
                embed.add_field(
                    name="🆓 Free Servers",
                    value="\n".join(free_servers),
                    inline=False
                )

            # Check if user can manage premium
            is_owner = self.is_bot_owner(ctx.user.id)
            home_guild = await self.bot.db_manager.guilds.find_one({
                "guild_id": guild_id,
                "is_home_server": True
            })

            if is_owner or home_guild:
                embed.add_field(
                    name="🛠️ Management",
                    value="Use `/premium assign` and `/premium revoke` to manage premium status.",
                    inline=False
                )

            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to check premium status: {e}")
            await ctx.respond("❌ Failed to check premium status.", ephemeral=True)

    server = discord.SlashCommandGroup("server", "Game server management commands")

    @server.command(name="add", description="Add a game server with SFTP credentials to this guild")
    @discord.default_permissions(administrator=True)
    async def server_add(self, ctx: discord.ApplicationContext, 
                        name: str, host: str, port: int, username: str, password: str, serverid: str):
        """Add a game server with full SFTP credentials to the guild"""
        try:
            guild_id = ctx.guild.id

            # Validate inputs
            serverid = serverid.strip()
            name = name.strip()
            host = host.strip()
            username = username.strip()
            password = password.strip()

            if not all([serverid, name, host, username, password]):
                await ctx.respond("❌ All fields are required: name, host, port, username, password, serverid", ephemeral=True)
                return

            if not (1 <= port <= 65535):
                await ctx.respond("❌ Port must be between 1 and 65535", ephemeral=True)
                return

            # Get or create guild
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                guild_config = await self.bot.db_manager.create_guild(guild_id, ctx.guild.name)

            # Check if server already exists
            existing_servers = guild_config.get('servers', [])
            for server in existing_servers:
                if server.get('_id') == serverid:
                    await ctx.respond(f"❌ Server **{serverid}** is already added!", ephemeral=True)
                    return

            # Create server config with full SFTP credentials
            server_config = {
                '_id': serverid,
                'server_id': serverid,  # Add explicit server_id field for better compatibility
                'name': name,
                'server_name': name,    # Add explicit server_name field for better compatibility
                'host': host,
                'hostname': host,       # Add hostname alias for better compatibility
                'port': port,
                'username': username,
                'password': password,
                'added_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }

            # Add server to guild config
            await self.bot.db_manager.add_server_to_guild(guild_id, server_config)

            # Respond with success
            embed = discord.Embed(
                title="✅ Server Added",
                description=f"Server **{name}** has been added to this guild!",
                color=0x00FF00,
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="🔄 Next Steps",
                value="The system will automatically attempt to connect to this server and verify the credentials.",
                inline=False
            )

            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

            # Schedule automatic refresh of server data
            try:
                if hasattr(self.bot, 'historical_parser'):
                    await self.bot.historical_parser.auto_refresh_after_server_add(guild_id, server_config)
            except Exception as e:
                logger.error(f"Failed to schedule automatic refresh: {e}")
                
        except Exception as e:
            logger.error(f"Failed to add server: {e}")
            await ctx.respond("❌ Failed to add server. Please try again.", ephemeral=True)
            
    @server.command(name="list", description="List all configured servers in this guild")
    async def server_list(self, ctx: discord.ApplicationContext):
        """List all servers configured in this guild"""
        try:
            guild_id = ctx.guild.id
            
            # Get guild configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            
            if not guild_config:
                await ctx.respond("❌ This guild is not configured!", ephemeral=True)
                return
                
            servers = guild_config.get('servers', [])
            
            if not servers:
                embed = discord.Embed(
                    title="📋 Server List",
                    description="No servers configured for this guild.",
                    color=0x808080,
                    timestamp=datetime.now(timezone.utc)
                )
                embed.add_field(
                    name="🎯 Next Steps",
                    value="Use `/server add` to add a game server.",
                    inline=False
                )
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                await ctx.respond(embed=embed)
                return
                
            # Create server list embed
            embed = discord.Embed(
                title="📋 Server List",
                description=f"Configured servers for **{ctx.guild.name}**",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc)
            )
            
            # Add server details
            for server in servers:
                # The server ID might be in different fields depending on how it was added
                # Try multiple fields in order of likelihood
                server_id = str(server.get('server_id', 
                              server.get('_id', 
                              server.get('id', 'unknown'))))
                
                # Get server metadata with better fallbacks
                server_name = server.get('name', server.get('server_name', f'Server {server_id}'))
                sftp_host = server.get('host', server.get('hostname', 'Not configured'))
                sftp_port = server.get('port', 22)
                
                # Log server details for debugging
                logger.info(f"Server details - ID: {server_id}, Name: {server_name}, Host: {sftp_host}")
                
                # Check premium status
                is_premium = await self.bot.db_manager.is_premium_server(guild_id, server_id)
                premium_status = "⭐ Premium" if is_premium else "🆓 Free tier"
                
                # Format server details
                server_details = f"**Host:** {sftp_host}:{sftp_port}\n**Status:** {premium_status}"
                
                embed.add_field(
                    name=f"{server_name} (ID: {server_id})",
                    value=server_details,
                    inline=False
                )
                
            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to list servers: {e}")
            await ctx.respond("❌ Failed to list servers. Please try again.", ephemeral=True)
            
    @server.command(name="remove", description="Remove a server from this guild")
    @discord.default_permissions(administrator=True)
    @discord.option(
        name="server",
        description="Select a server to remove",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def server_remove(self, ctx: discord.ApplicationContext, server: str):
        """Remove a server from the guild"""
        try:
            guild_id = ctx.guild.id
            server_id = server  # Server ID from autocomplete
            
            # Get guild configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            
            if not guild_config:
                await ctx.respond("❌ This guild is not configured!", ephemeral=True)
                return
                
            # Find server in the guild config - handle both old and new formats
            servers = guild_config.get('servers', [])
            server_found = False
            server_name = "Unknown Server"
            
            for srv in servers:
                # Check both _id (new format) and server_id (old format)
                srv_id = str(srv.get('_id', srv.get('server_id', 'unknown')))
                if srv_id == server_id:
                    server_found = True
                    server_name = srv.get('name', srv.get('server_name', f'Server {server_id}'))
                    break
                    
            if not server_found:
                await ctx.respond(f"❌ Server **{server_id}** not found in this guild!", ephemeral=True)
                return
                
            # Confirm removal
            confirm_embed = discord.Embed(
                title="⚠️ Confirm Server Removal",
                description=f"Are you sure you want to remove the server **{server_name}**?",
                color=0xFF6B6B,
                timestamp=datetime.now(timezone.utc)
            )
            
            confirm_embed.add_field(
                name="⚠️ Warning",
                value="This will permanently remove all server configuration and data. This action cannot be undone.",
                inline=False
            )
            
            confirm_embed.set_thumbnail(url="attachment://main.png")
            confirm_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            # Create confirmation buttons
            class ConfirmView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=60)
                    self.value = None
                    
                @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
                async def cancel_button(self, button, interaction):
                    self.value = False
                    self.stop()
                    await interaction.response.edit_message(content="🛑 Server removal cancelled.", embed=None, view=None)
                    
                @discord.ui.button(label="Remove Server", style=discord.ButtonStyle.danger, emoji="⚠️")
                async def confirm_button(self, button, interaction):
                    self.value = True
                    self.stop()
                    await interaction.response.edit_message(content="⏳ Removing server...", embed=None, view=None)
            
            # Send confirmation message
            view = ConfirmView()
            await ctx.respond(embed=confirm_embed, view=view)
            
            # Wait for confirmation
            await view.wait()
            
            if view.value:
                # Remove server from guild config
                result = await self.bot.db_manager.remove_server_from_guild(guild_id, server_id)
                
                if result:
                    success_embed = discord.Embed(
                        title="✅ Server Removed",
                        description=f"The server **{server_name}** has been removed from this guild.",
                        color=0x00FF00,
                        timestamp=datetime.now(timezone.utc)
                    )
                    
                    success_embed.set_thumbnail(url="attachment://main.png")
                    success_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                    
                    await ctx.followup.send(embed=success_embed)
                else:
                    await ctx.followup.send("❌ Failed to remove server. Please try again.")
            
        except Exception as e:
            logger.error(f"Failed to remove server: {e}")
            await ctx.respond("❌ Failed to remove server. Please try again.", ephemeral=True)
            
    @server.command(name="refresh", description="Refresh data for a server")
    @discord.default_permissions(administrator=True)
    @discord.option(
        name="server",
        description="Select a server to refresh",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def server_refresh(self, ctx: discord.ApplicationContext, server: str):
        """Refresh data for a server"""
        try:
            guild_id = ctx.guild.id
            server_id = server  # Server ID from autocomplete
            
            # Get guild configuration
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            
            if not guild_config:
                await ctx.respond("❌ This guild is not configured!", ephemeral=True)
                return
                
            # Find server in the guild config
            servers = guild_config.get('servers', [])
            server_found = False
            server_config = None
            server_name = "Unknown Server"
            
            for srv in servers:
                if str(srv.get('_id')) == server_id:
                    server_found = True
                    server_config = srv
                    server_name = srv.get('name', f'Server {server_id}')
                    break
                    
            if not server_found or not server_config:
                await ctx.respond(f"❌ Server **{server_id}** not found in this guild!", ephemeral=True)
                return
                
            # Respond with initial message
            await ctx.respond(f"⏳ Starting data refresh for server **{server_name}**...")
            
            # Verify we have the historical parser
            if not hasattr(self.bot, 'historical_parser'):
                await ctx.followup.send("❌ Historical parser is not available!")
                return
                
            # Run historical data refresh
            try:
                await self.bot.historical_parser.refresh_server_data(guild_id, server_config, channel=ctx.channel)
            except Exception as e:
                logger.error(f"Failed to refresh data: {e}")
                await ctx.followup.send("❌ Failed to start data refresh. Please try again later.")
            
        except Exception as e:
            logger.error(f"Failed to refresh server data: {e}")
            await ctx.respond("❌ Failed to initiate data refresh. Please try again.", ephemeral=True)

def setup(bot):
    bot.add_cog(Premium(bot))