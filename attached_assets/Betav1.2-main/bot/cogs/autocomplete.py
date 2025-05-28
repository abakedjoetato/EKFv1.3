import discord
from discord.ext import commands
from typing import List, Optional

class ServerAutocomplete:
    """
    Utility class to handle server name autocompletion for Discord slash commands.
    This replaces direct server_id inputs with user-friendly server names.
    """
    
    @staticmethod
    async def get_servers_for_guild(guild_id: int, database):
        """
        Fetch servers associated with a specific guild from the database.
        
        Args:
            guild_id: The Discord guild ID
            database: MongoDB database instance
            
        Returns:
            List of server documents containing name and ID
        """
        # Get guild configuration which contains the servers array
        guild_doc = await database.guilds.find_one({"guild_id": guild_id})
        
        if guild_doc and "servers" in guild_doc:
            return guild_doc["servers"]
        
        return []
    
    @staticmethod
    async def autocomplete_server_name(ctx: discord.AutocompleteContext):
        """
        Autocomplete callback for server names based on the guild context.
        
        Args:
            ctx: The Discord autocomplete context
            
        Returns:
            List of server names for the autocomplete dropdown
        """
        try:
            # Get bot instance from context
            bot = ctx.bot
            guild_id = ctx.interaction.guild_id
            
            # Use the database manager to ensure consistent access
            if hasattr(bot, 'db_manager'):
                # Get guild configuration directly from db_manager
                guild_config = await bot.db_manager.get_guild(guild_id)
                
                if guild_config and "servers" in guild_config:
                    servers = guild_config["servers"]
                    
                    # Return properly formatted server names for the autocomplete with multiple field fallbacks
                    return [
                        discord.OptionChoice(
                            name=server.get("name", 
                                 server.get("server_name", 
                                 f"Server {server.get('server_id', server.get('_id', 'unknown'))}")), 
                            value=str(server.get("server_id", server.get("_id", "unknown")))
                        )
                        for server in servers
                    ]
            else:
                # Compatibility layer for different database attribute names
                database = None
                if hasattr(bot, 'database'):
                    database = bot.database
                elif hasattr(bot, 'db_client'):
                    database = bot.db_client
                elif hasattr(bot, 'mongo_client') and hasattr(bot.mongo_client, 'emerald_killfeed'):
                    database = bot.mongo_client.emerald_killfeed
                else:
                    # Fallback to a collection directly
                    return [discord.OptionChoice(name="No servers found", value="none")]
                
                # Get servers for this guild from the database
                servers = await ServerAutocomplete.get_servers_for_guild(guild_id, database)
                
                # Return server names for the autocomplete with multiple field fallbacks
                return [
                    discord.OptionChoice(
                        name=server.get("name", 
                             server.get("server_name", 
                             f"Server {server.get('server_id', server.get('_id', 'unknown'))}")), 
                        value=str(server.get("server_id", server.get("_id", "unknown")))
                    )
                    for server in servers
                ]
                
        except Exception as e:
            # Log the error and return a safe fallback
            print(f"Autocomplete error: {e}")
            return [discord.OptionChoice(name="Error loading servers", value="none")]
            
        # Default fallback
        return [discord.OptionChoice(name="No servers found", value="none")]
    
    @staticmethod
    def get_server_id_from_name(server_name: str, servers: List[dict]) -> Optional[str]:
        """
        Convert a server name to its corresponding server_id.
        
        Args:
            server_name: The name of the server
            servers: List of server documents
            
        Returns:
            The server ID if found, None otherwise
        """
        for server in servers:
            # Check against name and server_name fields
            if server.get("name") == server_name or server.get("server_name") == server_name:
                # Return _id with backward compatibility fallback
                return str(server.get("_id", server.get("server_id", "unknown")))
        return None


class AutocompleteCog(commands.Cog):
    """
    Cog providing server name autocomplete functionality for slash commands.
    """
    
    def __init__(self, bot):
        self.bot = bot
        
    # This is an example of how to use the autocomplete in a slash command
    @discord.slash_command(name="example", description="Example command using server autocomplete")
    @discord.option(
        name="server",
        description="Select a server",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def example_command(self, ctx, server: str):
        """Example command showing how to use server autocomplete"""
        # Here server is the server_id value from the autocomplete
        await ctx.respond(f"Selected server ID: {server}")
        
    # Add additional commands that need autocomplete here
        
def setup(bot):
    bot.add_cog(AutocompleteCog(bot))