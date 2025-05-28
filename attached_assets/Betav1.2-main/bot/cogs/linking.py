"""
Emerald's Killfeed - Player Linking System (PHASE 5)
/link <char>, /alt add/remove, /linked, /unlink
Stored per guild, used by economy, stats, bounties, factions
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import discord
from discord.ext import commands

logger = logging.getLogger(__name__)

class Linking(commands.Cog):
    """
    LINKING (FREE)
    - /link <char>, /alt add/remove, /linked, /unlink
    - Stored per guild
    - Used by economy, stats, bounties, factions
    """
    
    def __init__(self, bot):
        self.bot = bot
    
    @discord.slash_command(name="link", description="Link your Discord account to a character")
    async def link(self, ctx, character: str):
        """Link Discord account to a character name"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Validate character name
            character = character.strip()
            if not character:
                await ctx.respond("❌ Character name cannot be empty!", ephemeral=True)
                return
            
            if len(character) > 32:
                await ctx.respond("❌ Character name too long! Maximum 32 characters.", ephemeral=True)
                return
            
            # Validate that player exists in PvP data
            actual_player_name = await self.bot.db_manager.find_player_in_pvp_data(guild_id, character)
            if not actual_player_name:
                await ctx.respond(
                    f"❌ Player **{character}** not found in the database! Make sure you've played on the server and the name is spelled correctly.",
                    ephemeral=True
                )
                return
            
            # Use the actual player name from database (correct capitalization)
            character = actual_player_name
            
            # Check if character is already linked to another user
            existing_link = await self.bot.db_manager.players.find_one({
                "guild_id": guild_id,
                "linked_characters": character
            })
            
            if existing_link and existing_link['discord_id'] != discord_id:
                await ctx.respond(
                    f"❌ Character **{character}** is already linked to another Discord account!",
                    ephemeral=True
                )
                return
            
            # Link the character
            try:
                success = await self.bot.db_manager.link_player(guild_id, discord_id, character)
            except Exception as db_error:
                logger.error(f"Database error during character linking: {db_error}")
                await ctx.respond("❌ Database error occurred. Please try again later.", ephemeral=True)
                return
            
            if success:
                # Get updated player data
                try:
                    player_data = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
                    if not player_data:
                        await ctx.respond("❌ Failed to retrieve updated player data.", ephemeral=True)
                        return
                except Exception as db_error:
                    logger.error(f"Database error retrieving updated player data: {db_error}")
                    await ctx.respond("❌ Database error occurred. Please try again later.", ephemeral=True)
                    return
                
                # Set Discord nickname to primary character name
                try:
                    member = ctx.guild.get_member(discord_id)
                    if member and member != ctx.guild.owner:  # Can't change owner's nickname
                        primary_char = player_data['primary_character']
                        if member.nick != primary_char:  # Only change if different
                            await member.edit(nick=primary_char)
                            logger.info(f"Set nickname for {member.display_name} to {primary_char}")
                except discord.Forbidden:
                    logger.warning(f"No permission to change nickname for user {discord_id}")
                except discord.HTTPException as e:
                    logger.error(f"Failed to set nickname: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error setting nickname: {e}")
                
                embed = discord.Embed(
                    title="🔗 Character Linked",
                    description=f"Successfully linked **{character}** to your Discord account!",
                    color=0x00FF00,
                    timestamp=datetime.now(timezone.utc)
                )
                
                embed.add_field(
                    name="👤 Linked Characters",
                    value="\n".join([f"• {char}" for char in player_data['linked_characters']]),
                    inline=False
                )
                
                embed.add_field(
                    name="⭐ Primary Character",
                    value=player_data['primary_character'],
                    inline=True
                )
                
                embed.set_thumbnail(url="attachment://main.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to link character. Please try again.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to link character: {e}")
            await ctx.respond("❌ Failed to link character.", ephemeral=True)
    
    alt = discord.SlashCommandGroup("alt", "Manage alternate characters")
    
    @alt.command(name="add", description="Add an alternate character")
    async def alt_add(self, ctx: discord.ApplicationContext, character: str):
        """Add an alternate character to your account"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Check if user has any linked characters
            player_data = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
            if not player_data:
                await ctx.respond(
                    "❌ You must link your main character first using `/link <character>`!",
                    ephemeral=True
                )
                return
            
            # Validate character name
            character = character.strip()
            if not character:
                await ctx.respond("❌ Character name cannot be empty!", ephemeral=True)
                return
            
            if len(character) > 32:
                await ctx.respond("❌ Character name too long! Maximum 32 characters.", ephemeral=True)
                return
            
            # Validate that player exists in PvP data
            actual_player_name = await self.bot.db_manager.find_player_in_pvp_data(guild_id, character)
            if not actual_player_name:
                await ctx.respond(
                    f"❌ Player **{character}** not found in the database! Make sure you've played on the server and the name is spelled correctly.",
                    ephemeral=True
                )
                return
            
            # Use the actual player name from database (correct capitalization)
            character = actual_player_name
            
            # Check if character is already linked
            if character in player_data['linked_characters']:
                await ctx.respond(f"❌ **{character}** is already linked to your account!", ephemeral=True)
                return
            
            # Check if character is linked to another user
            existing_link = await self.bot.db_manager.players.find_one({
                "guild_id": guild_id,
                "linked_characters": character
            })
            
            if existing_link and existing_link['discord_id'] != discord_id:
                await ctx.respond(
                    f"❌ Character **{character}** is already linked to another Discord account!",
                    ephemeral=True
                )
                return
            
            # Add the alternate character
            result = await self.bot.db_manager.players.update_one(
                {"guild_id": guild_id, "discord_id": discord_id},
                {"$addToSet": {"linked_characters": character}}
            )
            
            if result.modified_count > 0:
                # Get updated data
                updated_player = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
                
                embed = discord.Embed(
                    title="➕ Alternate Character Added",
                    description=f"Successfully added **{character}** as an alternate character!",
                    color=0x00FF00,
                    timestamp=datetime.now(timezone.utc)
                )
                
                embed.add_field(
                    name="👤 All Linked Characters",
                    value="\n".join([f"• {char}" for char in updated_player['linked_characters']]),
                    inline=False
                )
                
                embed.set_thumbnail(url="attachment://main.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to add alternate character.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to add alt character: {e}")
            await ctx.respond("❌ Failed to add alternate character.", ephemeral=True)
    
    @alt.command(name="remove", description="Remove an alternate character")
    async def alt_remove(self, ctx: discord.ApplicationContext, character: str):
        """Remove an alternate character from your account"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Get player data
            player_data = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
            if not player_data:
                await ctx.respond("❌ You don't have any linked characters!", ephemeral=True)
                return
            
            # Validate character name
            character = character.strip()
            if character not in player_data['linked_characters']:
                await ctx.respond(f"❌ **{character}** is not linked to your account!", ephemeral=True)
                return
            
            # Prevent removing primary character if it's the only one
            if len(player_data['linked_characters']) == 1:
                await ctx.respond(
                    "❌ Cannot remove your only character! Use `/unlink` to remove all characters.",
                    ephemeral=True
                )
                return
            
            # Remove the character
            result = await self.bot.db_manager.players.update_one(
                {"guild_id": guild_id, "discord_id": discord_id},
                {"$pull": {"linked_characters": character}}
            )
            
            if result.modified_count > 0:
                # If removed character was primary, set new primary
                if player_data['primary_character'] == character:
                    remaining_chars = [c for c in player_data['linked_characters'] if c != character]
                    await self.bot.db_manager.players.update_one(
                        {"guild_id": guild_id, "discord_id": discord_id},
                        {"$set": {"primary_character": remaining_chars[0]}}
                    )
                
                # Get updated data
                updated_player = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
                
                embed = discord.Embed(
                    title="➖ Alternate Character Removed",
                    description=f"Successfully removed **{character}** from your linked characters!",
                    color=0xFFA500,
                    timestamp=datetime.now(timezone.utc)
                )
                
                if updated_player['linked_characters']:
                    embed.add_field(
                        name="👤 Remaining Characters",
                        value="\n".join([f"• {char}" for char in updated_player['linked_characters']]),
                        inline=False
                    )
                    
                    embed.add_field(
                        name="⭐ Primary Character",
                        value=updated_player['primary_character'],
                        inline=True
                    )
                
                embed.set_thumbnail(url="attachment://main.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                await ctx.respond(embed=embed)
            else:
                await ctx.respond("❌ Failed to remove alternate character.", ephemeral=True)
                
        except Exception as e:
            logger.error(f"Failed to remove alt character: {e}")
            await ctx.respond("❌ Failed to remove alternate character.", ephemeral=True)
    
    @discord.slash_command(name="linked", description="View your linked characters")
    async def linked(self, ctx: discord.ApplicationContext, user: discord.Member = None):
        """View linked characters for yourself or another user"""
        try:
            guild_id = ctx.guild.id
            target_user = user or ctx.user
            
            # Get player data
            player_data = await self.bot.db_manager.get_linked_player(guild_id, target_user.id)
            
            if not player_data:
                if target_user == ctx.user:
                    await ctx.respond(
                        "❌ You don't have any linked characters! Use `/link <character>` to get started.",
                        ephemeral=True
                    )
                else:
                    await ctx.respond(
                        f"❌ {target_user.mention} doesn't have any linked characters!",
                        ephemeral=True
                    )
                return
            
            embed = discord.Embed(
                title="🔗 Linked Characters",
                description=f"Character information for {target_user.mention}",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="👤 Linked Characters",
                value="\n".join([f"• {char}" for char in player_data['linked_characters']]),
                inline=False
            )
            
            embed.add_field(
                name="⭐ Primary Character",
                value=player_data['primary_character'],
                inline=True
            )
            
            embed.add_field(
                name="📅 Linked Since",
                value=f"<t:{int(player_data['linked_at'].timestamp())}:F>",
                inline=True
            )
            
            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
            
            await ctx.respond(embed=embed)
            
        except Exception as e:
            logger.error(f"Failed to show linked characters: {e}")
            await ctx.respond("❌ Failed to retrieve linked characters.", ephemeral=True)
    
    @discord.slash_command(name="unlink", description="Unlink all your characters")
    async def unlink(self, ctx: discord.ApplicationContext):
        """Unlink all characters from your Discord account"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            
            # Get player data
            player_data = await self.bot.db_manager.get_linked_player(guild_id, discord_id)
            
            if not player_data:
                await ctx.respond("❌ You don't have any linked characters!", ephemeral=True)
                return
            
            # Create confirmation embed
            characters_list = "\n".join([f"• {char}" for char in player_data['linked_characters']])
            
            embed = discord.Embed(
                title="⚠️ Confirm Unlinking",
                description="Are you sure you want to unlink ALL your characters?",
                color=0xFF6B6B,
                timestamp=datetime.now(timezone.utc)
            )
            
            embed.add_field(
                name="👤 Characters to Unlink",
                value=characters_list,
                inline=False
            )
            
            embed.add_field(
                name="⚠️ Warning",
                value="This will remove all character links and cannot be undone!",
                inline=False
            )
            
            embed.set_footer(text="Click the buttons below to confirm or cancel")
            
            # Create confirmation view with buttons
            class UnlinkConfirmView(discord.ui.View):
                def __init__(self, timeout=30):
                    super().__init__(timeout=timeout)
                    self.value = None
                    
                @discord.ui.button(label="✅ Confirm", style=discord.ButtonStyle.danger)
                async def confirm(self, button: discord.ui.Button, interaction: discord.Interaction):
                    if interaction.user.id != discord_id:
                        await interaction.response.send_message("❌ Only the command user can confirm this action!", ephemeral=True)
                        return
                    
                    self.value = True
                    self.stop()
                    
                    # Proceed with unlinking
                    try:
                        result = await self.bot.db_manager.players.delete_one({
                            "guild_id": guild_id,
                            "discord_id": discord_id
                        })
                        
                        if result.deleted_count > 0:
                            success_embed = discord.Embed(
                                title="✅ Characters Unlinked",
                                description="All your characters have been successfully unlinked!",
                                color=0x00FF00,
                                timestamp=datetime.now(timezone.utc)
                            )
                            success_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                            
                            await interaction.response.edit_message(embed=success_embed, view=None)
                        else:
                            error_embed = discord.Embed(
                                title="❌ Unlinking Failed",
                                description="Failed to unlink characters. Please try again.",
                                color=0xFF0000,
                                timestamp=datetime.now(timezone.utc)
                            )
                            await interaction.response.edit_message(embed=error_embed, view=None)
                            
                    except Exception as e:
                        logger.error(f"Failed to unlink characters in confirm button: {e}")
                        error_embed = discord.Embed(
                            title="❌ Unlinking Failed",
                            description="An error occurred while unlinking characters.",
                            color=0xFF0000,
                            timestamp=datetime.now(timezone.utc)
                        )
                        await interaction.response.edit_message(embed=error_embed, view=None)
                
                @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
                async def cancel(self, button: discord.ui.Button, interaction: discord.Interaction):
                    if interaction.user.id != discord_id:
                        await interaction.response.send_message("❌ Only the command user can cancel this action!", ephemeral=True)
                        return
                    
                    self.value = False
                    self.stop()
                    
                    cancel_embed = discord.Embed(
                        title="❌ Unlinking Cancelled",
                        description="Your characters remain linked.",
                        color=0xFFD700,
                        timestamp=datetime.now(timezone.utc)
                    )
                    cancel_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                    
                    await interaction.response.edit_message(embed=cancel_embed, view=None)
                
                async def on_timeout(self):
                    timeout_embed = discord.Embed(
                        title="⏰ Confirmation Timeout",
                        description="Unlinking cancelled due to timeout.",
                        color=0x808080,
                        timestamp=datetime.now(timezone.utc)
                    )
                    timeout_embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                    
                    # Disable all buttons and update message
                    for item in self.children:
                        item.disabled = True
                    
                    try:
                        # Get the original message through the interaction
                        if hasattr(self, 'message') and self.message:
                            await self.message.edit(embed=timeout_embed, view=self)
                    except Exception as e:
                        logger.error(f"Failed to edit message on timeout: {e}")
            
            # Create the view and store bot reference
            view = UnlinkConfirmView()
            view.bot = self.bot
            
            # Send confirmation message with buttons
            await ctx.respond(embed=embed, view=view)
            
            # Store message reference for timeout handling
            try:
                message = await ctx.original_response()
                view.message = message
            except Exception as e:
                logger.warning(f"Could not get original response for timeout handling: {e}")
                
        except Exception as e:
            logger.error(f"Failed to unlink characters: {e}")
            try:
                await ctx.respond("❌ Failed to unlink characters. Please try again.", ephemeral=True)
            except:
                # If ctx.respond fails, try followup
                try:
                    await ctx.followup.send("❌ Failed to unlink characters. Please try again.", ephemeral=True)
                except:
                    logger.error("Failed to send error message to user")

def setup(bot):
    bot.add_cog(Linking(bot))