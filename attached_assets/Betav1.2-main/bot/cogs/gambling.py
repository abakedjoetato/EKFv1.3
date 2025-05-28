
"""
Emerald's Killfeed - ELITE GAMBLING SYSTEM v5.0 (SUPREMACY)
Advanced animated casino with interactive views, premium integration
py-cord 2.6.1 compatibility with View components and message edits
"""

import asyncio
import random
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import discord
from discord.ext import commands
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class SlotsView(discord.ui.View):
    """Interactive slots view with spin button"""
    
    def __init__(self, gambling_cog, ctx, bet_amount):
        super().__init__(timeout=60)
        self.gambling_cog = gambling_cog
        self.ctx = ctx
        self.bet_amount = bet_amount
        self.spinning = False

    @discord.ui.button(label="🎰 SPIN REELS", style=discord.ButtonStyle.success, emoji="🎲")
    async def spin_slots(self, button: discord.ui.Button, interaction: discord.Interaction):
        if self.spinning:
            await interaction.response.send_message("⚠️ Reels already spinning!", ephemeral=True)
            return
            
        if interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("❌ Only the bettor can spin!", ephemeral=True)
            return

        self.spinning = True
        button.disabled = True
        await interaction.response.edit_message(view=self)

        # Execute animated slots sequence
        await self.gambling_cog._execute_animated_slots(interaction, self.bet_amount)

class BlackjackView(discord.ui.View):
    """Interactive blackjack view with game buttons"""
    
    def __init__(self, gambling_cog, ctx, bet_amount, player_cards, dealer_cards):
        super().__init__(timeout=120)
        self.gambling_cog = gambling_cog
        self.ctx = ctx
        self.bet_amount = bet_amount
        self.player_cards = player_cards
        self.dealer_cards = dealer_cards
        self.game_over = False

    @discord.ui.button(label="🃏 HIT", style=discord.ButtonStyle.primary, emoji="➕")
    async def hit_card(self, button: discord.ui.Button, interaction: discord.Interaction):
        if self.game_over or interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("❌ Game not available", ephemeral=True)
            return

        await interaction.response.defer()
        await self.gambling_cog._blackjack_hit(interaction, self)

    @discord.ui.button(label="🛡️ STAND", style=discord.ButtonStyle.secondary, emoji="✋")
    async def stand_hand(self, button: discord.ui.Button, interaction: discord.Interaction):
        if self.game_over or interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("❌ Game not available", ephemeral=True)
            return

        await interaction.response.defer()
        await self.gambling_cog._blackjack_stand(interaction, self)

    @discord.ui.button(label="💰 DOUBLE", style=discord.ButtonStyle.success, emoji="⬆️")
    async def double_down(self, button: discord.ui.Button, interaction: discord.Interaction):
        if self.game_over or interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("❌ Game not available", ephemeral=True)
            return

        await interaction.response.defer()
        await self.gambling_cog._blackjack_double(interaction, self)

class RouletteView(discord.ui.View):
    """Interactive roulette view with betting options"""
    
    def __init__(self, gambling_cog, ctx, bet_amount, bet_choice):
        super().__init__(timeout=60)
        self.gambling_cog = gambling_cog
        self.ctx = ctx
        self.bet_amount = bet_amount
        self.bet_choice = bet_choice
        self.spinning = False

    @discord.ui.button(label="🎯 SPIN WHEEL", style=discord.ButtonStyle.danger, emoji="🌀")
    async def spin_wheel(self, button: discord.ui.Button, interaction: discord.Interaction):
        if self.spinning:
            await interaction.response.send_message("⚠️ Wheel already spinning!", ephemeral=True)
            return
            
        if interaction.user.id != self.ctx.user.id:
            await interaction.response.send_message("❌ Only the bettor can spin!", ephemeral=True)
            return

        self.spinning = True
        button.disabled = True
        await interaction.response.edit_message(view=self)

        # Execute animated roulette sequence
        await self.gambling_cog._execute_animated_roulette(interaction, self.bet_amount, self.bet_choice)

class Gambling(commands.Cog):
    """
    ELITE GAMBLING SYSTEM (PREMIUM)
    - Animated /slots with 3-reel simulation and themed symbols
    - Interactive /blackjack with Hit/Stand/Double buttons
    - Animated /roulette with realistic wheel spinning
    - Premium-only access with economy integration
    - py-cord 2.6.1 View components and message edits
    """

    def __init__(self, bot):
        self.bot = bot
        self.user_locks: Dict[str, asyncio.Lock] = {}
        self.active_games: Dict[str, str] = {}

        # Elite slot symbols with themed emerald/deadside elements
        self.slot_symbols = {
            '💎': {'weight': 2, 'value': 100, 'name': 'EMERALD'},
            '7️⃣': {'weight': 3, 'value': 50, 'name': 'LUCKY SEVEN'},
            '💀': {'weight': 5, 'value': 25, 'name': 'SKULL'},
            '📦': {'weight': 8, 'value': 15, 'name': 'LOOT CRATE'},
            '⚡': {'weight': 12, 'value': 10, 'name': 'ENERGY'},
            '🔫': {'weight': 15, 'value': 5, 'name': 'WEAPON'},
            '🍒': {'weight': 25, 'value': 3, 'name': 'CHERRY'},
            '🍋': {'weight': 30, 'value': 2, 'name': 'LEMON'}
        }

        # Themed gambling messages
        self.slot_messages = [
            "The wasteland's fortune favors the bold",
            "Emerald crystals align in your favor",
            "Death and riches dance together",
            "Survival rewards the desperate",
            "The reels of fate have spoken"
        ]

        self.roulette_messages = [
            "The wheel of fortune spins through bloodshed",
            "Luck determines who survives the night",
            "Chance rules this forsaken wasteland",
            "The gods of gambling smile upon you",
            "Fortune carved from desperation"
        ]

        self.blackjack_messages = [
            "Cards determine your survival",
            "Beat the dealer, beat the odds",
            "Twenty-one or bust in this wasteland",
            "The house edge cuts like a blade",
            "Blackjack supremacy achieved"
        ]

    def get_user_lock(self, user_key: str) -> asyncio.Lock:
        """Get or create a lock for a user to prevent concurrent bets"""
        if user_key not in self.user_locks:
            self.user_locks[user_key] = asyncio.Lock()
        return self.user_locks[user_key]

    async def check_premium_server(self, guild_id: int) -> bool:
        """Check if guild has premium access for gambling features"""
        try:
            guild_doc = await self.bot.db_manager.get_guild(guild_id)
            if not guild_doc:
                return False

            servers = guild_doc.get('servers', [])
            for server_config in servers:
                server_id = server_config.get('server_id', 'default')
                if await self.bot.db_manager.is_premium_server(guild_id, server_id):
                    return True

            return False
        except Exception as e:
            logger.error(f"Error checking premium server: {e}")
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

    def generate_slot_reels(self) -> List[str]:
        """Generate weighted random slot results"""
        symbols = list(self.slot_symbols.keys())
        weights = [self.slot_symbols[symbol]['weight'] for symbol in symbols]
        
        return [random.choices(symbols, weights=weights)[0] for _ in range(3)]

    def calculate_slot_payout(self, reels: List[str], bet: int) -> tuple[int, str]:
        """Calculate slot payout and win type"""
        # Triple match
        if reels[0] == reels[1] == reels[2]:
            symbol = reels[0]
            multiplier = self.slot_symbols[symbol]['value']
            name = self.slot_symbols[symbol]['name']
            return bet * multiplier, f"🎰 TRIPLE {name}! JACKPOT!"

        # Double match
        elif reels[0] == reels[1] or reels[1] == reels[2] or reels[0] == reels[2]:
            return bet * 2, "🎲 DOUBLE MATCH!"

        # No match
        return 0, "💸 NO MATCH"

    def draw_card(self) -> tuple[str, str, int]:
        """Draw a playing card with suit, face, and value"""
        values = [2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11]  # 11 = Ace
        suits = ['♠️', '♥️', '♦️', '♣️']
        
        value = random.choice(values)
        suit = random.choice(suits)
        
        if value == 11:
            return ("A", suit, 11)
        elif value == 10:
            face = random.choice(["10", "J", "Q", "K"])
            return (face, suit, 10)
        else:
            return (str(value), suit, value)

    def calculate_hand_value(self, cards: List[tuple]) -> int:
        """Calculate blackjack hand value with ace handling"""
        total = sum(card[2] for card in cards)
        aces = sum(1 for card in cards if card[0] == "A")

        while total > 21 and aces > 0:
            total -= 10
            aces -= 1

        return total

    def format_cards(self, cards: List[tuple]) -> List[str]:
        """Format cards for display"""
        return [f"{card[0]}{card[1]}" for card in cards]

    @discord.slash_command(name="slots", description="🎰 Elite animated slot machine with emerald themes")
    async def slots(self, ctx: discord.ApplicationContext, bet: int):
        """Elite animated slot machine with 3-reel simulation"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            user_key = f"{guild_id}_{discord_id}"

            # Premium access check
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🚫 PREMIUM ACCESS REQUIRED",
                    description="The Elite Gambling System requires premium server access.\n\n**Contact server administrators for premium activation.**",
                    color=0xff5e5e
                )
                embed.set_thumbnail(url="attachment://Gamble.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
                
                file = discord.File('assets/Gamble.png', filename='Gamble.png')
                await ctx.respond(embed=embed, file=file, ephemeral=True)
                return

            # Validate bet amount
            if bet <= 0:
                await ctx.respond("❌ Bet amount must be positive!", ephemeral=True)
                return

            if bet > 25000:
                await ctx.respond("❌ Maximum bet is $25,000!", ephemeral=True)
                return

            # Use lock to prevent concurrent gambling
            async with self.get_user_lock(user_key):
                # Check wallet balance
                wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
                if wallet['balance'] < bet:
                    await ctx.respond(
                        f"❌ Insufficient funds! You have **${wallet['balance']:,}** but need **${bet:,}**",
                        ephemeral=True
                    )
                    return

                # Create initial slots setup with interactive view
                embed = discord.Embed(
                    title="🎰 EMERALD SLOTS",
                    description=f"**Bet Amount:** ${bet:,}\n**Balance:** ${wallet['balance']:,}\n\n*Click SPIN REELS to begin the sequence*",
                    color=0x7f5af0
                )
                embed.add_field(
                    name="💎 PAYOUT TABLE",
                    value="```💎💎💎 = 100x Bet (EMERALD JACKPOT)\n7️⃣7️⃣7️⃣ = 50x Bet (LUCKY SEVENS)\n💀💀💀 = 25x Bet (DEATH MATCH)\n📦📦📦 = 15x Bet (LOOT BONANZA)\nDouble Match = 2x Bet```",
                    inline=False
                )
                embed.set_thumbnail(url="attachment://Gamble.png")
                embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

                file = discord.File('assets/Gamble.png', filename='Gamble.png')
                view = SlotsView(self, ctx, bet)

                await ctx.respond(embed=embed, file=file, view=view)

        except Exception as e:
            logger.error(f"Failed to initialize slots: {e}")
            await ctx.respond("❌ Slots initialization failed. Please try again.", ephemeral=True)

    async def _execute_animated_slots(self, interaction: discord.Interaction, bet: int):
        """Execute animated slots sequence with message edits"""
        try:
            guild_id = interaction.guild.id
            discord_id = interaction.user.id

            # Deduct bet amount
            await self.bot.db_manager.update_wallet(guild_id, discord_id, -bet, "gambling_slots")

            # Animated spinning sequence (3 frames)
            spin_frames = [
                "🎰 🎰 🎰\n⚡ SPINNING... ⚡",
                "💫 💫 💫\n🌀 ROLLING... 🌀", 
                "✨ ✨ ✨\n🎲 FINALIZING... 🎲"
            ]

            for i, frame in enumerate(spin_frames):
                embed = discord.Embed(
                    title="🎰 EMERALD SLOTS - SPINNING",
                    description=f"**Bet:** ${bet:,}\n\n{frame}",
                    color=0x7f5af0
                )
                embed.set_thumbnail(url="attachment://Gamble.png")
                embed.set_footer(text="The reels of fate are spinning...")

                await interaction.edit_original_response(embed=embed, view=None)
                await asyncio.sleep(1.5)

            # Generate final results
            reels = self.generate_slot_reels()
            winnings, win_type = self.calculate_slot_payout(reels, bet)

            # Update wallet with winnings
            if winnings > 0:
                await self.bot.db_manager.update_wallet(guild_id, discord_id, winnings, "gambling_slots")

            # Add transaction event
            net_result = winnings - bet
            await self.add_wallet_event(
                guild_id, discord_id, net_result, "gambling_slots",
                f"Slots: {' '.join(reels)} | Bet: ${bet:,} | Win: ${winnings:,}"
            )

            # Get updated balance
            updated_wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)

            # Create final result embed
            embed = discord.Embed(
                title="🎰 EMERALD SLOTS - RESULT",
                color=0x00d38a if winnings > 0 else 0xff5e5e
            )

            # Slot display
            reel_display = f"| {reels[0]} | {reels[1]} | {reels[2]} |"
            embed.add_field(name="🎲 Final Reels", value=f"```{reel_display}```", inline=False)

            # Result summary
            if winnings > 0:
                embed.add_field(
                    name="🎉 VICTORY",
                    value=f"**{win_type}**\n💰 Won: ${winnings:,}\n📈 Net: +${net_result:,}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="💸 DEFEAT",
                    value=f"**{win_type}**\n💔 Lost: ${bet:,}\n📉 Net: -${bet:,}",
                    inline=True
                )

            embed.add_field(
                name="💳 New Balance",
                value=f"${updated_wallet['balance']:,}",
                inline=True
            )

            # Add themed message
            themed_msg = random.choice(self.slot_messages)
            embed.add_field(name="⚔️ Combat Log", value=f"*{themed_msg}*", inline=False)

            embed.set_thumbnail(url="attachment://Gamble.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            logger.error(f"Failed to execute animated slots: {e}")
            await interaction.followup.send("❌ Slots execution failed!", ephemeral=True)

    @discord.slash_command(name="roulette", description="🎯 Elite animated roulette with realistic wheel physics")
    async def roulette(self, ctx: discord.ApplicationContext, bet: int, 
                      choice: discord.Option(str, "Choose your bet (red/black/green/even/odd/low/high or number 0-36)", choices=[
                          "red", "black", "green", "even", "odd", "low", "high"
                      ])):
        """Elite animated roulette with realistic wheel simulation"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            user_key = f"{guild_id}_{discord_id}"

            # Premium check
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🚫 PREMIUM ACCESS REQUIRED",
                    description="Elite Roulette requires premium server access.",
                    color=0xff5e5e
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return

            # Validate bet amount
            if bet <= 0:
                await ctx.respond("❌ Bet amount must be positive!", ephemeral=True)
                return

            if bet > 2000:
                await ctx.respond("❌ Maximum bet is $2,000!", ephemeral=True)
                return

            # Validate choice - support both dropdown choices and manual number input
            choice_lower = choice.lower().strip()
            
            # Check if it's a valid color/type bet
            valid_type_bets = {'red', 'black', 'green', 'odd', 'even', 'low', 'high'}
            
            # Check if it's a valid number (0-36)
            is_valid_number = False
            if choice_lower.isdigit():
                num = int(choice_lower)
                if 0 <= num <= 36:
                    is_valid_number = True
            
            # Validate the choice
            if choice_lower not in valid_type_bets and not is_valid_number:
                await ctx.respond(
                    "❌ Invalid choice! Use:\n"
                    "• **Colors:** red, black, green\n"
                    "• **Types:** even, odd, low (1-18), high (19-36)\n"
                    "• **Numbers:** 0-36 (type the number manually)\n"
                    "💡 For specific numbers, just type the number in the choice field.",
                    ephemeral=True
                )
                return

            # Use lock to prevent concurrent games
            async with self.get_user_lock(user_key):
                # Check balance
                wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
                if wallet['balance'] < bet:
                    await ctx.respond(
                        f"❌ Insufficient funds! You have **${wallet['balance']:,}** but need **${bet:,}**",
                        ephemeral=True
                    )
                    return

                # Create interactive roulette view
                view = RouletteView(self, ctx, bet, choice_lower)
                
                embed = discord.Embed(
                    title="🎯 ELITE ROULETTE",
                    description=f"**Bet:** ${bet:,}\n**Choice:** {choice_lower.title()}\n\nClick **SPIN WHEEL** to begin!",
                    color=0xff6b35
                )
                embed.add_field(
                    name="🎲 Your Bet",
                    value=f"${bet:,} on **{choice_lower.title()}**",
                    inline=True
                )
                embed.add_field(
                    name="💰 Balance",
                    value=f"${wallet['balance']:,}",
                    inline=True
                )
                embed.set_footer(text="🎯 Good luck! Click SPIN to play")
                
                await ctx.respond(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Failed to initialize roulette: {e}")
            await ctx.respond("❌ Roulette initialization failed.", ephemeral=True)

    async def _execute_animated_roulette(self, interaction: discord.Interaction, bet: int, choice: str):
        """Execute animated roulette sequence"""
        try:
            guild_id = interaction.guild.id
            discord_id = interaction.user.id

            # Deduct bet
            await self.bot.db_manager.update_wallet(guild_id, discord_id, -bet, "gambling_roulette")

            # Animated spinning sequence (5 frames)
            spin_sequence = [
                "🌀 Ball released...",
                "⚡ Rolling around the wheel...",
                "💫 Bouncing between numbers...",
                "🎯 Slowing down...",
                "✨ Final bounce..."
            ]

            for i, frame in enumerate(spin_sequence):
                embed = discord.Embed(
                    title="🎯 EMERALD ROULETTE - SPINNING",
                    description=f"**Bet:** ${bet:,} on **{choice.upper()}**\n\n{frame}",
                    color=0xef4444
                )
                embed.set_thumbnail(url="attachment://Gamble.png")
                embed.set_footer(text="The wheel determines your fate...")

                await interaction.edit_original_response(embed=embed, view=None)
                await asyncio.sleep(1.2)

            # Generate result
            number = random.randint(0, 36)
            if number == 0:
                color = "green"
                color_emoji = "🟢"
            elif number in [1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36]:
                color = "red"
                color_emoji = "🔴"
            else:
                color = "black"
                color_emoji = "⚫"

            # Check win conditions
            win = False
            multiplier = 0

            if choice.isdigit() and int(choice) == number:
                win = True
                multiplier = 35
            elif choice == color:
                win = True
                multiplier = 35 if color == "green" else 1
            elif choice == "even" and number % 2 == 0 and number != 0:
                win = True
                multiplier = 1
            elif choice == "odd" and number % 2 == 1:
                win = True
                multiplier = 1
            elif choice == "low" and 1 <= number <= 18:
                win = True
                multiplier = 1
            elif choice == "high" and 19 <= number <= 36:
                win = True
                multiplier = 1

            # Calculate winnings
            winnings = bet * (multiplier + 1) if win else 0

            if winnings > 0:
                await self.bot.db_manager.update_wallet(guild_id, discord_id, winnings, "gambling_roulette")

            # Add event
            net_result = winnings - bet
            await self.add_wallet_event(
                guild_id, discord_id, net_result, "gambling_roulette",
                f"Roulette: {number} {color} | Bet: {choice} ${bet:,} | Win: ${winnings:,}"
            )

            # Get updated balance
            updated_wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)

            # Create result embed
            embed = discord.Embed(
                title="🎯 EMERALD ROULETTE - RESULT",
                color=0x00d38a if win else 0xff5e5e
            )

            embed.add_field(
                name="🎲 Winning Number",
                value=f"**{color_emoji} {number} ({color.upper()})**",
                inline=False
            )

            if win:
                embed.add_field(
                    name="🎉 VICTORY",
                    value=f"Your bet on **{choice.upper()}** wins!\n💰 Payout: ${winnings:,}\n📈 Net: +${net_result:,}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="💸 DEFEAT", 
                    value=f"Your bet on **{choice.upper()}** loses.\n💔 Lost: ${bet:,}\n📉 Net: -${bet:,}",
                    inline=True
                )

            embed.add_field(
                name="💳 New Balance",
                value=f"${updated_wallet['balance']:,}",
                inline=True
            )

            # Themed message
            themed_msg = random.choice(self.roulette_messages)
            embed.add_field(name="⚔️ Combat Log", value=f"*{themed_msg}*", inline=False)

            embed.set_thumbnail(url="attachment://Gamble.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            logger.error(f"Failed to execute roulette: {e}")
            await interaction.followup.send("❌ Roulette execution failed!", ephemeral=True)

    @discord.slash_command(name="blackjack", description="🃏 Elite interactive blackjack with Hit/Stand/Double buttons")
    async def blackjack(self, ctx: discord.ApplicationContext, bet: int):
        """Elite interactive blackjack with full button controls"""
        try:
            guild_id = ctx.guild.id
            discord_id = ctx.user.id
            user_key = f"{guild_id}_{discord_id}"

            # Premium check
            if not await self.check_premium_server(guild_id):
                embed = discord.Embed(
                    title="🚫 PREMIUM ACCESS REQUIRED",
                    description="Elite Blackjack requires premium server access.",
                    color=0xff5e5e
                )
                await ctx.respond(embed=embed, ephemeral=True)
                return

            # Validate bet
            if bet <= 0 or bet > 25000:
                await ctx.respond("❌ Bet must be between $1 and $25,000!", ephemeral=True)
                return

            async with self.get_user_lock(user_key):
                # Check balance
                wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
                if wallet['balance'] < bet:
                    await ctx.respond(f"❌ Insufficient funds! You have **${wallet['balance']:,}**", ephemeral=True)
                    return

                # Deduct bet
                await self.bot.db_manager.update_wallet(guild_id, discord_id, -bet, "gambling_blackjack")

                # Deal initial cards
                player_cards = [self.draw_card(), self.draw_card()]
                dealer_cards = [self.draw_card(), self.draw_card()]

                player_total = self.calculate_hand_value(player_cards)
                dealer_total = self.calculate_hand_value(dealer_cards)

                # Check for immediate blackjack
                player_blackjack = player_total == 21
                dealer_blackjack = dealer_total == 21

                if player_blackjack or dealer_blackjack:
                    await self._blackjack_finish_game(ctx, bet, player_cards, dealer_cards, "initial")
                    return

                # Create interactive game
                embed = discord.Embed(
                    title="🃏 EMERALD BLACKJACK",
                    color=0x22c55e
                )

                player_display = ' '.join(self.format_cards(player_cards))
                dealer_display = f"{self.format_cards(dealer_cards)[0]} 🎴"  # Hide second card

                embed.add_field(
                    name="🃏 Your Hand",
                    value=f"**{player_display}** (Total: {player_total})",
                    inline=False
                )

                embed.add_field(
                    name="🎴 Dealer Hand",
                    value=f"**{dealer_display}** (Total: ?)",
                    inline=False
                )

                embed.add_field(name="💰 Bet", value=f"${bet:,}", inline=True)
                embed.add_field(name="🎯 Goal", value="Get 21 or beat dealer", inline=True)

                embed.set_thumbnail(url="attachment://Gamble.png")
                embed.set_footer(text="Choose your action: Hit, Stand, or Double")

                file = discord.File('assets/Gamble.png', filename='Gamble.png')
                view = BlackjackView(self, ctx, bet, player_cards, dealer_cards)

                await ctx.respond(embed=embed, file=file, view=view)

        except Exception as e:
            logger.error(f"Failed to initialize blackjack: {e}")
            await ctx.respond("❌ Blackjack initialization failed.", ephemeral=True)

    async def _blackjack_hit(self, interaction: discord.Interaction, view: BlackjackView):
        """Handle blackjack hit action"""
        try:
            # Draw new card
            new_card = self.draw_card()
            view.player_cards.append(new_card)
            
            player_total = self.calculate_hand_value(view.player_cards)
            
            # Update display
            player_display = ' '.join(self.format_cards(view.player_cards))
            dealer_display = f"{self.format_cards(view.dealer_cards)[0]} 🎴"

            embed = discord.Embed(title="🃏 EMERALD BLACKJACK", color=0x22c55e)
            embed.add_field(
                name="🃏 Your Hand", 
                value=f"**{player_display}** (Total: {player_total})",
                inline=False
            )
            embed.add_field(
                name="🎴 Dealer Hand",
                value=f"**{dealer_display}** (Total: ?)",
                inline=False
            )

            if player_total > 21:
                # Bust - end game
                view.game_over = True
                view.clear_items()
                embed.add_field(name="💥 BUST!", value="You went over 21!", inline=False)
                await self._blackjack_finish_game_from_view(interaction, view, "bust")
            else:
                embed.set_footer(text="Choose your next action")
                await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Blackjack hit error: {e}")

    async def _blackjack_stand(self, interaction: discord.Interaction, view: BlackjackView):
        """Handle blackjack stand action"""
        try:
            view.game_over = True
            view.clear_items()
            await self._blackjack_finish_game_from_view(interaction, view, "stand")

        except Exception as e:
            logger.error(f"Blackjack stand error: {e}")

    async def _blackjack_double(self, interaction: discord.Interaction, view: BlackjackView):
        """Handle blackjack double down"""
        try:
            # Check if user has enough for double
            guild_id = interaction.guild.id
            discord_id = interaction.user.id
            
            wallet = await self.bot.db_manager.get_wallet(guild_id, discord_id)
            if wallet['balance'] < view.bet_amount:
                await interaction.followup.send("❌ Insufficient funds to double down!", ephemeral=True)
                return

            # Deduct additional bet
            await self.bot.db_manager.update_wallet(guild_id, discord_id, -view.bet_amount, "gambling_blackjack")
            view.bet_amount *= 2

            # Draw one card and end turn
            new_card = self.draw_card()
            view.player_cards.append(new_card)
            view.game_over = True
            view.clear_items()

            await self._blackjack_finish_game_from_view(interaction, view, "double")

        except Exception as e:
            logger.error(f"Blackjack double error: {e}")

    async def _blackjack_finish_game_from_view(self, interaction: discord.Interaction, view: BlackjackView, action: str):
        """Finish blackjack game from view interaction"""
        try:
            # Dealer plays
            while self.calculate_hand_value(view.dealer_cards) < 17:
                view.dealer_cards.append(self.draw_card())

            player_total = self.calculate_hand_value(view.player_cards)
            dealer_total = self.calculate_hand_value(view.dealer_cards)

            # Determine winner
            winnings = 0
            status = ""

            if player_total > 21:
                status = "💥 BUST! You lose!"
            elif dealer_total > 21:
                winnings = view.bet_amount * 2
                status = "🎉 Dealer BUST! You win!"
            elif player_total == 21 and len(view.player_cards) == 2:
                winnings = int(view.bet_amount * 2.5)
                status = "🃏 BLACKJACK! You win!"
            elif player_total > dealer_total:
                winnings = view.bet_amount * 2
                status = "🎉 You win!"
            elif dealer_total > player_total:
                status = "💸 Dealer wins!"
            else:
                winnings = view.bet_amount
                status = "🤝 Push! Tie game!"

            # Update wallet
            if winnings > 0:
                await self.bot.db_manager.update_wallet(interaction.guild.id, interaction.user.id, winnings, "gambling_blackjack")

            # Create final embed
            net_result = winnings - view.bet_amount
            updated_wallet = await self.bot.db_manager.get_wallet(interaction.guild.id, interaction.user.id)

            embed = discord.Embed(
                title="🃏 EMERALD BLACKJACK - RESULT",
                color=0x00d38a if winnings > 0 else 0xff5e5e
            )

            player_display = ' '.join(self.format_cards(view.player_cards))
            dealer_display = ' '.join(self.format_cards(view.dealer_cards))

            embed.add_field(
                name="🃏 Final Hands",
                value=f"**You:** {player_display} (Total: {player_total})\n**Dealer:** {dealer_display} (Total: {dealer_total})",
                inline=False
            )

            embed.add_field(name="🎯 Result", value=status, inline=True)
            embed.add_field(name="💰 Net", value=f"${net_result:+,}", inline=True)
            embed.add_field(name="💳 Balance", value=f"${updated_wallet['balance']:,}", inline=True)

            # Add themed message
            themed_msg = random.choice(self.blackjack_messages)
            embed.add_field(name="⚔️ Combat Log", value=f"*{themed_msg}*", inline=False)

            embed.set_thumbnail(url="attachment://Gamble.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await interaction.edit_original_response(embed=embed, view=None)

            # Add wallet event
            await self.add_wallet_event(
                interaction.guild.id, interaction.user.id, net_result, "gambling_blackjack",
                f"Blackjack: P:{player_total} D:{dealer_total} | Bet: ${view.bet_amount:,} | Win: ${winnings:,}"
            )

        except Exception as e:
            logger.error(f"Blackjack finish error: {e}")

    async def _blackjack_finish_game(self, ctx, bet: int, player_cards: List, dealer_cards: List, game_type: str):
        """Finish immediate blackjack game (for natural 21s)"""
        try:
            player_total = self.calculate_hand_value(player_cards)
            dealer_total = self.calculate_hand_value(dealer_cards)

            player_blackjack = player_total == 21
            dealer_blackjack = dealer_total == 21

            winnings = 0
            status = ""

            if player_blackjack and not dealer_blackjack:
                winnings = int(bet * 2.5)
                status = "🃏 BLACKJACK! You win!"
            elif dealer_blackjack and not player_blackjack:
                status = "💸 Dealer Blackjack! You lose!"
            elif player_blackjack and dealer_blackjack:
                winnings = bet
                status = "🤝 Push! Both Blackjack!"

            if winnings > 0:
                await self.bot.db_manager.update_wallet(ctx.guild.id, ctx.user.id, winnings, "gambling_blackjack")

            net_result = winnings - bet
            updated_wallet = await self.bot.db_manager.get_wallet(ctx.guild.id, ctx.user.id)

            # Create result embed
            embed = discord.Embed(
                title="🃏 EMERALD BLACKJACK - RESULT",
                color=0x00d38a if winnings > 0 else 0xff5e5e
            )

            player_display = ' '.join(self.format_cards(player_cards))
            dealer_display = ' '.join(self.format_cards(dealer_cards))

            embed.add_field(
                name="🃏 Final Hands",
                value=f"**You:** {player_display} (Total: {player_total})\n**Dealer:** {dealer_display} (Total: {dealer_total})",
                inline=False
            )

            embed.add_field(name="🎯 Result", value=status, inline=True)
            embed.add_field(name="💰 Net", value=f"${net_result:+,}", inline=True)
            embed.add_field(name="💳 Balance", value=f"${updated_wallet['balance']:,}", inline=True)

            themed_msg = random.choice(self.blackjack_messages)
            embed.add_field(name="⚔️ Combat Log", value=f"*{themed_msg}*", inline=False)

            embed.set_thumbnail(url="attachment://Gamble.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            file = discord.File('assets/Gamble.png', filename='Gamble.png')
            await ctx.respond(embed=embed, file=file)

            # Add wallet event
            await self.add_wallet_event(
                ctx.guild.id, ctx.user.id, net_result, "gambling_blackjack",
                f"Blackjack: P:{player_total} D:{dealer_total} | Bet: ${bet:,} | Win: ${winnings:,}"
            )

        except Exception as e:
            logger.error(f"Blackjack immediate finish error: {e}")

def setup(bot):
    bot.add_cog(Gambling(bot))
