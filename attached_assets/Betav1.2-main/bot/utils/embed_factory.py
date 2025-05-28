"""
Emerald's Killfeed - Embed Factory
Centralized embed creation with consistent theming and styling
"""

import discord
import random
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from zoneinfo import ZoneInfo
import asyncio

class EmbedFactory:
    """
    Centralized embed factory for consistent Discord embed styling
    """

    # Color schemes for different event types
    COLORS = {
        'mission_ready': 0x2ECC71,     # Green
        'airdrop': 0xF39C12,           # Orange
        'helicrash': 0xC0392B,         # Red
        'player_join': 0x2980B9,       # Blue
        'player_leave': 0x8E44AD,      # Purple
        'vehicle_spawn': 0x95A5A6,     # Gray
        'success': 0x00FF00,
        'error': 0xFF0000,
        'warning': 0xFFD700,
        'info': 0x1E90FF,
        'default': 0x7289DA,
        'killfeed': 0x00d38a,
        'suicide': 0xff5e5e,
        'fall': 0xc084fc,
        'slots': 0x7f5af0,
        'roulette': 0xef4444,
        'blackjack': 0x22c55e,
        'profile': 0x00d38a,
        'bounty': 0xfacc15,
        'admin': 0x64748b,
        'leaderboard': 0xFFD700,
        'trader': 0xFFD700
    }

    # Thumbnail mappings for event types
    THUMBNAILS = {
        'mission_ready': 'Mission.png',
        'airdrop': 'Airdrop.png',
        'helicrash': 'Helicrash.png',
        'player_join': 'Connections.png',
        'player_leave': 'Connections.png',
        'vehicle_spawn': 'Vehicle.png',
        'killfeed': 'Killfeed.png',
        'leaderboard': 'Leaderboard.png',
        'trader': 'Trader.png',
        'default': 'main.png'
    }

    # Militaristic message variations for different event types
    MILITARY_MESSAGES = {
        'mission_ready': [
            "Tactical teams, move to positions! Mission zone is hot.",
            "All units converge on target area. Mission parameters confirmed.",
            "Operational zone secured. Commence tactical deployment.",
            "Strike team authorization granted. Proceed with caution.",
            "Combat zone established. All personnel maintain readiness."
        ],
        'airdrop': [
            "Supply aircraft detected on approach. Valuable cargo incoming!",
            "High-priority supplies inbound. Secure the drop zone immediately.",
            "Air support deploying critical resources. Move to intercept.",
            "Supply drop confirmed. Multiple hostiles may converge on location.",
            "Logistics package incoming. Establish perimeter around landing zone."
        ],
        'helicrash': [
            "Aircraft down! Crash site detected. High-value loot reported.",
            "Emergency beacon active. Wreckage contains valuable equipment.",
            "Helicopter eliminated. Salvage operations are a go.",
            "Aircraft debris located. Expect heavy resistance at crash site.",
            "Air asset compromised. Recovery teams deploy immediately."
        ],
        'player_join': [
            "Operative connecting to battlefield communications.",
            "New combatant entering the operation zone.",
            "Additional personnel joining tactical network.",
            "Reinforcement arriving at forward operating base.",
            "Field operative establishing secure connection."
        ],
        'player_leave': [
            "Operative disconnecting from tactical network.",
            "Personnel departing from operation zone.",
            "Field agent ending mission participation.",
            "Combatant leaving battlefield communications.",
            "Tactical unit withdrawing from active duty."
        ],
        'vehicle_spawn': [
            "Transportation asset deployed to the field.",
            "Vehicle unit now available for tactical operations.",
            "Mechanical support deployed to combat zone.",
            "Transport vehicle assigned to operational area.",
            "Mobile asset ready for field deployment."
        ]
    }

    # Tactical message variations
    TACTICAL_MESSAGES = {
        'trader': [
            "Black market trader has arrived; re-arm and re-supply.",
            "Trading post established; secure the area for commerce.",
            "Merchant convoy spotted; exchange intel for resources.",
            "The market is open; opportunity knocks for those who dare.",
            "New deals on the horizon; approach the trader with caution."
        ]
    }

    # Title pools for different embed types
    TITLE_POOLS = {
        'killfeed': [
            "Silhouette Erased",
            "Hostile Removed",
            "Contact Dismantled",
            "Kill Confirmed",
            "Eyes Off Target"
        ],
        'suicide': [
            "Self-Termination Logged",
            "Manual Override",
            "Exit Chosen"
        ],
        'fall': [
            "Gravity Kill Logged",
            "Terminal Descent",
            "Cliffside Casualty"
        ],
        'bounty': [
            "Target Flagged",
            "HVT Logged",
            "Kill Contract Active"
        ],
        'mission_ready': [
            "Contract Activated",
            "Target Zone Marked",
            "Mission Greenlit"
        ],
        'vehicle_spawn': [
            "Asset Deployed",
            "Logistics Confirmed"
        ],
        'player_join': [
            "Connection Established",
            "New Arrival Detected"
        ],
        'player_leave': [
            "Connection Lost",
            "Departure Recorded"
        ],
        'airdrop': [
            "Supplies Incoming",
            "Package Deployed"
        ],
        'helicrash': [
            "Crash Detected",
            "Wreckage Located"
        ],
        'trader': [
            "Trading Post Open",
            "Merchant Sighted",
            "Market Deployed"
        ]
    }

    # Combat log message pools
    COMBAT_LOGS = {
        'kill': [
            "Another shadow fades from the wasteland.",
            "The survivor count drops by one.",
            "Territory claimed through violence.",
            "Blood marks another chapter in survival.",
            "The weak have been culled from the herd.",
            "Death arrives on schedule in Deadside.",
            "One less mouth to feed in this barren world.",
            "The food chain adjusts itself once more."
        ],
        'suicide': [
            "Sometimes the only escape is through the void.",
            "The wasteland claims another volunteer.",
            "Exit strategy: permanent.",
            "Final decision executed successfully.",
            "The burden of survival lifted by choice.",
            "Another soul releases itself from this hell."
        ],
        'fall': [
            "Gravity shows no mercy in the wasteland.",
            "The ground always wins in the end.",
            "Physics delivers its final verdict.",
            "Another lesson in terminal velocity.",
            "The earth reclaims what fell from above.",
            "Descent complete. No survivors."
        ],
        'gambling': [
            "Fortune favors the desperate in Deadside.",
            "The house edge cuts deeper than any blade.",
            "Luck is just another scarce resource here.",
            "Survived the dealer. Survived the odds.",
            "In this wasteland, even chance is hostile.",
            "Risk and reward dance their eternal waltz."
        ],
        'bounty': [
            "A price on their head. A target on their back.",
            "The hunter becomes the hunted.",
            "Blood money flows through these lands.",
            "Marked for termination by popular demand.",
            "Contract issued. Payment pending delivery.",
            "The kill order has been authorized."
        ]
    }

    @staticmethod
    async def get_leaderboard_title(stat_type: str) -> str:
        """Get randomized themed title for leaderboard type"""
        titles = {
            'kills': ["🏆 Elite Eliminators", "⚔️ Death Dealers", "🔫 Combat Champions"],
            'deaths': ["💀 Most Fallen", "⚰️ Battlefield Casualties", "🪦 Frequent Respawners"],
            'kdr': ["📊 Kill/Death Masters", "🎯 Efficiency Legends", "⚡ Combat Elites"],
            'distance': ["🏹 Long Range Snipers", "📏 Distance Champions", "🎯 Precision Masters"],
            'weapons': ["🔫 Arsenal Analysis", "⚔️ Weapon Mastery", "💥 Combat Tools"],
            'factions': ["🏛️ Faction Dominance", "⚔️ Alliance Power", "🛡️ Faction Rankings"]
        }
        return random.choice(titles.get(stat_type, ["📊 Leaderboard"]))

    @staticmethod
    async def get_leaderboard_thumbnail(stat_type: str) -> str:
        """Get stat-specific thumbnail URL - all use Leaderboard.png"""
        thumbnails = {
            'kills': 'attachment://Leaderboard.png',
            'deaths': 'attachment://Leaderboard.png',
            'kdr': 'attachment://Leaderboard.png',
            'distance': 'attachment://Leaderboard.png',
            'weapons': 'attachment://Leaderboard.png',
            'factions': 'attachment://Leaderboard.png'
        }
        return thumbnails.get(stat_type, 'attachment://Leaderboard.png')

    @staticmethod
    async def build(embed_type: str, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """
        Build an embed of the specified type with provided data

        Args:
            embed_type: Type of embed to create
            data: Data dictionary containing embed content

        Returns:
            Tuple of (discord.Embed, discord.File or None)
        """
        if embed_type == 'killfeed':
            return EmbedFactory._build_killfeed(data)
        elif embed_type == 'suicide':
            return EmbedFactory._build_suicide(data)
        elif embed_type == 'fall':
            return EmbedFactory._build_fall(data)
        elif embed_type == 'slots':
            return EmbedFactory._build_slots(data)
        elif embed_type == 'roulette':
            return EmbedFactory._build_roulette(data)
        elif embed_type == 'blackjack':
            return EmbedFactory._build_blackjack(data)
        elif embed_type == 'profile':
            return EmbedFactory._build_profile(data)
        elif embed_type == 'bounty':
            return EmbedFactory._build_bounty(data)
        elif embed_type == 'admin':
            return EmbedFactory._build_admin(data)
        elif embed_type == 'comparison':
            return EmbedFactory._build_comparison(data)
        elif embed_type == 'leaderboard':
            # Get stat type from data for dynamic styling
            stat_type = data.get('stat_type', 'kills')
            style_variant = data.get('style_variant', stat_type)

            # Use dynamic title generation
            title = data.get('title') or await EmbedFactory.get_leaderboard_title(stat_type)

            embed = discord.Embed(
                title=title,
                description=data.get('description', f'Top performers in {stat_type}'),
                color=EmbedFactory.COLORS['leaderboard'],
                timestamp=datetime.now(ZoneInfo('UTC'))
            )

            if 'rankings' in data:
                embed.add_field(
                    name="🏆 Rankings",
                    value=data['rankings'][:1024],
                    inline=False
                )

            # Add stats summary if available
            if data.get('total_kills') or data.get('total_deaths'):
                stats_text = []
                if data.get('total_kills'):
                    stats_text.append(f"Total Kills: {data['total_kills']:,}")
                if data.get('total_deaths'):
                    stats_text.append(f"Total Deaths: {data['total_deaths']:,}")

                if stats_text:
                    embed.add_field(
                        name="📊 Server Stats",
                        value=" | ".join(stats_text),
                        inline=False
                    )

            # Use dynamic thumbnail and create file attachment
            thumbnail_url = data.get('thumbnail_url') or await EmbedFactory.get_leaderboard_thumbnail(stat_type)
            embed.set_thumbnail(url=thumbnail_url)

            # Create file attachment if thumbnail is an attachment URL
            file_attachment = None
            if thumbnail_url and thumbnail_url.startswith('attachment://'):
                filename = thumbnail_url.replace('attachment://', '')
                file_path = f'assets/{filename}'
                try:
                    file_attachment = discord.File(file_path, filename=filename)
                except FileNotFoundError:
                    pass

            # Set consistent footer branding
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            return embed, file_attachment

        # LOG PARSER EVENT EMBEDS
        elif embed_type == 'player_connection':
            return EmbedFactory._build_player_connection(data)
        elif embed_type == 'player_disconnection':
            return EmbedFactory._build_player_disconnection(data)
        elif embed_type == 'player_join':
            return EmbedFactory._build_player_connection(data)
        elif embed_type == 'player_leave':
            return EmbedFactory._build_player_disconnection(data)
        elif embed_type == 'mission_event':
            return EmbedFactory._build_mission_event(data)
        elif embed_type == 'airdrop_event':
            return EmbedFactory._build_airdrop_event(data)
        elif embed_type == 'helicrash_event':
            return EmbedFactory._build_helicrash_event(data)
        elif embed_type == 'trader_event':
            return EmbedFactory._build_trader_event(data)
        elif embed_type == 'vehicle_event':
            return EmbedFactory._build_vehicle_event(data)
        else:
            raise ValueError(f"Unknown embed type: {embed_type}")

    @classmethod
    def create_embed(
        cls,
        embed_type: str,
        title: str = None,
        description: str = None,
        color: int = None,
        thumbnail: str = None,
        fields: List[Dict[str, Any]] = None,
        footer_text: str = None,
        timestamp: datetime = None,
        randomize_description: bool = True
    ) -> discord.Embed:
        """
        Create a styled embed with consistent theming

        Args:
            embed_type: Type of embed (mission_ready, airdrop, etc.)
            title: Embed title
            description: Embed description (can be randomized)
            color: Custom color (uses type default if None)
            thumbnail: Custom thumbnail (uses type default if None)
            fields: List of fields to add
            footer_text: Custom footer text
            timestamp: Embed timestamp
            randomize_description: Whether to randomize description with military messages

        Returns:
            discord.Embed: Styled embed ready for sending
        """

        # Use type-specific color or provided color
        embed_color = color or cls.COLORS.get(embed_type, cls.COLORS['default'])

        # Create the embed
        embed = discord.Embed(
            title=title or "Server Event",
            description=description,
            color=embed_color,
            timestamp=timestamp or datetime.now(ZoneInfo('UTC'))
        )

        # Add randomized military description if enabled and available
        if randomize_description and embed_type in cls.MILITARY_MESSAGES:
            if not description:
                description = random.choice(cls.MILITARY_MESSAGES[embed_type])
                embed.description = description
            else:
                # Append random military flavor text
                military_flavor = random.choice(cls.MILITARY_MESSAGES[embed_type])
                embed.description = f"{description}\n\n*{military_flavor}*"

        # Add fields if provided
        if fields:
            for field in fields:
                embed.add_field(
                    name=field.get('name', 'Field'),
                    value=field.get('value', 'Value'),
                    inline=field.get('inline', False)
                )

        # Set thumbnail
        thumbnail_file = thumbnail or cls.THUMBNAILS.get(embed_type, cls.THUMBNAILS['default'])
        thumbnail_path = Path(f'./assets/{thumbnail_file}')
        if thumbnail_path.exists():
            embed.set_thumbnail(url=f"attachment://{thumbnail_file}")

        # Set footer
        footer = footer_text or "Powered by Discord.gg/EmeraldServers"
        embed.set_footer(text=footer)

        return embed

    @classmethod
    def create_mission_embed(
        cls,
        mission_name: str,
        state: str = "READY",
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a specialized mission embed"""

        state_icons = {'READY': '🟢', 'WAITING': '🟡', 'INITIAL': '🔴'}
        state_colors = {'READY': 0x00FF00, 'WAITING': 0xFFD700, 'INITIAL': 0xFF4500}

        return cls.create_embed(
            'mission_ready',
            title=f"{state_icons.get(state, '❓')} Mission {state.title()}",
            description=f"**{mission_name}** is now {state.lower()}",
            color=state_colors.get(state, cls.COLORS['mission_ready']),
            fields=[
                {"name": "Mission Status", "value": state, "inline": True},
                {"name": "Zone", "value": mission_name, "inline": True}
            ],
            timestamp=timestamp
        )

    @classmethod
    def create_player_event_embed(
        cls,
        event_type: str,  # 'join' or 'leave'
        connection_info: str,
        ip_address: str = None,
        port: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a specialized player connection embed"""

        if event_type == 'join':
            embed_type = 'player_join'
            # Random title
            title = random.choice(cls.TITLE_POOLS['player_join'])
            description = f"Connection **{connection_info}** joined the server"
            color = cls.COLORS['player_join']
        else:
            embed_type = 'player_leave'
            # Random title
            title = random.choice(cls.TITLE_POOLS['player_leave'])
            description = f"Connection **{connection_info}** left the server"
            color = cls.COLORS['player_leave']

        fields = []
        if ip_address:
            fields.append({"name": "IP Address", "value": ip_address, "inline": True})
        if port:
            fields.append({"name": "Port", "value": str(port), "inline": True})

        embed = cls.create_embed(
            embed_type,
            title=title,
            description=description,
            fields=fields,
            timestamp=timestamp,
            color=color
        )

        return embed

    @classmethod
    def create_airdrop_embed(
        cls,
        state: str = "flying",
        location: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a specialized airdrop embed"""

        fields = [
            {"name": "Status", "value": state.title(), "inline": True},
            {"name": "Alert Level", "value": "HIGH", "inline": True}
        ]

        if location:
            fields.append({"name": "Location", "value": location, "inline": True})

        return cls.create_embed(
            'airdrop',
            title="📦 Supply Drop Inbound",
            fields=fields,
            timestamp=timestamp
        )

    @classmethod
    def create_helicrash_embed(
        cls,
        location: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a specialized helicopter crash embed"""

        fields = [
            {"name": "Status", "value": "Crashed", "inline": True},
            {"name": "Threat Level", "value": "EXTREME", "inline": True}
        ]

        if location:
            fields.append({"name": "Crash Site", "value": location, "inline": True})

        return cls.create_embed(
            'helicrash',
            title="🚁 Helicopter Crash",
            fields=fields,
            timestamp=timestamp
        )

    @classmethod
    def create_vehicle_embed(
        cls,
        action: str,  # 'spawn' or 'delete'
        vehicle_type: str = "Unknown Vehicle",
        current_count: int = None,
        max_count: int = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a specialized vehicle embed"""

        if action == 'spawn':
            title = "🚗 Vehicle Deployed"
            description = f"**{vehicle_type}** has been deployed to the field"
            status = "Deployed"
        else:
            title = "🔧 Vehicle Removed"
            description = f"**{vehicle_type}** has been removed from service"
            status = "Removed"

        fields = [
            {"name": "Vehicle Type", "value": vehicle_type, "inline": True},
            {"name": "Status", "value": status, "inline": True}
        ]

        if current_count is not None and max_count is not None:
            fields.append({"name": "Fleet Status", "value": f"{current_count}/{max_count}", "inline": True})

        return cls.create_embed(
            'vehicle_spawn',
            title=title,
            description=description,
            fields=fields,
            timestamp=timestamp
        )

    @classmethod
    def create_error_embed(
        cls,
        error_message: str,
        details: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a standardized error embed"""

        description = f"❌ {error_message}"
        if details:
            description += f"\n\n**Details:** {details}"

        return cls.create_embed(
            'error',
            title="Error",
            description=description,
            timestamp=timestamp,
            randomize_description=False
        )

    @classmethod
    def create_success_embed(
        cls,
        success_message: str,
        details: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a standardized success embed"""

        description = f"✅ {success_message}"
        if details:
            description += f"\n\n**Details:** {details}"

        return cls.create_embed(
            'success',
            title="Success",
            description=description,
            timestamp=timestamp,
            randomize_description=False
        )

    @classmethod
    def create_info_embed(
        cls,
        info_message: str,
        details: str = None,
        timestamp: datetime = None
    ) -> discord.Embed:
        """Create a standardized info embed"""

        description = f"ℹ️ {info_message}"
        if details:
            description += f"\n\n**Details:** {details}"

        return cls.create_embed(
            'info',
            title="Information",
            description=description,
            timestamp=timestamp,
            randomize_description=False
        )

    @classmethod
    def get_thumbnail_path(cls, embed_type: str) -> Optional[str]:
        """Get the full path to a thumbnail file"""
        thumbnail_file = cls.THUMBNAILS.get(embed_type, cls.THUMBNAILS['default'])
        thumbnail_path = Path(f'./assets/{thumbnail_file}')
        return str(thumbnail_path) if thumbnail_path.exists() else None

    @classmethod
    def _build_killfeed(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build modern killfeed embed - clean aesthetic with themed title and right-aligned logo"""
        # Restore themed titles from the title pool
        title = random.choice(cls.TITLE_POOLS['killfeed']).upper()

        # Create clean embed with themed title
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['killfeed'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Main kill description - clean, bold format
        killer_name = data.get('killer_name', 'Unknown')
        victim_name = data.get('victim_name', 'Unknown')
        killer_kdr = data.get('killer_kdr', '0.00')
        victim_kdr = data.get('victim_kdr', '0.00')

        # Primary kill info in description (like the screenshot)
        kill_text = f"**{killer_name}** (KDR: {killer_kdr})\neliminated\n**{victim_name}** (KDR: {victim_kdr})"
        embed.description = kill_text

        # Weapon and distance info - clean format
        weapon = data.get('weapon', 'Unknown')
        distance = data.get('distance', '0')

        weapon_text = f"**Weapon:** {weapon}\n**From** {distance} Meters"
        embed.add_field(name="", value=weapon_text, inline=False)

        # Combat log message - atmospheric flavor text
        combat_msg = random.choice(cls.COMBAT_LOGS['kill'])
        embed.add_field(name="", value=f"*{combat_msg}*", inline=False)

        # Right-aligned logo as small thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Killfeed.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Server info footer (like in the screenshot)
        timestamp_str = datetime.now().strftime("%m/%d/%Y %I:%M %p")
        embed.set_footer(text=f"Server: Emerald EU | discord.gg/EmeraldServers | {timestamp_str}")

        return embed, file_attachment

    @classmethod
    def _build_suicide(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build suicide embed"""
        title = random.choice(cls.TITLE_POOLS['suicide'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['suicide'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Subject
        faction = f" [{data.get('faction')}]" if data.get('faction') else ""
        embed.add_field(
            name="Subject",
            value=f"{data.get('player_name', 'Unknown')}{faction}",
            inline=True
        )

        # Cause
        cause = data.get('cause', 'Menu Suicide')
        embed.add_field(
            name="Cause",
            value=cause,
            inline=True
        )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['suicide'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://main.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

        return embed, file_attachment

    @classmethod
    def _build_fall(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build fall damage embed"""
        title = random.choice(cls.TITLE_POOLS['fall'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['fall'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Subject
        faction = f" [{data.get('faction')}]" if data.get('faction') else ""
        embed.add_field(
            name="Subject",
            value=f"{data.get('player_name', 'Unknown')}{faction}",
            inline=True
        )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['fall'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://main.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

        return embed, file_attachment

    @classmethod
    def _build_slots(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build slots gambling embed"""
        embed = discord.Embed(
            title="🎰 Wasteland Slots",
            color=cls.COLORS['slots'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Handle slot display
        if data.get('slot_display'):
            embed.add_field(
                name="Reels",
                value=data['slot_display'],
                inline=False
            )

        # Handle status
        if data.get('status'):
            embed.add_field(
                name="Status",
                value=data['status'],
                inline=False
            )

        # Handle bet amount
        if data.get('bet_amount'):
            embed.add_field(
                name="Bet",
                value=f"${data['bet_amount']:,}",
                inline=True
            )

        # Handle winnings
        if data.get('winnings'):
            embed.add_field(
                name="Winnings",
                value=f"+${data['winnings']:,}",
                inline=True
            )

        # Handle net result
        if data.get('net_result') is not None:
            if data['net_result'] > 0:
                embed.add_field(
                    name="Net Result",
                    value=f"+${data['net_result']:,}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="Net Result",
                    value=f"-${abs(data['net_result']):,}",
                    inline=True
                )

        # Handle new balance
        if data.get('new_balance') is not None:
            embed.add_field(
                name="New Balance",
                value=f"${data['new_balance']:,}",
                inline=True
            )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['gambling'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Gamble.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

        return embed, file_attachment

    @classmethod
    def _build_roulette(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build roulette gambling embed"""
        embed = discord.Embed(
            title="🎯 Deadside Roulette",
            color=cls.COLORS['roulette'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Handle status
        if data.get('status'):
            embed.add_field(
                name="Status",
                value=data['status'],
                inline=False
            )

        # Handle player choice
        if data.get('player_choice'):
            embed.add_field(
                name="Player Pick",
                value=data['player_choice'],
                inline=True
            )

        # Handle result
        if data.get('result'):
            embed.add_field(
                name="Spin Result",
                value=data['result'],
                inline=True
            )

        # Handle bet amount
        if data.get('bet_amount'):
            embed.add_field(
                name="Bet",
                value=f"${data['bet_amount']:,}",
                inline=True
            )

        # Handle winnings
        if data.get('winnings'):
            embed.add_field(
                name="Winnings",
                value=f"+${data['winnings']:,}",
                inline=True
            )

        # Handle net result
        if data.get('net_result') is not None:
            if data['net_result'] > 0:
                embed.add_field(
                    name="Net Result",
                    value=f"+${data['net_result']:,}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="Net Result",
                    value=f"-${abs(data['net_result']):,}",
                    inline=True
                )

        # Handle new balance
        if data.get('new_balance') is not None:
            embed.add_field(
                name="New Balance",
                value=f"${data['new_balance']:,}",
                inline=True
            )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['gambling'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Gamble.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_blackjack(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build blackjack gambling embed"""
        embed = discord.Embed(
            title="🃏 Deadside Blackjack",
            color=cls.COLORS['blackjack'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Handle status
        if data.get('status'):
            embed.add_field(
                name="Status",
                value=data['status'],
                inline=False
            )

        # Handle player hand
        if data.get('player_hand'):
            embed.add_field(
                name="Your Hand",
                value=data['player_hand'],
                inline=True
            )

        # Handle dealer hand
        if data.get('dealer_hand'):
            embed.add_field(
                name="Dealer Hand",
                value=data['dealer_hand'],
                inline=True
            )

        # Handle bet amount
        if data.get('bet_amount'):
            embed.add_field(
                name="Bet",
                value=f"${data['bet_amount']:,}",
                inline=True
            )

        # Handle winnings
        if data.get('winnings'):
            embed.add_field(
                name="Winnings",
                value=f"+${data['winnings']:,}",
                inline=True
            )

        # Handle net result
        if data.get('net_result') is not None:
            if data['net_result'] > 0:
                embed.add_field(
                    name="Net Result",
                    value=f"+${data['net_result']:,}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="Net Result",
                    value=f"-${abs(data['net_result']):,}",
                    inline=True
                )

        # Handle new balance
        if data.get('new_balance') is not None:
            embed.add_field(
                name="New Balance",
                value=f"${data['new_balance']:,}",
                inline=True
            )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['gambling'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Gamble.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_profile(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build profile embed"""
        embed = discord.Embed(
            title="👤 Player Profile",
            color=cls.COLORS['profile'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Player name
        if data.get('player_name'):
            embed.add_field(
                name="Name",
                value=data['player_name'],
                inline=True
            )

        # Faction
        if data.get('faction'):
            embed.add_field(
                name="Faction",
                value=data['faction'],
                inline=True
            )

        # Kills
        if data.get('kills') is not None:
            embed.add_field(
                name="Kills",
                value=f"{data['kills']:,}",
                inline=True
            )

        # Deaths
        if data.get('deaths') is not None:
            embed.add_field(
                name="Deaths",
                value=f"{data['deaths']:,}",
                inline=True
            )

        # KDR
        if data.get('kdr') is not None:
            embed.add_field(
                name="KDR",
                value=data['kdr'],
                inline=True
            )

        # Distance
        if data.get('distance') is not None:
            embed.add_field(
                name="Distance",
                value=f"{data['distance']:,} m",
                inline=True
            )

        # Playtime
        if data.get('playtime'):
            embed.add_field(
                name="Playtime",
                value=data['playtime'],
                inline=True
            )

        # Bounty
        if data.get('bounty') is not None:
            embed.add_field(
                name="Bounty",
                value=f"${data['bounty']:,}",
                inline=True
            )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://main.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_bounty(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build bounty embed"""
        title = random.choice(cls.TITLE_POOLS['bounty'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['bounty'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Target
        faction = f" [{data.get('faction')}]" if data.get('faction') else ""
        embed.add_field(
            name="Target",
            value=f"{data.get('player_name', 'Unknown')}{faction}",
            inline=True
        )

        # Amount
        amount = data.get('amount', 'Unknown')
        embed.add_field(
            name="Amount",
            value=f"${amount:,}",
            inline=True
        )

        # Combat log
        combat_log = random.choice(cls.COMBAT_LOGS['bounty'])
        embed.add_field(
            name="Combat Log",
            value=combat_log,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Bounty.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_admin(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build admin command embed"""
        embed = discord.Embed(
            title="⚙️ Admin Command",
            color=cls.COLORS['admin'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Admin
        admin = data.get('admin', 'Unknown')
        embed.add_field(
            name="Admin",
            value=admin,
            inline=True
        )

        # Command
        command = data.get('command', 'Unknown')
        embed.add_field(
            name="Command",
            value=command,
            inline=True
        )

        # Target
        target = data.get('target', 'Unknown')
        embed.add_field(
            name="Target",
            value=target,
            inline=True
        )

        # Details
        details = data.get('details', 'None')
        embed.add_field(
            name="Details",
            value=details,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://main.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_comparison(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build comparison embed"""
        embed = discord.Embed(
            title="📊 Stat Comparison",
            color=cls.COLORS['leaderboard'],
            timestamp=datetime.now(ZoneInfo('UTC'))
        )

        # Player 1
        player1 = data.get('player1', 'Unknown')
        embed.add_field(
            name="Player 1",
            value=player1,
            inline=True
        )

        # Player 2
        player2 = data.get('player2', 'Unknown')
        embed.add_field(
            name="Player 2",
            value=player2,
            inline=True
        )

        # Stat
        stat = data.get('stat', 'Kills')
        embed.add_field(
            name="Stat",
            value=stat,
            inline=True
        )

        # Value 1
        value1 = data.get('value1', 'Unknown')
        embed.add_field(
            name="Value 1",
            value=value1,
            inline=True
        )

        # Value 2
        value2 = data.get('value2', 'Unknown')
        embed.add_field(
            name="Value 2",
            value=value2,
            inline=True
        )

        # Winner
        winner = data.get('winner', 'Unknown')
        embed.add_field(
            name="Winner",
            value=winner,
            inline=True
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Leaderboard.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        # Footer
        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_player_connection(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build player connection embed"""
        title = random.choice(cls.TITLE_POOLS['player_join'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['player_join'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        connection_id = data.get('connection_id', 'Unknown')
        embed.add_field(
            name="Connection",
            value=connection_id,
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['player_join'])
        embed.add_field(
            name="Status",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Connections.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_player_disconnection(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build player disconnection embed"""
        title = random.choice(cls.TITLE_POOLS['player_leave'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['player_leave'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        connection_id = data.get('connection_id', 'Unknown')
        embed.add_field(
            name="Connection",
            value=connection_id,
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['player_leave'])
        embed.add_field(
            name="Status",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Connections.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_mission_event(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build mission event embed"""
        title = random.choice(cls.TITLE_POOLS['mission_ready'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['mission_ready'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        mission_name = data.get('mission_name', 'Unknown Mission')
        state = data.get('state', 'READY')

        embed.add_field(
            name="Mission Zone",
            value=mission_name,
            inline=True
        )

        embed.add_field(
            name="Status",
            value=state.title(),
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['mission_ready'])
        embed.add_field(
            name="Tactical Update",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Mission.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_airdrop_event(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build airdrop event embed"""
        title = random.choice(cls.TITLE_POOLS['airdrop'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['airdrop'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        embed.add_field(
            name="Status",
            value="Incoming",
            inline=True
        )

        embed.add_field(
            name="Priority",
            value="HIGH",
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['airdrop'])
        embed.add_field(
            name="Intelligence Report",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Airdrop.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_helicrash_event(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build helicopter crash event embed"""
        title = random.choice(cls.TITLE_POOLS['helicrash'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['helicrash'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        location = data.get('location', 'Unknown')
        embed.add_field(
            name="Crash Site",
            value=location,
            inline=True
        )

        embed.add_field(
            name="Threat Level",
            value="EXTREME",
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['helicrash'])
        embed.add_field(
            name="Operational Alert",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Helicrash.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod
    def _build_trader_event(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build trader event embed"""
        title = random.choice(cls.TITLE_POOLS['trader'])
        embed = discord.Embed(
            title=title,
            color=cls.COLORS['trader'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        location = data.get('location', 'Unknown')
        embed.add_field(
            name="Trading Post",
            value=location,
            inline=True
        )

        embed.add_field(
            name="Status",
            value="Open",
            inline=True
        )

        # Tactical message
        tactical_msg = random.choice(cls.TACTICAL_MESSAGES['trader'])
        embed.add_field(
            name="Market Update",
            value=tactical_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Trader.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment

    @classmethod  
    def _build_vehicle_event(cls, data: Dict[str, Any]) -> tuple[discord.Embed, Optional[discord.File]]:
        """Build vehicle event embed"""
        action = data.get('action', 'spawn')
        title = random.choice(cls.TITLE_POOLS['vehicle_spawn'])

        embed = discord.Embed(
            title=title,
            color=cls.COLORS['vehicle_spawn'],
            timestamp=data.get('timestamp') or datetime.now(ZoneInfo('UTC'))
        )

        vehicle_type = data.get('vehicle_type', 'Military Vehicle')
        embed.add_field(
            name="Vehicle Type",
            value=vehicle_type,
            inline=True
        )

        status = "Deployed" if action == 'spawn' else "Removed"
        embed.add_field(
            name="Status",
            value=status,
            inline=True
        )

        # Military message
        military_msg = random.choice(cls.MILITARY_MESSAGES['vehicle_spawn'])
        embed.add_field(
            name="Logistics Update",
            value=military_msg,
            inline=False
        )

        # Thumbnail
        thumbnail_url = data.get('thumbnail_url', 'attachment://Vehicle.png')
        embed.set_thumbnail(url=thumbnail_url)

        # Create file attachment
        file_attachment = None
        if thumbnail_url and thumbnail_url.startswith('attachment://'):
            filename = thumbnail_url.replace('attachment://', '')
            file_path = f'assets/{filename}'
            try:
                file_attachment = discord.File(file_path, filename=filename)
            except FileNotFoundError:
                pass

        embed.set_footer(text="Powered by Discord.gg/EmeraldServers")
        return embed, file_attachment