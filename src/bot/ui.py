#==== ui.py ====

from telethon import Button
from .models import BotUser

def get_main_settings_menu(user: BotUser):
    """Generates a rich, instructive main settings menu."""
    
    # --- Source Display ---
    source_display = "🚫 Not Set"
    if user.source_channel_title:
        source_display = f"✅ {user.source_channel_title}"
    elif user.source_channel_id:
        source_display = f"✅ `ID: {user.source_channel_id}`"

    # --- DEFINITIVE: Destination Display Logic ---
    dest_display = "🚫 Not Set"
    if user.upload_to_gdrive:
        dest_display = "✅ ☁️ Google Drive"
    elif user.upload_to_koofr: 
        dest_display = "✅ 🗄️ Koofr"
    elif user.target_channel_id is None:
        dest_display = "✅ 💬 Bot PM (Default)"
    elif user.target_channel_id == 777:
        dest_display = "✅ 📂 Saved Messages"
    else:
        dest_display = f"✅ 📢 {user.target_channel_title or f'ID: {user.target_channel_id}'}"

    # --- Range Display ---
    range_display = "🚫 Not Set"
    if user.message_range:
        count = abs(user.message_range[1] - user.message_range[0]) + 1
        range_display = f"✅ `{user.message_range[0]}` → `{user.message_range[1]}` ({count:,} msgs)"
        
    config_complete = all([user.source_channel_id, user.message_range])
    config_status = "✅ Ready to Queue" if config_complete else "⚠️ Incomplete"

    text = (
        f"**⚙️ Bot Configuration Panel**\n"
        f"Status: `{config_status}`\n\n"
        f"**__Current Transfer Job__**\n"
        f"**Source:** {source_display}\n"
        f"**Destination:** {dest_display}\n"
        f"**Range:** {range_display}\n\n"
        f"➡️ Select a category below to modify settings."
    )
    
    buttons = [
        [Button.inline("🎯 Transfer Setup", b'menu_transfer'), Button.inline("✍️ Naming & Text", b'menu_naming')],
        [Button.inline("🖼️ Media & Thumbnail", b'menu_media'), Button.inline("📦 Zip/Unzip", b'menu_zip')],
        [Button.inline("⚡ Performance", b'menu_advanced'), Button.inline("🔧 Tools", b'menu_tools')]
    ]
    return text, buttons

def get_submenu(user: BotUser, menu_key: str):
    """Generates instructive submenus for each settings category."""
    back_button = [Button.inline("⬅️ Back to Settings", b'back_to_main_menu')]

    if menu_key == 'menu_transfer':
        is_ready = all([user.source_channel_id, user.message_range])
        text = (
            "**🎯 Transfer Setup**\n\n"
            "This is where you define what to transfer and where it should go.\n\n"
            "1️⃣ **Set Range / Source:** Provide 1 or 2 message links to automatically set the source channel and the message range.\n\n"
            "2️⃣ **Set Destination:** Choose where the files will be sent."
        )
        buttons = [
            [Button.inline("📊 Set Range / Source", b'set_range')],
            [Button.inline("📤 Set Destination", b'set_target')]
        ]
        if is_ready:
            buttons.insert(0, [Button.inline("➕ Queue This Job", b'queue_batch'), Button.inline("▶️ Start Queue", b'start_queue')])
        buttons.append(back_button)
        return text, buttons

    elif menu_key == 'menu_naming':
        example_filename = "Sample Video.mp4"
        if user.rename_prefix or user.rename_suffix:
            example_result = f"{user.rename_prefix}{example_filename.replace('.mp4', '')}{user.rename_suffix}.mp4"
            example_display = f"\n**Example:** `{example_filename}` → `{example_result}`"
        else:
            example_display = f"\n**Example:** `{example_filename}` → `{example_filename}` (unchanged)"

        prefix_status = f"✅ `{user.rename_prefix}`" if user.rename_prefix else "🚫 None"
        suffix_status = f"✅ `{user.rename_suffix}`" if user.rename_suffix else "🚫 None"
        caption_status = "✅ Custom Template" if user.custom_caption else "🚫 Use Original"

        fname_rules_count = len(user.filename_replace_rules.split('|')) if user.filename_replace_rules else 0
        caption_rules_count = len(user.caption_replace_rules.split('|')) if user.caption_replace_rules else 0

        text = (
            f"📝 **Naming & Text Customization**\n\n"
            f"**📁 Filename Modification:**\n"
            f"├ ➕ Prefix: {prefix_status}\n"
            f"├ ➕ Suffix: {suffix_status}\n"
            f"└ 🔄 Rules: {fname_rules_count} active{example_display}\n\n"
            f"**💬 Caption Settings:**\n"
            f"├ 📝 Template: {caption_status}\n"
            f"└ 🔄 Rules: {caption_rules_count} active\n\n"
            f"*Use rules format: `old:new|remove_this|regex:pattern:replacement`*"
        )

        return (text, [
            [Button.inline("➕ Prefix", b'set_prefix'), Button.inline("➕ Suffix", b'set_suffix')], 
            [Button.inline("🔄 Filename Rules", b'set_filename_replace')],
            [Button.inline("💬 Caption Template", b'set_caption')],
            [Button.inline("🔄 Caption Rules", b'set_caption_replace')],
            back_button
        ])
                
    elif menu_key == 'menu_media':
        thumb_status = "✅ Set" if user.thumbnail_url else "🚫 None"
        thumb_all_status = "✅ All Files" if user.thumbnail_for_all else "📹 Videos Only"
        video_mode = "📺 Stream" if user.stream_videos else "📁 Document"
        auto_thumb_status = f"✅ @ {user.thumbnail_timestamp}s" if user.auto_thumbnail else "🚫 Disabled"
        
        text = (
            f"**🖼️ Media & Thumbnail Settings**\n\n"
            f"**Manual Thumbnail:** {thumb_status}\n"
            f"**Auto-Generate:** {auto_thumb_status}\n\n" # <-- NEW
            f"**Apply To:** {thumb_all_status}\n"
            f"**Video Upload Mode:** {video_mode}"
        )
        buttons = [
            [Button.inline("🖼️ Set Manual Thumbnail", b'set_thumbnail'), Button.inline("🗑️ Clear Manual", b'clear_thumbnail')],
            [Button.inline(f"Toggle Auto-Generate", b'toggle_auto_thumb')], # <-- NEW
            [Button.inline(f"Set Auto-Generate Time", b'set_thumb_time')], # <-- NEW
            [Button.inline(f"Toggle Apply to All", b'toggle_thumb_all'), Button.inline(f"Toggle Video Mode", b'toggle_stream')],
            back_button
        ]
        return text, buttons
        
    elif menu_key == 'menu_advanced':
        text = (
            f"⚡ **Performance Settings**\n\n"
            f"**🚀 Worker Threads:**\n`{user.worker_limit}` (Applies to downloads and uploads)"
        )
        buttons = [[Button.inline("🚀 Set Workers", b'set_workers')], back_button]
        return text, buttons

    elif menu_key == 'menu_tools':
        text = (
            f"🔧 **Configuration Tools**\n\n"
            f"**📤 Backup & Restore:**\n"
            f"Save your settings to avoid reconfiguration.\n\n"
            f"**🔄 Reset Options:**\n"
            f"Start fresh with default settings.\n\n"
            f"**📊 Quick Actions:**\n"
            f"Test your setup before queuing transfers."
        )

        return (text, [
            [Button.inline("💾 Export Config", b'export_config')],
            [Button.inline("📂 Import Config", b'import_config')], 
            [Button.inline("♻️ Reset All", b'reset_all')],
            [Button.inline("🧪 Test Setup", b'test_config')],
            back_button
        ])
  
    elif menu_key == 'menu_zip':
        if not user.allow_zip and user.role != 'Owner':
            return "❌ This is a premium feature.", [back_button]
            
        zip_status = "✅ Enabled" if user.zip_files else "🚫 Disabled"
        unzip_status = "✅ Enabled" if user.unzip_files else "🚫 Disabled"
        pass_status = "✅ Set" if user.zip_password else "⚠️ Not Set"

        text = (
            f"**📦 Archive Settings**\n\n"
            f"**Zipping:** {zip_status}\n"
            f"**Unzipping:** {unzip_status}\n"
            f"**Password:** {pass_status}"
        )

        buttons = [
            [Button.inline(f"Toggle Zipping ({zip_status})", b"toggle_zip")],
            [Button.inline(f"Toggle Unzipping ({unzip_status})", b"toggle_unzip")]
        ]

        if user.zip_files or user.unzip_files:
            buttons.append([Button.inline("🔑 Set Password", b"set_zip_password")])

        buttons.append(back_button)
        return text, buttons

    return None, None

def get_manage_menu():
    """Builds the text and buttons for the /manage command menu."""
    text = (
        "**⚙️ Job & Data Management**\n\n"
        "Use the buttons below to view or manage your pending jobs and failed transfers."
    )
    buttons = [
        [Button.inline("📑 View Queue", b'manage_view_queue'), Button.inline("🗑️ Clear Queue", b'manage_clear_queue')],
        [Button.inline("📋 View Failed", b'manage_view_failed'), Button.inline("♻️ Retry All Failed", b'manage_retry_failed')],
        [Button.inline("🧹 Clear Failed Log", b'manage_clear_failed_log')] # <-- NEW
    ]
    return text, buttons

def get_admin_menu():
    """Builds the main admin menu, now including the Owner Dashboard."""
    text = "👑 **Admin & Owner Panel**\n\nSelect a category to manage."
    buttons = [
        [Button.inline("👤 User Management", b"admin_menu:users")],
        [Button.inline("🚫 Content Moderation", b"admin_menu:content")],
        [Button.inline("🔬 Bot Diagnostics", b"admin_menu:diagnostics")],
        [Button.inline("📣 Bot Actions", b"admin_menu:actions")],
        # --- NEW: Dashboard Button ---
        [Button.inline("👑 Owner Dashboard", b"admin:dashboard")],
        [Button.inline("🔌 Shutdown Bot", b"admin:shutdown")]
    ]
    return text, buttons


def get_admin_submenu(menu_key: str):
    """Builds the various submenus for the admin panel with an improved layout."""
    back_button = [Button.inline("⬅️ Back to Admin Menu", b"admin_menu:main")]
    text, buttons = None, None

    if menu_key == 'users':
        text = "**👤 User Management**"
        buttons = [
            # Grouping related actions
            [Button.inline("➕ Add Sub", b"admin:add_sub"), Button.inline("➖ Remove User", b"admin:remove_user")],
            [Button.inline("🚫 Ban User", b"admin:ban"), Button.inline("✅ Unban User", b"admin:unban")],
            [Button.inline("✨ Add Admin", b"admin:add_admin"), Button.inline("❌ Remove Admin", b"admin:remove_admin")],
            [Button.inline("💰 Set Credits", b"admin:set_credits"), Button.inline("🌍 Set Universal Limit", b"admin:set_universal_credit")],
            [Button.inline("👥 List Subscribers", b"admin:list_subs")],
            back_button
        ]

    elif menu_key == 'content':
        text = "**🚫 Content Moderation**"
        buttons = [
            [Button.inline("➕ Add Blacklist", b"admin:add_blacklist"), Button.inline("➖ Remove Blacklist", b"admin:remove_blacklist")],
            [Button.inline("📖 View Blacklist", b"admin:view_blacklist")],
            back_button
        ]

    elif menu_key == 'diagnostics':
        text = "**🔬 Bot Diagnostics**"
        buttons = [
            [Button.inline("🧪 Test Setup", b"admin:test_config"), Button.inline("📊 DB Stats", b"admin:db_stats")],
            [Button.inline("📈 All Analytics", b"admin:all_analytics"), Button.inline("🔎 Run SQL", b"admin:sql_query")],
            back_button
        ]
    
    elif menu_key == 'actions':
        text = "**📣 Bot Actions**"
        buttons = [
            [Button.inline("📢 Broadcast Message", b"admin:broadcast")],
            back_button
        ]
        
    return text, buttons


def get_dashboard_menu():
    """Builds the main owner dashboard menu."""
    text = "**👑 Owner Dashboard**\n\nSelect a report to view."
    buttons = [
        [Button.inline("📈 Overview", b"dashboard:overview")],
        [Button.inline("📊 Plan Breakdown", b"dashboard:plans")],
        [Button.inline("⚠️ System Health", b"dashboard:health")]
    ]
    return text, buttons
