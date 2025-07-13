#==== settings_handler.py ====

import json
import os
import re
import time
import select
import urllib.parse
from datetime import datetime, timedelta
from io import BytesIO

import httpx

from sqlalchemy import func

from sqlmodel import Session, create_engine, delete, func, select

from telethon import Button

from telethon.errors import MessageNotModifiedError

from .. import config

from ..models import BotUser, Job, JobAnalytics

from .decorators import admin_only, subscriber_only, check_banned, owner_only

from ..database import (
    FailedItem,
    get_blacklist,
    get_or_create_user,
    get_session,
    get_user_role,
)

from ..utils import (
    check_channel_membership,
    format_duration,
    get_job_preview,
    get_user_client,
    humanbytes,
    is_blacklisted,
    parse_telegram_message_link,
)

from .admin_commands import (
    PLAN_SPECS,
    add_blacklist_handler,
    add_subscriber_handler,
    all_analytics_handler,
    ban_handler,
    broadcast_handler,
    db_stats_handler,
    remove_blacklist_handler,
    remove_user_handler,
    shutdown_handler,
    sql_query_handler,
    subscribers_handler,
    test_handler,
    unban_handler,
    view_blacklist_handler,
    dashboard_handler,
)

from .user_commands import (
    clear_queue_handler,
    clone_job_handler,
    failed_log_handler,
    gdrive_login_handler,
    gdrive_logout_handler,
    koofr_login_handler,
    koofr_logout_handler,
    login_handler,
    logout_handler,
    myplan_handler,
    pause_resume_job_handler,
    queue_batch_handler,
    retry_handler,
    start_queue_handler,
    view_queue_handler,
)

from ..ui import (
    get_admin_menu,
    get_admin_submenu,
    get_dashboard_menu,
    get_main_settings_menu,
    get_manage_menu,
    get_submenu,
    
)




# Standard library imports
import json
import os
import re
import urllib.parse
from io import BytesIO

# Third-party imports
from sqlmodel import delete, func, select
from telethon import Button
from telethon.errors import MessageNotModifiedError

# Local application imports
from .. import config
from ..database import get_or_create_user, get_session
from ..models import BotUser, Job, JobAnalytics
from ..ui import (get_admin_menu, get_admin_submenu, get_dashboard_menu,
                  get_main_settings_menu, get_manage_menu)
from ..utils import (check_channel_membership, format_duration, get_job_preview,
                     get_user_client, humanbytes,
                     parse_telegram_message_link, send_traceable_edit,
                     send_traceable_reply)

# Handlers from other modules called by buttons
from .admin_commands import (PLAN_SPECS, all_analytics_handler, db_stats_handler,
                             shutdown_handler,
                             subscribers_handler, test_handler,
                             view_blacklist_handler)
from .user_commands import (gdrive_login_handler, gdrive_logout_handler,
                            koofr_login_handler, koofr_logout_handler,
                            login_handler, logout_handler, myplan_handler, queue_batch_handler)




# In bot/handlers/settings_handler.py

# ... (other imports)
from ..utils import (check_channel_membership, format_duration, get_job_preview,
                     get_user_client, humanbytes, parse_telegram_message_link,
                     send_traceable_reply, send_traceable_edit) # <-- ADD send_traceable_reply and send_traceable_edit HERE
# ... (other imports)


# In bot/handlers/settings_handler.py

from ..database import (get_or_create_user, get_session, get_overview_stats, 
                       get_plan_breakdown, get_recent_errors)


# ==============================================================================
# MAIN HANDLER & DISPATCHER
# ==============================================================================

async def settings_handler(event):
    """Displays the user's settings menu, with a preview from the thumbnail_url."""
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        text, buttons = get_main_settings_menu(user)
        thumb_url = user.thumbnail_url

    if thumb_url:
        try:
            await event.client.send_file(
                event.chat_id, file=thumb_url, caption=f"📸 **Current Thumbnail Preview**\n\n{text}", 
                buttons=buttons, parse_mode='md'
            )
        except Exception as e:
            config.logger.error(f"Failed to send thumbnail preview from URL for user {user_id}: {e}")
            await event.reply(f"⚠️ Could not load thumbnail preview.\n\n{text}", buttons=buttons, parse_mode='md')
    else:
        await event.client.send_message(event.chat_id, text, buttons=buttons, parse_mode='md')

async def response_handler(event):
    """
    The single, unified handler for all stateful replies, with corrected persistence logic.
    """
    user_id = event.sender_id
    state = config.CONVERSATION_STATE.get(user_id)
    
    # --- NEW: Diagnostic Logging ---
    # This will log every message that is considered a reply.
    config.logger.info(
        f"Response handler triggered for user {user_id}. "
        f"State: {state}. Message: '{event.raw_text}'"
    )

    if not state or 'step' not in state:
        # This path handles direct link submissions for premium users
        if event.text and ('t.me/c/' in event.text or '/t.me/' in event.text):
            role = get_user_role(user_id)
            if role in ['Owner', 'Admin', 'Subscriber']:
                return await _handle_direct_link_transfer(event, user_id)
        return # Ignore other non-conversation messages

    step = state['step']
    config.logger.info(f"Response handler triggered for user {user_id}. Step: '{step}'.")

    try:
        # --- Koofr Login Conversation ---
        if step == 'koofr_email':
            state['email'] = event.text.strip()
            state['step'] = 'koofr_password'
            # Do NOT delete conversation state here
            return await event.reply(
                "✅ Email received. Now, please generate and send an **App Password** from your Koofr account settings."
                "\n\n*Do not use your main password.*"
            )
        elif step == 'koofr_password':
            email = state.get('email')
            app_password = event.text.strip()
            if not email:
                raise ValueError("Koofr email was missing from conversation state.")

            with get_session() as session:
                user = get_or_create_user(session, user_id)
                user.koofr_email = email
                user.koofr_app_password = app_password
                session.add(user)
                session.commit() # <-- This is the crucial commit
            
            # Now that it's saved, clean up and confirm.
            del config.CONVERSATION_STATE[user_id]
            return await event.reply("✅ **Koofr account connected successfully!**")

        # --- Google Drive Login Conversation ---
        elif step == 'gdrive_code':
            return await _handle_gdrive_code(event, state, user_id)
     
        # --- Admin and User Settings Conversations ---
        elif step.startswith('admin_'):
            await _handle_admin_replies(event, step, event.text.strip())
        else:
            with get_session() as session:
                user = get_or_create_user(session, user_id)
                value = event.text.strip() if event.text else None
                
                if step in ['set_source', 'set_target']:
                    await _handle_set_channel(event, user, user_id, step, value)
                elif step == 'set_range':
                    await _handle_set_range(event, user, user_id, value)

                elif step in ['set_prefix', 'set_suffix', 'set_caption', 'set_filename_replace', 'set_caption_replace']:
                    await _handle_set_naming(event, user, value, step, session)

                elif step == 'set_workers':
                    await _handle_set_workers(event, user, value)
                
                elif step == 'set_zip_password':
                   if not user.allow_zip and user.role != 'Owner':
                       return await event.reply("❌ This is a premium feature.")
                   await _handle_set_zip_password(event, user, value)

                elif step == 'set_thumbnail':
                    await _handle_set_thumbnail(event, user, user_id)

                elif step == 'import_config':
                    await _handle_import_config(event, user, user_id)

                session.add(user)
                session.commit()
            await settings_handler(event) # Refresh menu

        # Clean up conversation state after a successful step
        if user_id in config.CONVERSATION_STATE:
            del config.CONVERSATION_STATE[user_id]

    except Exception as e:
        config.logger.exception(f"FATAL in response_handler for user {user_id}: {e}")
        if user_id in config.CONVERSATION_STATE:
            del config.CONVERSATION_STATE[user_id]
        await event.reply("❌ An unexpected error occurred. The operation has been cancelled.")

# In bot/handlers/settings_handler.py
from .decorators import log_callback #<-- Add this import


@log_callback
async def callback_query_handler(event):
    """The final, definitive dispatcher for all button presses."""
    user_id = event.sender_id
    data = event.data.decode('utf-8')

    state = config.CONVERSATION_STATE.get(user_id)
    allowed_steps = ['direct_action_confirm', 'confirm_queue_action']
    if state and state.get('step') not in allowed_steps:
        del config.CONVERSATION_STATE[user_id]
        return await event.answer("✅ Prompt cancelled.")

    try:
        config.logger.info(f"Callback triggered for user {user_id}. Data: '{data}'")

        # --- Main Dispatcher ---

        # 1. Handle Direct Link actions first, as it's a special case
        if data.startswith('direct_'):
            action = data.split('_', 1)[1]
            if action == 'cancel':
                config.CONVERSATION_STATE.pop(user_id, None)
                return await send_traceable_edit(event, "❌ Action cancelled.")
            is_start = (action == 'start')
            return await _handle_direct_action(event, user_id, start_immediately=is_start)
        
        # --- REFACTORED LOGIC ---
        # After creating the job and getting the job_id:
            job.status = 'in_queue'
            session.add(job)
            session.commit()
        
        # Push the new job's ID to the Redis queue
            bot.config.redis_client.lpush('job_queue', job.id)
        
            await event.edit(
            "✅ **Job has been sent to the processing queue!**\n\n"
            "The Muscle workers will pick it up shortly. You can monitor progress with `/status`."
        )
            return # End the function here


        # 2. Handle all other pattern-based routes
        elif data.startswith('menu_') or data == 'back_to_main_menu':
            return await _handle_menu_navigation(event, data, user_id)
        elif data.startswith('admin_menu:'):
            return await _handle_admin_menu_nav(event, data.split(':', 1)[1])
        elif data.startswith('admin:'):
            return await _handle_admin_actions(event, data.split(':', 1)[1])
        elif data.startswith('manage_'):
            return await _handle_manage_action(event, data.split('_', 1)[1])
        elif data.startswith('toggle_'):
            return await _handle_toggle_setting(event, data, user_id)
        # --- CORRECTED ROUTING ---
        elif data.startswith('dest_') and data != 'dest_set_channel':
             return await _handle_destination_setting(event, data, user_id)
        elif data.startswith('select_plan:'):
            return await _handle_select_plan(event, data)
        elif data.startswith('dashboard:'):
            return await _handle_dashboard_actions(event, data.split(':', 1)[1])
        elif data.startswith('q_bump:'):
            return await _handle_job_priority_bump(event, data, user_id)

        # 3. Handle all static (exact match) routes using a dictionary
        handlers = {
            'set_target': lambda: _handle_set_target_menu(event),
            'dest_set_channel': lambda: _handle_set_channel_prompt(event),
            'queue_batch': lambda: queue_batch_handler(event),
            'start_queue': lambda: start_queue_handler(event),
            'confirm_queue_yes': lambda: _handle_queue_confirmation(event, user_id),
            'view_plans': lambda: _handle_view_plans(event, user_id),
            'get_payment_qr': lambda: _handle_get_payment_qr(event),
            'paid_notify': lambda: _handle_paid_notify(event),
            'login_tg': lambda: login_handler(event),
            'logout_tg': lambda: logout_handler(event),
            'login_gdrive': lambda: gdrive_login_handler(event),
            'logout_gdrive': lambda: gdrive_logout_handler(event),
            'login_koofr': lambda: koofr_login_handler(event),
            'logout_koofr': lambda: koofr_logout_handler(event),
            'clear_thumbnail': lambda: _handle_clear_thumbnail(event, user_id),
            'reset_all': lambda: _handle_reset_all(event),
            'export_config': lambda: _handle_export_config(event, user_id),
            'check_join': lambda: _handle_check_join(event),
            'close_menu': lambda: event.delete(),
        }

        if data in handlers:
            return await handlers[data]()

        # 4. Fallback for any other unhandled button press
        return await _handle_prompt_setup(event, data, user_id)

    except MessageNotModifiedError:
        pass
    except Exception as e:
        config.logger.exception(f"Callback handler error for user {user_id} on data '{data}': {e}")
        await event.answer("❌ An error occurred.", alert=True)



# ==============================================================================
# HELPER FUNCTIONS (These should all be present in the file)
# ==============================================================================

async def _handle_queue_confirmation(event, user_id):
    """
    Creates and queues a job AFTER the user has confirmed via a button press.
    """
    if config.CONVERSATION_STATE.get(user_id, {}).get('step') != 'confirm_queue_action':
        return await event.answer("⚠️ This confirmation has expired.", alert=True)
    
    del config.CONVERSATION_STATE[user_id]

    with get_session() as session:
        user = get_or_create_user(session, user_id)
        
        queued_count = session.exec(select(func.count(Job.id)).where(Job.user_id == user_id, Job.status == "queued")).one()
        if queued_count >= user.max_queue_limit and user.role != 'Owner':
            return await event.edit(f"❌ **Queue Full!**")

        # --- Create the Job ---
        job_data = user.model_dump(
            exclude={'id', 'role', 'plan', 'expiry_date', 'last_login', 'ban_expiry_date', 'ban_reason', 'gdrive_creds_json', 'koofr_email', 'koofr_app_password'}
        )
        
        # --- DEFINITIVE FIX: Add the missing user_id ---
        job_data['user_id'] = user_id
        
        job_data.update({
            'download_workers': user.worker_limit,
            'upload_workers': user.worker_limit,
        })
        
        job_to_queue = Job.model_validate(job_data)
        session.add(job_to_queue)
        session.commit()
        session.refresh(job_to_queue)

        await event.edit(f"✅ **Batch Queued!** (Job ID: #{job_to_queue.id})")

async def _handle_admin_replies(event, step, value):
    """
    Processes replies to admin prompts by recreating the command context
    and calling the original admin handler.
    """
    # Map the conversation step to the original command and handler
    action = step.split('_', 1)[1]
    command_map = {
        'add_sub': ('addsubscriber', add_subscriber_handler),
        'remove_user': ('removeuser', remove_user_handler),
        'add_blacklist': ('add_blacklist', add_blacklist_handler),
        'remove_blacklist': ('remove_blacklist', remove_blacklist_handler),
        'sql_query': ('sql', sql_query_handler),
    }

    if action not in command_map:
        return

    command_name, handler_func = command_map[action]
    
    # We create a "fake" event text so the original handler can parse it
    # This is a clean way to reuse our existing command logic
    event.text = f"/{command_name} {value}"
    
    # Call the original handler (e.g., add_subscriber_handler)
    await handler_func(event)

# In bot/handlers/settings_handler.py

async def _handle_set_channel(event: "Message", user: "BotUser", user_id: int, step: str, value: "Optional[str]"):
    """
    Handles setting the source or target channel.
    This version edits the main prompt message to avoid errors.
    """
    trace_id = getattr(event, 'trace_id', 'N/A')
    
    if not value:
        # We don't need to reply here, as the user can just try again.
        return

    # No need to send a "Verifying..." message. We will edit the main prompt.
    
    try:
        channel_id, _ = parse_telegram_message_link(value)
        if not channel_id:
            channel_id = int(value)
    except (ValueError, TypeError):
        return await send_traceable_edit(event.client, event.chat_id, event.message.id, "❌ Invalid input. Please provide a valid channel ID or message link.")

    try:
        user_client = await get_user_client(user_id)
        if not user_client:
            return await send_traceable_edit(event.client, event.chat_id, event.message.id, "❌ User session invalid. Please `/login` again.")

        entity = await user_client.get_entity(channel_id)
        entity_title = getattr(entity, 'title', f"ID: {entity.id}")
        
        key_prefix = 'source_channel' if step == 'set_source' else 'target_channel'

        if step == 'set_target':
            user.upload_to_gdrive = False
            user.upload_to_koofr = False

        setattr(user, f'{key_prefix}_id', entity.id if isinstance(channel_id, str) else channel_id)
        setattr(user, f'{key_prefix}_title', entity_title)

        # Instead of replying, we go back to the main settings menu, which will now show the updated value.
        # This is a cleaner user experience.
        await settings_handler(event)
        
    except Exception as e:
        await send_traceable_reply(event, f"❌ Cannot access that channel: `{e}`")



async def _handle_set_range(event, user: BotUser, user_id: int, value: str):
    """
    Handles setting the message range with full validation for all input types.
    """
    if not value:
        return await event.reply("❌ Invalid input. Please provide a message link or ID(s).")
    
    parts = value.split()
    start_id, end_id, channel_id_from_link = None, None, None

    # --- Step 1: Determine Start and End IDs from input ---
    if len(parts) >= 1 and len(parts) <= 2:
        try:
            if 't.me' in parts[0]: # Input is link(s)
                ch1, start_id = parse_telegram_message_link(parts[0])
                if len(parts) == 2:
                    ch2, end_id = parse_telegram_message_link(parts[1])
                    if not all([ch1, start_id, ch2, end_id]) or ch1 != ch2:
                        return await event.reply("❌ Invalid links or links from different channels.")
                    channel_id_from_link = ch1
                else:
                    end_id = start_id
                    channel_id_from_link = ch1
            else: # Input is ID(s)
                start_id, end_id = (map(int, parts) if len(parts) == 2 else (int(parts[0]), int(parts[0])))
        except (ValueError, TypeError):
            return await event.reply("⚠️ Invalid format. Send one or two links/IDs.")
    else:
        return await event.reply("❌ Invalid input. Please send one or two message links/IDs.")

    # --- Step 2: Validate Permissions & Limits ---
    count = abs(end_id - start_id) + 1
    if count > user.max_batch_size and user.role != 'Owner':
        # This single check handles all plan limits, including free users (max_batch_size=5).
        return await event.reply(f"❌ **Batch Limit Exceeded!** Your plan allows a maximum of **{user.max_batch_size}** messages per batch.")
    
    # --- Step 3: Validate Channel if provided by link ---
    if channel_id_from_link:
        try:
            user_client = await get_user_client(user_id)
            if not user_client: return await event.reply("❌ User session invalid.")
            
            entity = await user_client.get_entity(channel_id_from_link)
            if isinstance(channel_id_from_link, str):
                user.source_channel_id = entity.id
            else:
                user.source_channel_id = channel_id_from_link
            user.source_channel_title = getattr(entity, 'title', f"ID: {entity.id}")
        except Exception as e:
            return await event.reply(f"❌ Could not verify channel from link: {e}")
    
    # --- Step 4: Save the final message range and reply ---
    user.message_range = [min(start_id, end_id), max(start_id, end_id)]
    await event.reply(
        f"✅ **Settings Updated!**\n\n"
        f"**Source:** `{user.source_channel_title or f'ID: {user.source_channel_id}'}`\n"
        f"**Range:** `{user.message_range[0]}` to `{user.message_range[1]}`"
    )

async def _handle_set_zip_password(event, user, value):
    """Handles the logic for setting the zip password and replies to the user."""
    # A None or empty string will clear the password
    password_to_save = value if value and value.lower() not in ['none', 'clear', 'remove'] else None
    user.zip_password = password_to_save
    config.logger.info(f"User {user.id} updated their zip password.")

    if password_to_save:
        await event.reply("✅ Zip password has been set.")
    else:
        await event.reply("✅ Zip password has been removed.")

async def _handle_set_naming(event, user, value, step, session):
    """Handle custom naming settings."""
    if not user.allow_renaming and user.role != 'Owner':
        return await event.reply("❌ Custom naming/captions are a paid feature.")
    
    value_to_save = "" if value.lower() in ['none', 'clear', 'remove'] else value
    key_map = {
        'set_prefix': 'rename_prefix', 
        'set_suffix': 'rename_suffix', 
        'set_caption': 'custom_caption',
        'set_filename_replace': 'filename_replace_rules', 
        'set_caption_replace': 'caption_replace_rules'
    }
    db_key = key_map[step]
    config.logger.info(f"Processing '{step}' for user {user_id}. Setting '{db_key}'.")
    
    blacklist = get_blacklist(session)
    if is_blacklisted(value_to_save, blacklist):
        return await event.reply("❌ Your input contains a blocked keyword. Please try again.")

    setattr(user, db_key, value_to_save)
    config.logger.debug(f"User {user_id} set {db_key} to: '{value_to_save}'")

async def _handle_set_workers(event, user, value):
    """Handle setting the worker limit."""
    if not user.allow_renaming and user.role != 'Owner':
        return await event.reply("❌ Custom worker settings are a paid feature.")
    
    try: 
        limit = int(value)
        if not (1 <= limit <= 15): 
            return await event.reply("⚠️ Worker limit must be between 1 and 15.")
        user.worker_limit = limit
    except (ValueError, TypeError): 
        return await event.reply("⚠️ Invalid format. Please use a single number.")

async def _handle_set_thumbnail(event, user, user_id):
    """Handle setting the thumbnail image."""
    if not user.allow_thumbnails and user.role != 'Owner':
        return await event.reply("❌ Custom thumbnails are a Pro/Power plan feature.")
    
    if not event.photo:
        return await event.reply("⚠️ This step requires an image. Please send a photo, not a file or text.")

    msg = await event.reply("🖼️ Processing thumbnail...")
    buffer = BytesIO()
    await event.client.download_media(event.photo, file=buffer)
    buffer.seek(0)
    file_path_in_storage = f"user_{user_id}/thumbnail_{int(time.time())}.jpg"
    
    try:
        config.logger.debug(f"Uploading to Supabase path: {file_path_in_storage}")
        config.supabase_client.storage.from_("thumbnails").upload(
            path=file_path_in_storage, file=buffer.getvalue(), file_options={"content-type": "image/jpeg"}
        )
        public_url = config.supabase_client.storage.from_("thumbnails").get_public_url(file_path_in_storage)
        user.thumbnail_url = public_url
        config.logger.info(f"Successfully updated thumbnail for user {user_id}.")
        await msg.edit("✅ Thumbnail updated successfully!")
    except Exception as e:
        config.logger.error(f"Supabase upload failed for user {user_id}: {e}", exc_info=True)
        await msg.edit("❌ Failed to upload thumbnail due to a storage error.")

async def _handle_import_config(event, user, user_id):
    """Handle importing settings from a JSON configuration file."""
    if not user.allow_renaming and user.role != 'Owner':
        return await event.reply("❌ Importing configurations is a paid feature.")
    
    if not event.document or event.document.mime_type != 'application/json':
        return await event.reply("⚠️ Please upload a valid configuration file (`.json`).")
    
    temp_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"config_{user_id}.json")
    await event.client.download_media(event.message, temp_path)
    
    try:
        with open(temp_path, 'r') as f:
            new_settings_dict = json.load(f)
    finally:
        os.remove(temp_path)

    if not isinstance(new_settings_dict, dict):
        return await event.reply("❌ Invalid or corrupted config file. Must be a JSON object.")
    
    for key, val in new_settings_dict.items():
        if hasattr(user, key) and key not in ['id', 'role', 'plan', 'expiry_date', 'thumbnail_url']:
            setattr(user, key, val)

    config.logger.info(f"Successfully imported settings for user {user_id}.")
    await event.reply("✅ Your configuration has been imported!")


# Helper functions for better organization and reusability
async def _handle_job_priority_bump(event, data, user_id):
    """Handle job priority bumping with proper error handling."""
    try:
        job_id_to_bump = int(data.split(':', 1)[1])
        with get_session() as session:
            job = session.get(Job, job_id_to_bump)
            if job and job.user_id == user_id:
                job.priority += 1
                session.add(job)
                session.commit()
                await event.answer(f"✅ Priority bumped for Job #{job_id_to_bump}")
                return await view_queue_handler(event, edit=True)
            else:
                return await event.answer("❌ Job not found in your queue.", alert=True)
    except (ValueError, IndexError):
        return await event.answer("❌ Invalid job ID.", alert=True)


async def _handle_menu_navigation(event, data, user_id):
    """Handle menu navigation with centralized logic."""
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        if data == 'back_to_main_menu':
            text, buttons = get_main_settings_menu(user)
        else:
            text, buttons = get_submenu(user, data)
    
    if text:
        return await event.edit(text, buttons=buttons, parse_mode='md')


async def _handle_clear_thumbnail(event, user_id):
    """Handle thumbnail clearing with proper cleanup."""
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        if user.thumbnail_url:
            try:
                file_path_to_delete = "/".join(user.thumbnail_url.split('/')[-2:])
                config.supabase_client.storage.from_("thumbnails").remove([file_path_to_delete])
            except Exception as e:
                config.logger.error(f"Failed to delete old thumbnail for user {user_id}: {e}")
        user.thumbnail_url = None
        session.add(user)
        session.commit()
    
    await event.answer("✅ Thumbnail cleared.")
    return await settings_handler(event)


async def _handle_toggle_setting(event, data, user_id):
    """Handles all toggle settings with integrated permission checks."""
    
    # Maps the button data to the database field name
    field_map = {
        'toggle_governor': 'enable_governor',
        'toggle_stream': 'stream_videos',
        'toggle_thumb_all': 'thumbnail_for_all',
        'toggle_zip': 'zip_files',
        'toggle_unzip': 'unzip_files'
    }
    
    # Maps protected toggles to the permission flag that controls them
    protected_toggles = {
        'toggle_zip': 'allow_zip',
        'toggle_unzip': 'allow_unzip'
    }

    field_to_toggle = field_map.get(data)
    if not field_to_toggle:
        return await event.answer("❌ Unknown setting.", alert=True)
    
    with get_session() as session:
        user = get_or_create_user(session, user_id)

        # Check permissions for protected features
        if data in protected_toggles:
            permission_field = protected_toggles[data]
            if not getattr(user, permission_field) and user.role != 'Owner':
                return await event.answer("❌ This is a premium feature.", alert=True)

        # Toggle the value
        current_value = getattr(user, field_to_toggle)
        setattr(user, field_to_toggle, not current_value)
        session.add(user)
        session.commit()
    
    await event.answer("✅ Setting updated.")
    return await settings_handler(event)


async def _handle_export_config(event, user_id):
    """Handles configuration export with proper file cleanup."""
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        # Exclude sensitive and non-user-configurable fields
        exclude_fields = {'id', 'role', 'plan', 'expiry_date', 'last_login'}
        settings_dict = user.model_dump(exclude=exclude_fields)
    
    export_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"settings_{user_id}.json")
    try:
        with open(export_path, 'w') as f:
            json.dump(settings_dict, f, indent=4, default=str)
        
        await event.client.send_file(
            event.chat_id, # Send to the chat where the button was pressed
            export_path,
            caption="Here is your bot configuration file. You can use `/settings` -> Tools -> Import Config to restore it later."
        )
        await event.answer("✅ Config exported!")
    except Exception as e:
        config.logger.error(f"Failed to export config for user {user_id}: {e}")
        await event.answer("❌ Failed to export config.", alert=True)
    finally:
        if os.path.exists(export_path):
            os.remove(export_path)

async def _handle_set_channel_prompt(event):
    """
    Handles the 'Set Channel' button press by creating a conversation
    step and prompting the user to forward a message.
    """
    user_id = event.sender_id
    
    # Set the state so the response_handler knows how to process the next message
    config.CONVERSATION_STATE[user_id] = {'step': 'set_target'}
    
    # Provide clear instructions to the user
    text = (
        "**🎯 Set Target Channel/Group**\n\n"
        "Please forward any message from your desired **public or private** target channel to me.\n\n"
        "I will automatically set it as your destination."
    )
    
    await event.edit(text, buttons=[[Button.inline("❌ Cancel", b"back_to_main_menu")]])


async def _handle_set_target_menu(event):
    """Display the target destination selection menu."""
    dest_text = "Please choose where you want to send the downloaded files."
    buttons = [
        [Button.inline("💬 To this Private Chat (Default)", b"dest_bot_pm")],
        [Button.inline("📂 To my Saved Messages", b"dest_saved_msgs")],
        [Button.inline("📢 To a Different Channel/Group", b"dest_set_channel")],
        [Button.inline("☁️ To My Google Drive", b"dest_gdrive")],
        [Button.inline("🗄️ To My Koofr", b"dest_koofr")],
        [Button.inline("⬅️ Back", b"back_to_main_menu")]
    ]
    return await event.edit(dest_text, buttons=buttons)


async def _handle_destination_setting(event, data, user_id):
    """Handle destination setting changes with mutually exclusive logic."""
    message = ""
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        
        # Reset all cloud flags first for a clean slate
        user.upload_to_gdrive = False
        user.upload_to_koofr = False
        
        if data == 'dest_bot_pm':
            user.target_channel_id = None
            message = "✅ Destination set to this PM."
        elif data == 'dest_saved_msgs':
            user.target_channel_id = 777
            message = "✅ Destination set to Saved Messages."
        elif data == 'dest_gdrive':
            if user.gdrive_creds_json:
                user.upload_to_gdrive = True
                message = "✅ Destination set to Google Drive."
            else:
                message = "⚠️ Google Drive not connected! Use `/login` first."
        elif data == 'dest_koofr':
            if user.koofr_email and user.koofr_app_password:
                user.upload_to_koofr = True
                message = "✅ Destination set to Koofr."
            else:
                message = "⚠️ Koofr not connected! Please use `/login` first."
        
        session.commit()
    
    await event.edit(message, buttons=[[Button.inline("⬅️ Back to Settings", b"back_to_main_menu")]])


async def _handle_reset_all(event):
    """Resets all of a user's settings to their default values."""
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        
        # Create a temporary, new user object to get all default values
        default_settings = BotUser()
        
        # List of fields that should NOT be reset
        protected_fields = {'id', 'role', 'plan', 'expiry_date', 'last_login'}
        
        # Loop through all model fields and reset them if not protected
        for field in user.model_fields:
            if field not in protected_fields:
                setattr(user, field, getattr(default_settings, field))
                
        session.add(user)
        session.commit()
    
    await event.answer("✅ Your settings have been reset to default.")
    # Refresh the menu to show the changes
    await settings_handler(event)


async def _handle_prompt_setup(event, data, user_id):
    """Handles setting conversation state and sending instructive prompts."""

    # --- Enhanced Instructional Texts ---
    
    range_prompt = (
        "📊 **Set Message Range**\n\n"
        "You can set the range in multiple ways:\n\n"
        "1️⃣ **Single Message (by ID):**\n   `178443`\n\n"
        "2️⃣ **Single Message (by Link):**\n   `https://t.me/c/123.../178443`\n\n"
        "3️⃣ **Batch of Messages (by IDs):**\n   `178443 178450`\n\n"
        "4️⃣ **Batch of Messages (by Links):**\n   _(Send two message links, separated by a space)_"
    )

    replace_rules_prompt = (
        "🔄 **Set Replacement Rules**\n\n"
        "Define rules to automatically modify text, separated by `|`.\n\n"
        "**Formats:**\n"
        "1. **Find & Replace:** `old text:new text`\n"
        "2. **Remove Text:** `text to remove`\n"
        "3. **Regular Expression:** `regex:pattern:replacement`\n\n"
        "**Example:**\n`Sponsored by:|For more info|regex:[0-9]+:NUMBER`\n\n"
        "To clear all rules, send `none`."
    )
    
    caption_prompt = (
        "📝 **Set Custom Caption**\n\n"
        "You can use placeholders and Markdown.\n\n"
        "**Placeholders:** `{filename}`, `{size}`, `{duration}`\n\n"
        "**Example:**\n`**File:** {filename}\n**Size:** {size}`\n\n"
        "To use the original caption, send `none`."
    )

    # --- Prompts Dictionary ---
    prompts_map = {
        'set_source': "➡️ Send the **Source** message link or Channel ID.",
        'set_target': "➡️ Send the **Destination** message link or Channel ID.",
        'set_range': range_prompt,
        'set_prefix': "➡️ Send the **Filename Prefix** (`none` to clear).",
        'set_suffix': "➡️ Send the **Filename Suffix** (`none` to clear).",
        'set_workers': "➡️ Send the new **Worker Limit** (e.g., `8`).",
        'set_thumbnail': "➡️ Send an **image** for your thumbnail.",
        'set_zip_password': "🔑 Send the **password** for your archives (`none` to clear).",
        'set_caption': caption_prompt,
        'set_filename_replace': replace_rules_prompt,
        'set_caption_replace': replace_rules_prompt,
        'import_config': "➡️ Upload your `settings.json` file."
    }

    if data in prompts_map:
        config.CONVERSATION_STATE[user_id] = {'step': data}
        await event.edit(
            prompts_map[data],
            buttons=[[Button.inline("⬅️ Back", b'back_to_main_menu')]],
            parse_mode='md',
            link_preview=False
        )
        return True 
    
    return False 


async def _handle_admin_menu_nav(event, action):
    """Handles navigation within the admin submenus."""
    if action == 'main':
        text, buttons = get_admin_menu()
    else:
        text, buttons = get_admin_submenu(action)
    
    if text:
        await event.edit(text, buttons=buttons, parse_mode='md')


async def _handle_admin_actions(event, action):
    """The single, consolidated dispatcher for all admin button actions."""
    # This block handles buttons that require the admin to type a command.
    # 'dashboard' has been removed from here.
    prompts = {
        'add_sub': "➡️ Send: `/addsubscriber <user_id> <duration> [plan]`",
        'remove_user': "➡️ Send: `/removeuser <user_id>`",
        'add_blacklist': "➡️ Send: `/add_blacklist <word1> ...`",
        'remove_blacklist': "➡️ Send: `/remove_blacklist <word1> ...`",
        'sql_query': "➡️ Send: `/sql SELECT ...`",
        'ban': "➡️ Send: `/ban <user_id> [duration] [reason]`",
        'unban': "➡️ Send: `/unban <user_id>`",
        'set_credits': "➡️ Send: `/set_credits <user_id> <amount>`",
        'set_universal_credit': "➡️ Send: `/set_universal_credit <amount>`",
        'add_admin': "➡️ Send: `/addadmin <user_id>`",
        'remove_admin': "➡️ Send: `/removeadmin <user_id>`",
        'broadcast': "➡️ Send: `/broadcast <message>`",
    }
    if action in prompts:
        return await event.edit(prompts[action], buttons=[[Button.inline("⬅️ Back", b"admin_menu:main")]])

    # This block handles buttons that trigger an action directly.
    action_map = {
        'list_subs': subscribers_handler,
        'view_blacklist': view_blacklist_handler, 
        'db_stats': db_stats_handler,
        'test_config': test_handler,
        'all_analytics': all_analytics_handler,
        'shutdown_confirm': shutdown_handler,
        'dashboard': dashboard_handler,
    }
    if action in action_map:
        return await action_map[action](event)
    
    # This handles the special case for the shutdown confirmation
    if action == 'shutdown':
        return await event.edit(
            "⚠️ **Are you sure you want to shut down the bot?**",
            buttons=[[Button.inline("YES, SHUT DOWN", b"admin:shutdown_confirm"), Button.inline("CANCEL", b"admin_menu:main")]]
        )


async def _handle_manage_action(event, action):
    """Dispatches actions from the /manage menu."""
    if action == 'view_queue':
        await view_queue_handler(event, edit=True)
    elif action == 'clear_queue':
        await clear_queue_handler(event)
    elif action == 'view_failed':
        await failed_log_handler(event)
    elif action == 'retry_failed':
        await retry_handler(event)
    elif action == 'clear_failed_log':
        with get_session() as session:
            statement = delete(FailedItem).where(FailedItem.user_id == event.sender_id)
            session.exec(statement)
            session.commit()
        await event.answer("✅ All failed items have been cleared.")
        text, buttons = get_manage_menu()
        await event.edit(text, buttons=buttons)


async def _handle_view_plans(event, user_id):
    """Displays a rich, detailed comparison of all subscription plans."""
    with get_session() as session:
        user = get_or_create_user(session, user_id)
    
    current_plan = user.plan
    
    # Plan details defined for easy maintenance
    plans = [
        {"name": "Free", "emoji": "🔰", "spec_key": "free"},
        {"name": "Standard", "emoji": "🥈", "spec_key": "standard"},
        {"name": "Pro", "emoji": "🥇", "spec_key": "pro"},
        {"name": "Power", "emoji": "💎", "spec_key": "power"}
    ]
    
    message_parts = ["**✨ Available Subscription Plans & Features ✨**\n"]
    
    for plan in plans:
        spec = PLAN_SPECS.get(plan['spec_key'], {})
        is_current = "✅ **(Your Current Plan)**" if current_plan == plan['spec_key'] else ""
        
        message_parts.append(f"---")
        message_parts.append(f"{plan['emoji']} **{plan['name']} Plan** {is_current}")
        message_parts.append(f"  - Batch Limit: **{spec.get('max_batch_size', 'N/A')}** messages")
        message_parts.append(f"  - Concurrent Jobs: **{spec.get('max_concurrent_jobs', 'N/A')}**")
        message_parts.append(f"  - Worker Speed: **{spec.get('worker_limit', 'N/A')}x**")
        message_parts.append(f"  - Customization: **{'Yes' if spec.get('allow_renaming') else 'No'}**")
        message_parts.append(f"  - Thumbnails: **{'Yes' if spec.get('allow_thumbnails') else 'No'}**")
        message_parts.append(f"  - Zip/Unzip: **{'Yes' if spec.get('allow_zip') else 'No'}**\n")

    # CORRECTED: Join the message parts to create the final text
    final_text = "\n".join(message_parts)

    buttons = [
        [Button.inline("🥈 Select Standard", b"select_plan:standard")],
        [Button.inline("🥇 Select Pro", b"select_plan:pro")],
        [Button.inline("💎 Select Power", b"select_plan:power")],
        [Button.inline("⬅️ Back", b"back_to_main_menu")]
    ]
    
    await event.edit(final_text, buttons=buttons, link_preview=False, parse_mode='md')


async def _handle_select_plan(event, data):
    """Confirms a plan selection and shows the payment button."""
    plan = data.split(':', 1)[1]
    text = (
        f"You have selected the **{plan.capitalize()} Plan**.\n\n"
        "Click the button below to see payment instructions, or go back."
    )
    # CORRECTED: Added a 'Back' button
    buttons = [
        [Button.inline("➡️ Get Payment Info", b"get_payment_qr")],
        [Button.inline("⬅️ Back to Plans", b"view_plans")]
    ]
    await event.edit(text, buttons=buttons)


async def _handle_get_payment_qr(event):
    """Displays the link to the payment instructions page."""
    text = "Please follow the instructions at the link below, then notify the admin."
    # CORRECTED: Added a 'Back' button
    buttons = [
        [Button.url("View Payment Instructions", config.PAYMENT_INFO_URL)],
        [Button.inline("I've Paid, Notify Admin", b"paid_notify")],
        [Button.inline("⬅️ Back", b"view_plans")]
    ]
    await event.edit(text, buttons=buttons)


async def _handle_paid_notify(event):
    """Notifies the owner and gives the user clear next steps."""
    sender = await event.get_sender()
    owner_mention = f"[{sender.first_name}](tg://user?id={sender.id})"
    
    # Notify the owner in the background
    await config.client.send_message(
        config.OWNER_ID,
        f"🔔 **Payment Notification**\nUser {owner_mention} (`{sender.id}`) has indicated they have paid. Please verify their payment and use `/addsubscriber`."
    )
    
    # Update the user's message with clear instructions and buttons
    text = (
        "✅ **Admin Notified!**\n\n"
        "Thank you! The owner has been notified of your payment. "
        "Please send your payment receipt directly to the owner to complete the process."
    )
    buttons = [
        [Button.url("✉️ Send Receipt to Admin", f"https://t.me/{config.OWNER_USERNAME}")],
        [Button.inline("⬅️ Back to Main Menu", b'back_to_main_menu')]
    ]
    await event.edit(text, buttons=buttons)


async def manage_handler(event):
    text, buttons = get_manage_menu()
    await event.reply(text, buttons=buttons)


async def _handle_admin_prompt(event, action):
    """Sends a user-friendly prompt for admin actions."""
    prompts = {
        'add_sub': "➡️ Send: `/addsubscriber <user_id> <duration> [plan]`",
        'remove_user': "➡️ Send: `/removeuser <user_id>`",
        'ban': "➡️ Send: `/ban <user_id> [duration] [reason]`",
        'unban': "➡️ Send: `/unban <user_id>`",
        'set_credits': "➡️ Send: `/set_credits <user_id> <amount>`",
        'set_universal_credit': "➡️ Send: `/set_universal_credit <amount>`",
        'add_blacklist': "➡️ Please send the **word(s)** to add to the blacklist.",
        'remove_blacklist': "➡️ Please send the **word(s)** to remove from the blacklist.",
        'sql_query': "➡️ Please send your read-only **SELECT query**."
    }
    if action in prompts:
        config.CONVERSATION_STATE[user_id] = {'step': f'admin_{action}'}
        await event.edit(prompts[action], buttons=[[Button.inline("⬅️ Back", b"admin_menu:main")]])


async def _handle_manage_action(event, action):
    """Dispatches actions from the /manage menu."""
    user_id = event.sender_id
    if action == 'view_queue':
        await view_queue_handler(event, edit=True)
    elif action == 'clear_queue':
        await clear_queue_handler(event)
    elif action == 'view_failed':
        await failed_log_handler(event) 
    elif action == 'retry_failed':
        await retry_handler(event)
    elif action == 'clear_failed_log':
        with get_session() as session:
            statement = delete(FailedItem).where(FailedItem.user_id == user_id)
            session.exec(statement)
            session.commit()
        await event.answer("✅ All failed items have been cleared.")
        # Optionally, refresh the manage menu
        text, buttons = get_manage_menu()
        await event.edit(text, buttons=buttons)


async def _handle_direct_link_transfer(event, user_id):
    """Parses all valid Telegram links from a message and presents action buttons."""
    config.logger.info(f"User {user_id} triggered direct link workflow.")
    
    # CORRECTED: This regex now finds both private AND public links.
    # It looks for "t.me/", then a channel part (\w+), then the message part.
    link_pattern = r"https://t\.me/(?:c/)?\w+/(?:\d+/)?\d+"
    links = re.findall(link_pattern, event.text)
    
    if not links:
        config.logger.warning("Direct link workflow triggered, but no valid links found in message.")
        return

    # Parse all found links using our correct utility
    parsed_links = [parse_telegram_message_link(link) for link in links]
    valid_links = [l for l in parsed_links if all(l)]

    if not valid_links:
        return await event.reply("❌ The links you sent appear to be invalid or unsupported.")

    # Check if all links are from the same channel
    first_channel_id = valid_links[0][0]
    if not all(l[0] == first_channel_id for l in valid_links):
        return await event.reply("❌ Error: All links must be from the same channel.")

    # --- CORRECTED: These lines must be active ---
    # Determine the message range from all valid links found
    msg_ids = [l[1] for l in valid_links]
    message_range = [min(msg_ids), max(msg_ids)]
    
    # --- Get the pre-flight summary ---
    # The count is based on the full range, not just the provided links
    total_files_in_range = abs(message_range[1] - message_range[0]) + 1
    preview = get_job_preview(user_id, total_files_in_range)

    # Temporarily store the proposed job in the conversation state
    config.CONVERSATION_STATE[user_id] = {
        'step': 'direct_action_confirm',
        'source_channel_id': first_channel_id,
        'message_range': message_range
    }

    # --- Update the confirmation message with the summary ---
    text = (
        f"**✅ Job Preview**\n\n"
        f"**Source:** `ID {first_channel_id}`\n"
        f"**Files to Process:** {preview['file_count']:,}\n"
        f"**Estimated Time:** ~{preview['estimated_duration']}\n\n"
        "What would you like to do?"
    )
    buttons = [
        [Button.inline("➕ Add to Queue", b'direct_queue'), Button.inline("▶️ Start Now", b'direct_start')],
        [Button.inline("❌ Cancel", b'direct_cancel')]
    ]
    await event.reply(text, buttons=buttons)


async def _handle_direct_action(event, user_id, start_immediately=False):
    """
    Handles the final confirmation of a job from a direct link, adds it to the
    database, and pushes it to the Redis queue if starting immediately.
    """
    state = config.CONVERSATION_STATE.pop(user_id, None)
    if not state or state.get('step') != 'direct_action_confirm':
        return await send_traceable_edit(event, "⚠️ This action has expired.", buttons=None)

    try:
        user_client = await get_user_client(user_id)
        if not user_client:
            return await send_traceable_edit(event, "❌ Your user session is invalid. Please /login again.", buttons=None)

        entity = await user_client.get_entity(state['source_channel_id'])
        final_channel_id = entity.id
        final_channel_title = getattr(entity, 'title', f"ID: {final_channel_id}")

        with get_session() as session:
            user = get_or_create_user(session, user_id)
            
            # --- VALIDATION (from Version 2) ---
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            usage_bytes = session.exec(select(func.sum(JobAnalytics.data_transferred_bytes)).where(JobAnalytics.user_id == user_id, JobAnalytics.completed_at > thirty_days_ago)).one_or_none() or 0
            if usage_bytes >= (user.data_cap_gb * (1024**3)) and user.role != 'Owner':
                return await send_traceable_edit(event, f"❌ Monthly data limit reached.")

            queued_count = session.exec(select(func.count(Job.id)).where(Job.user_id == user_id, Job.status == "queued")).one()
            if queued_count >= user.max_queue_limit and user.role != 'Owner':
                return await send_traceable_edit(event, f"❌ Queue is full.")

            # --- JOB CREATION & REDIS PUSH (from Version 1) ---
            job_settings = user.model_dump(
                exclude={'id', 'role', 'plan', 'expiry_date', 'last_login', 'ban_expiry_date', 'ban_reason', 'gdrive_creds_json', 'koofr_email', 'koofr_app_password'}
            )
            
            job_settings.update({
                'user_id': user.id,
                'source_channel_id': final_channel_id,
                'source_channel_title': final_channel_title,
                'message_range': state['message_range'],
                'status': 'in_queue' if start_immediately else 'queued',
                'download_workers': user.worker_limit,
                'upload_workers': user.worker_limit,
            })
            
            new_job = Job.model_validate(job_settings)
            session.add(new_job)
            session.commit()
            session.refresh(new_job)

            if start_immediately:
                bot.config.redis_client.lpush('job_queue', new_job.id)
                reply_text = (
                    "✅ **Job sent to processing queue!**\n\n"
                    f"The Muscle workers will begin processing Job `#{new_job.id}` shortly."
                )
                buttons = None
            else:
                reply_text = (
                    f"✅ **Job `#{new_job.id}` added to your queue.**\n\n"
                    "Run `/start_queue` when you are ready to begin processing."
                )
                buttons = [[Button.inline("▶️ Start Queue Now", b'start_queue')]]
        
        await send_traceable_edit(event, reply_text, buttons=buttons)

    except Exception as e:
        config.logger.exception(f"FATAL error in _handle_direct_action for user {user_id}: {e}")
        await send_traceable_edit(event, f"❌ An unexpected error occurred: `{e}`.", buttons=None)


#async def _handle_direct_action(event, user_id, start_immediately=False):
#    """Handles the 'Add to Queue' or 'Start Now' button press with full validation."""
#    state = config.CONVERSATION_STATE.get(user_id)
#    if not (state and state.get('step') == 'direct_action_confirm'):
#        return await event.edit("⚠️ This action has expired. Please send the links again.")

#    channel_id_or_username = state['source_channel_id']
#    msg_range = state['message_range']

#    try:
#        user_client = await get_user_client(user_id)
#        if not user_client: return await event.edit("❌ User session invalid.")
#        
#        entity = await user_client.get_entity(channel_id_or_username)
#        final_channel_id = entity.id
#        final_channel_title = getattr(entity, 'title', f"ID: {final_channel_id}")

#        with get_session() as session:
#            user = get_or_create_user(session, user_id)
#            
#            # --- Perform all validations ---
#            # Data Cap Check...
#            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
#            usage_bytes = session.exec(select(func.sum(JobAnalytics.data_transferred_bytes)).where(JobAnalytics.user_id == user_id, JobAnalytics.completed_at > thirty_days_ago)).one_or_none() or 0
#            if usage_bytes >= (user.data_cap_gb * (1024**3)) and user.role != 'Owner':
#                return await event.edit(f"❌ Monthly data limit reached.")

#            # Queue Limit Check...
#            queued_count = session.exec(select(func.count(Job.id)).where(Job.user_id == user_id, Job.status == "queued")).one()
#            if queued_count >= user.max_queue_limit and user.role != 'Owner':
#                return await event.edit(f"❌ Queue is full.")

#            # --- Create the Job ---
#            job_data = user.model_dump(
#                exclude={'id', 'role', 'plan', 'expiry_date', 'last_login', 'ban_expiry_date', 
#                         'ban_reason', 'access_token', 'token_expiry', 'gdrive_creds_json',
#                         'source_channel_id', 'source_channel_title', 'message_range', 'worker_limit'}
#            )
#            
#            # --- CORRECTED: Add all required fields for the new Job ---
#            job_data.update({
#                'user_id': user.id,
#                'source_channel_id': final_channel_id,
#                'source_channel_title': final_channel_title,
#                'message_range': msg_range,
#                'download_workers': user.worker_limit,
#                'upload_workers': user.worker_limit,
#                'upload_to_gdrive': user.upload_to_gdrive
#            })
#            
#            job_to_queue = Job.model_validate(job_data)
#            session.add(job_to_queue)
#            session.commit()
#            session.refresh(job_to_queue)
#            

#            # --- DEFINITIVE FIX: Redis logic is now correctly placed here ---
#            if start_immediately:
#                # Push the new job's ID to the Redis queue for immediate processing
#                bot.config.redis_client.lpush('job_queue', job.id)
#                reply_text = (
#                    "✅ **Job has been sent to the processing queue!**\n\n"
#                    "The Muscle workers will pick it up shortly. You can monitor progress with `/status`."
#            )
#            else:
#                reply_text = (
#                    f"✅ **Job #{job.id} has been added to your local queue.**\n\n"
#                    f"Run `/start_queue` when you are ready to send it for processing."
#                )

#        await send_traceable_edit(event, reply_text)

#        del config.CONVERSATION_STATE[user_id]
#        
#        if start_immediately:
#            await event.edit("🚀 Your transfer is starting now...", buttons=None)
#            await start_queue_handler(event)
#        else:
#            text = f"✅ **Job #{job_to_queue.id} has been added to your queue.**"
#            buttons = [[Button.inline("▶️ Start Queue Now", b'start_queue'), Button.inline("🗂️ Manage Queue", b'manage_view_queue')]]
#            await event.edit(text, buttons=buttons)

#    except Exception as e:
#        config.logger.exception(f"Error in direct action handler for user {user_id}: {e}")
#        if user_id in config.CONVERSATION_STATE:
#            del config.CONVERSATION_STATE[user_id]
#        await event.edit(f"❌ An error occurred: `{e}`")


async def _handle_check_join(event):
    """Handles the 'Check Again' button after a user is asked to join."""
    await event.answer("Checking your membership status...")
    
    # Calls the shared utility function we created
    is_member = await check_channel_membership(event)
    
    if is_member:
        # If they are now a member, delete the "Please Join" message
        # and tell them to proceed.
        await event.delete()
        await event.respond("✅ Thank you for joining! You can now use `/login` again.")

# In bot/handlers/settings_handler.py

async def _handle_dashboard_actions(event, action):
    """
    Handles all button presses from the /dashboard menu with comprehensive logging.
    """
    trace_id = getattr(event, 'trace_id', 'N/A')
    user_id = event.sender_id
    text = "An unknown error occurred." # Default text
    buttons = [[Button.inline("⬅️ Back to Dashboard", b"dashboard:main")]]

    try:
        if action == 'overview':
            config.logger.info(f"TRACE [{trace_id}] - Generating 'Overview' dashboard for user {user_id}.")
            with get_session() as session:
                stats = get_overview_stats(session)
            text = (
                f"**📈 Bot Overview**\n\n"
                f"**Total Users:** {stats['total_users']}\n"
                f"**Total Jobs:** {stats['total_jobs']}\n"
                f"**Completed:** {stats['completed_jobs']}\n"
                f"**Failed:** {stats['failed_jobs']}\n"
                f"**Total Data Transferred:** {stats['total_data_gb']:.2f} GB"
            )

        elif action == 'plans':
            config.logger.info(f"TRACE [{trace_id}] - Generating 'Plans' dashboard for user {user_id}.")
            with get_session() as session:
                stats = get_plan_breakdown(session)
            plan_lines = [f"**- {plan.title()}:** {count} users" for plan, count in stats]
            text = "**📊 Plan Breakdown**\n\n" + ("\n".join(plan_lines) if plan_lines else "No user data found.")

        elif action == 'health':
            config.logger.info(f"TRACE [{trace_id}] - Generating 'Health' dashboard for user {user_id}.")
            with get_session() as session:
                errors = get_recent_errors(session)
            error_lines = [f"**- ID {e.id}:** `{e.error_message}`" for e in errors]
            text = "**⚠️ System Health**\n\n**Recent Errors:**\n" + ("\n".join(error_lines) if error_lines else "No recent errors logged. ✅")

        else: # This handles the 'main' action
            config.logger.info(f"TRACE [{trace_id}] - Returning to main dashboard for user {user_id}.")
            text, buttons = get_dashboard_menu()

        # The edit action is now within the try block
        await event.edit(text, buttons=buttons)
        config.logger.info(f"TRACE [{trace_id}] - Successfully sent '{action}' dashboard to user {user_id}.")

    except Exception as e:
        config.logger.exception(f"TRACE [{trace_id}] - CRITICAL FAILURE in dashboard handler for action '{action}': {e}")
        # Attempt to notify the user of the failure
        try:
            await event.edit(f"❌ An error occurred while generating the '{action}' report. Trace ID: `{trace_id}`")
        except Exception:
            pass # Ignore if we can't even send the error message



async def _handle_gdrive_code(event, state, user_id):
    """
    Handles the authorization code from the user, completes the OAuth flow
    initiated by the rclone identity, and saves the raw token to the database.
    """
    flow = state.get('flow')
    text = event.text.strip()
    if not flow:
        return await event.reply("⚠️ Your login session has expired. Please try `/gdrive_login` again.")

    try:
        # The user's code is correct here.
        auth_code = text
        if 'code=' in text:
            match = re.search(r'code=([^&]+)', text)
            if match: auth_code = match.group(1)
        auth_code = urllib.parse.unquote(auth_code)

        # --- Use the flow object from the login handler to fetch the token ---
        # This ensures we use the same client_id/secret for the entire process.
        flow.fetch_token(code=auth_code)
        
        # The credentials object from the flow contains the refresh_token.
        # We need to convert it to a JSON format that rclone understands.
        creds = flow.credentials
        
        # rclone expects a specific JSON structure.
        token_data = {
            'access_token': creds.token,
            'token_type': 'Bearer',
            'refresh_token': creds.refresh_token,
            'expiry': creds.expiry.isoformat() + 'Z'
        }
        
        # Save this rclone-compatible JSON to the database.
        with get_session() as session:
            user = get_or_create_user(session, user_id)
            user.gdrive_creds_json = json.dumps(token_data)
            session.add(user)
            session.commit()
            
        await event.reply("✅ **Google Drive connected successfully!**")
        
    except Exception as e:
        config.logger.exception(f"GDrive authentication failed for user {user_id}: {e}")
        await event.reply(f"❌ **Authentication Failed:** `{e}`. Please try `/gdrive_login` again.")
    finally:
        config.CONVERSATION_STATE.pop(user_id, None)

