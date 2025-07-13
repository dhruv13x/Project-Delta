#==== admin_commands.py ====

# Standard library imports
import asyncio
import time
import csv
import os
import json
import shutil
import psutil
from datetime import datetime, timedelta
from io import StringIO

# Third-party imports
from sqlalchemy import case, func, text
from sqlmodel import delete, select
from telethon.tl import types

# Local application imports
from .. import config

from ..database import (
    clear_blacklist_cache,
    get_blacklist,
    get_or_create_user,
    get_session,
    get_user_role, 
    log_audit_event,
)

from ..models import (
    BlacklistedWord,
    BotUser,
    FailedItem,
    Job,
    JobAnalytics,
)
from ..ui import get_admin_menu, get_dashboard_menu
from ..utils import get_user_client, humanbytes, send_traceable_reply, send_traceable_edit
from .decorators import admin_only, owner_only, subscriber_only, log_command


# In bot/handlers/admin_commands.py

from ..ui import get_dashboard_menu

# ... (the rest of your file's code)


# ==============================================================================
# Constants
# ==============================================================================
# In admin_commands.py

PLAN_SPECS = {
    "free": {
        "max_concurrent_jobs": 1, 
        "max_queue_limit": 3, 
        "worker_limit": 2, 
        "max_batch_size": 10,
        "data_cap_gb": 5,
        "allow_batch": True, 
        "allow_renaming": False, 
        "allow_thumbnails": False, 
        "allow_zip": False, 
        "allow_unzip": False,
        "allow_gdrive": False,
        "allow_koofr": False,
        "allow_auto_thumb": False
    },
    "standard": {
        "max_concurrent_jobs": 2, 
        "max_queue_limit": 10, 
        "worker_limit": 5, 
        "max_batch_size": 500,  
        "data_cap_gb": 50,
        "allow_batch": True, 
        "allow_renaming": True,  
        "allow_thumbnails": True, 
        "allow_zip": False, 
        "allow_unzip": True,
        "allow_gdrive": True,
        "allow_koofr": False,
        "allow_auto_thumb": False
    },
    "pro": {
        "max_concurrent_jobs": 3, 
        "max_queue_limit": 50, 
        "worker_limit": 10, 
        "max_batch_size": 2000, 
        "data_cap_gb": 200,
        "allow_batch": True, 
        "allow_renaming": True,  
        "allow_thumbnails": True,  
        "allow_zip": True,  
        "allow_unzip": True,
        "allow_gdrive": True,
        "allow_koofr": True,
        "allow_auto_thumb": True
    },
    "power": {
        "max_concurrent_jobs": 5, 
        "max_queue_limit": 999,
        "worker_limit": 15, 
        "max_batch_size": 4000, 
        "data_cap_gb": 1000,
        "allow_batch": True, 
        "allow_renaming": True,  
        "allow_thumbnails": True,  
        "allow_zip": True,  
        "allow_unzip": True,
        "allow_gdrive": True,
        "allow_koofr": True,
        "allow_auto_thumb": True
    },
}

# ==============================================================================
# Admin & Owner Command Handlers
# ==============================================================================

@admin_only
async def admin_handler(event):
    """Displays the main admin command panel."""
    text, buttons = get_admin_menu()
    await event.reply(text, buttons=buttons, parse_mode='md')

@owner_only
async def add_admin_handler(event):
    """Gamma v2: Promotes a user to Admin by updating their record in the database."""
    try:
        _, user_id_str = event.text.split()
        user_id = int(user_id_str)

        if user_id == event.sender_id:
            return await event.reply("Owners cannot be Admins.")
        
        with get_session() as session:
            # Get the user's record, or create a new one if they don't exist yet
            user = get_or_create_user(session, user_id)
            user.role = 'Admin'
            # Admins do not have plans or expiry dates, so we clear them
            user.plan = 'admin'
            user.expiry_date = None
            
            session.add(user)
            session.commit()

        log_audit_event(event.sender_id, "ADD_ADMIN", f"Target User: {user_id}")
        await event.reply(f"✅ User `{user_id}` has been promoted to **Admin**.")
    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/addadmin <user_id>`")

@owner_only
async def remove_admin_handler(event):
    """Gamma v2: Demotes an Admin to User by updating their database record."""
    try:
        _, user_id_str = event.text.split()
        user_id = int(user_id_str)

        if user_id == config.OWNER_ID:
            return await event.reply("❌ The Owner cannot be removed.")

        with get_session() as session:
            user = session.get(BotUser, user_id)
            if user and user.role == 'Admin':
                # Demote the user, don't delete them
                user.role = 'User'
                user.plan = 'standard' # Revert to a default plan
                session.add(user)
                session.commit()
                log_audit_event(event.sender_id, "REMOVE_ADMIN", f"Target User: {user_id_str}")
                await event.reply(f"✅ User `{user_id_str}` has been demoted from Admin role.")
            else:
                await event.reply(f"⚠️ User `{user_id_str}` is not an Admin.")
    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/removeadmin <user_id>`")

@admin_only
async def test_handler(event):
    """
    Runs a comprehensive health check on the bot's core systems,
    including the dump channel.
    """
    user_id = event.sender_id
    msg = await send_traceable_reply(event, "🧪 **Running Comprehensive Health Check...** Please wait.")
    
    results = []
    is_ok = True

    # 1. Test Database Connection
    try:
        with get_session() as session:
            session.exec(select(1)).one()
        results.append("✅ **PostgreSQL:** Connection OK")
    except Exception as e:
        results.append(f"❌ **PostgreSQL:** FAILED - `{e}`")
        is_ok = False

    # 2. Test Supabase Storage Connection
    try:
        config.supabase_client.storage.list_buckets()
        results.append("✅ **Supabase Storage:** Connection OK")
    except Exception as e:
        results.append(f"❌ **Supabase Storage:** FAILED - `{e}`")
        is_ok = False

    # 3. Test User Client Session
    try:
        user_client = await get_user_client(user_id)
        if not user_client or not await user_client.is_user_authorized():
            raise ConnectionError("Login session is invalid or expired.")
        results.append("✅ **Your Client:** Login session is active.")
    except Exception as e:
        results.append(f"❌ **Your Client:** FAILED - `{e}`. Use /login.")
        is_ok = False
        
    # 4. === NEW: Test Dump Channel ===
    if not config.DUMP_CHANNEL_ID:
        results.append("⚠️ **Dump Channel:** Not configured.")
    else:
        try:
            test_msg = f"🧪 **Health Check:** Dump channel test successful at {datetime.utcnow()} UTC."
            await config.client.send_message(config.DUMP_CHANNEL_ID, test_msg)
            results.append("✅ **Dump Channel:** Access and send OK.")
        except Exception as e:
            results.append(f"❌ **Dump Channel:** FAILED - `{e}`")
            is_ok = False
        
    final_text = f"**{'✅ All Systems Operational' if is_ok else '❌ Health Check Failed'}**\n\n" + "\n".join(results)
    await send_traceable_edit(msg, final_text)

@admin_only
async def add_subscriber_handler(event):
    """Adds or updates a subscriber and sets their plan limits."""
    try:
        config.logger.info(f"ADD_SUBSCRIBER: Received raw command: '{event.text}'")
        parts = event.text.split()
        
        # --- Definitive, Robust Argument Parsing ---
        if len(parts) < 3:
            raise ValueError("Not enough arguments.")
            
        user_id_str = parts[1]
        duration_str = parts[2]
        plan_name = parts[3].lower() if len(parts) > 3 else 'standard'

        if plan_name not in PLAN_SPECS:
            return await event.reply("⚠️ Invalid plan. Use: 'standard', 'pro', or 'power'.")

        user_id = int(user_id_str)
        duration_val = int(duration_str[:-1])
        duration_unit = duration_str[-1].lower()

        # CORRECTED: Use UTC for all timestamp creation
        if duration_unit == 'd': expiry_date = datetime.utcnow() + timedelta(days=duration_val)
        elif duration_unit == 'm': expiry_date = datetime.utcnow() + timedelta(days=duration_val * 30)
        else: raise ValueError("Invalid duration unit.")

        with get_session() as session:
            user = get_or_create_user(session, user_id)
            specs = PLAN_SPECS[plan_name]

            # Use a single, clean loop to set all plan-specific attributes
            for key, value in specs.items():
                setattr(user, key, value)
            
            user.role = 'Subscriber'
            user.plan = plan_name
            user.expiry_date = expiry_date
            
            session.add(user)
            session.commit()

        log_audit_event(event.sender_id, "ADD_SUBSCRIBER", f"Target: {user_id}, Plan: {plan_name}")
        
        # Confirm action to the admin
        await event.reply(f"✅ User `{user_id}` is now a **{plan_name.capitalize()} Subscriber**.")
        
        # Notify the user directly
        try:
            await config.client.send_message(
                user_id,
                f"🎉 **Subscription Activated!**\n\nYour **{plan_name.capitalize()} Plan** is now active."
            )
        except Exception as e:
            await event.reply(f"⚠️ Could not notify the user: {e}")
            
    except (ValueError, IndexError) as e:
        config.logger.error(f"ADD_SUBSCRIBER: Parsing failed. Error: {e}", exc_info=True)
        await event.reply("⚠️ **Invalid format.** Use: `/addsubscriber <user_id> <duration> [plan]`")

@admin_only
async def remove_user_handler(event):
    """Removes a user and all their associated data."""
    try:
        _, user_id_str = event.text.split()
        user_id = int(user_id_str)

        if user_id == config.OWNER_ID:
            return await event.reply("❌ The Owner cannot be removed.")

        with get_session() as session:
            user_to_delete = session.get(BotUser, user_id)
            if not user_to_delete:
                return await event.reply(f"⚠️ User `{user_id}` not found.")
            
            # CORRECTED: Capture the role before deletion
            role = user_to_delete.role
            
            session.exec(delete(Job).where(Job.user_id == user_id))
            session.exec(delete(FailedItem).where(FailedItem.user_id == user_id))
            session.exec(delete(JobAnalytics).where(JobAnalytics.user_id == user_id))
            
            session.delete(user_to_delete)
            session.commit()
        
        session_file = os.path.join(config.SESSIONS_DIR, f"user_{user_id_str}.session_string")
        if os.path.exists(session_file):
            os.remove(session_file)

        try:
            user_folder_path = f"user_{user_id}"
            files = config.supabase_client.storage.from_("thumbnails").list(path=user_folder_path)
            if files:
                file_paths_to_remove = [f"{user_folder_path}/{file['name']}" for file in files]
                config.supabase_client.storage.from_("thumbnails").remove(file_paths_to_remove)
        except Exception as e:
            config.logger.error(f"Could not remove Supabase assets for user {user_id}: {e}")

        log_audit_event(event.sender_id, "REMOVE_USER", f"Removed {role}: {user_id_str}")
        await event.reply(f"✅ User `{user_id}` has been completely removed from the system.")
    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/removeuser <user_id>`")

@admin_only
async def subscribers_handler(event):
    """Gamma v2: Displays user list by querying the database."""
    with get_session() as session:
        # Define a custom sort order: Owner > Admin > Others
        role_order = case(
            (BotUser.role == 'Owner', 1),
            (BotUser.role == 'Admin', 2),
            else_=3
        )
        # Fetch all users, sorted by the custom role order, then by ID
        all_users = session.exec(select(BotUser).order_by(role_order, BotUser.id)).all()

    if not all_users:
        return await event.reply("No users found in the database.")

    parts = ["**👥 User List**\n"]
    for user in all_users:
        if user.role == 'Owner':
            parts.append(f"👑 `{user.id}` - **Owner** (System)")
        elif user.role == 'Admin':
            parts.append(f"🛡️ `{user.id}` - **Admin** (Never expires)")
            
        elif user.role == 'Subscriber':
            # CORRECTED: Compare against UTC now
            status_emoji = "✅" if user.expiry_date and user.expiry_date > datetime.utcnow() else "❌"
            expiry_str = f"Expires: {user.expiry_date.strftime('%Y-%m-%d')}" if user.expiry_date else "Expired"
            plan = user.plan.capitalize()
            parts.append(f"👤 `{user.id}` - **{plan} Subscriber** ({status_emoji} {expiry_str})")
            
        elif user.role == 'Banned': 
            expiry_str = f"until {user.ban_expiry_date.strftime('%Y-%m-%d')}" if user.ban_expiry_date else "Permanently"
            reason_str = f"Reason: {user.ban_reason}" if user.ban_reason else ""
            parts.append(f"🚫 `{user.id}` - **Banned** ({expiry_str}) {reason_str}")
            
        else: # Regular users
            parts.append(f"⚪️ `{user.id}` - **User** (No subscription)")
            
    await event.reply("\n".join(parts))

@admin_only
async def add_blacklist_handler(event):
    """Adds one or more words to the database blacklist."""
    try:
        words_to_add = event.text.split()[1:]
        if not words_to_add:
            return await event.reply("Usage: `/add_blacklist word1 word2 ...`")

        added_count = 0
        with get_session() as session:
            for word in words_to_add:
                # Check if word already exists
                exists = session.exec(select(BlacklistedWord).where(BlacklistedWord.word == word.lower())).first()
                if not exists:
                    new_word = BlacklistedWord(word=word.lower())
                    session.add(new_word)
                    added_count += 1
            session.commit()
        
        clear_blacklist_cache() # Invalidate the cache immediately
        await event.reply(f"✅ Added {added_count} new word(s) to the blacklist.")
    except Exception as e:
        await event.reply(f"❌ An error occurred: {e}")

@admin_only
async def remove_blacklist_handler(event):
    """Removes one or more words from the database blacklist."""
    try:
        words_to_remove = event.text.split()[1:]
        if not words_to_remove:
            return await event.reply("Usage: `/remove_blacklist word1 word2 ...`")

        with get_session() as session:
            statement = delete(BlacklistedWord).where(BlacklistedWord.word.in_([w.lower() for w in words_to_remove]))
            result = session.exec(statement)
            session.commit()
        
        clear_blacklist_cache() # Invalidate the cache immediately
        await event.reply(f"✅ Removed {result.rowcount} word(s) from the blacklist.")
    except Exception as e:
        await event.reply(f"❌ An error occurred: {e}")

@admin_only
async def view_blacklist_handler(event):
    """Views all words currently in the blacklist."""
    with get_session() as session:
        blacklist = get_blacklist(session, max_age_seconds=0) # Force refresh for viewing
    
    if not blacklist:
        return await event.reply("📭 The blacklist is currently empty.")
    
    message = "**🚫 Blacklisted Words:**\n\n" + "\n".join(f"- `{word}`" for word in sorted(blacklist))
    await event.reply(message)


async def shutdown_handler(event):
    """Safely cancels tasks, resets jobs, and shuts down."""
    
    # --- NEW: Internal Permission Check ---
    # If the event exists, it was triggered by a user. Check their role.
    # If event is None, it was triggered by the system, so we allow it.
    if event:
        role = get_user_role(event.sender_id)
        if role != 'Owner':
            return await event.reply("❌ This is an Owner-only command.")
        await event.reply("✅ **System Shutdown Initiated...**")
    else:
        config.logger.info("System shutdown initiated by internal process.")
    
    active_tasks = list(config.main_task_per_user.values())
    for task in active_tasks:
        task.cancel()
    
    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)
    
    config.logger.info("All user tasks cancelled. Reverting 'running' jobs...")
    
    try:
        with get_session() as session:
            running_jobs = session.exec(select(Job).where(Job.status == "running")).all()
            if running_jobs:
                for job in running_jobs:
                    job.status = "queued"
                    session.add(job)
                session.commit()
                config.logger.info(f"Reverted {len(running_jobs)} jobs to 'queued'.")
            else:
                config.logger.info("No 'running' jobs found to revert.")
    except Exception as e:
        config.logger.error(f"CRITICAL: Failed to reset job statuses: {e}")

    config.logger.info("Application-level shutdown tasks complete.")

    # --- NEW: Disconnect the client to stop the script ---
    if config.client:
        config.logger.info("Disconnecting client...")
        await config.client.disconnect()

@owner_only
async def sql_query_handler(event):
    """Executes a read-only SQL query and returns the result as a CSV file."""
    query = event.text.split('/sql', 1)[1].strip()
    
    # --- CRITICAL SECURITY CHECK ---
    forbidden_keywords = ['UPDATE', 'DELETE', 'INSERT', 'DROP', 'TRUNCATE', 'ALTER', 'CREATE', 'GRANT', 'REVOKE']
    if any(keyword in query.upper() for keyword in forbidden_keywords):
        return await event.reply("❌ **Security Alert:** Only read-only `SELECT` queries are allowed.")
        
    if not query.upper().startswith('SELECT'):
        return await event.reply("❌ **Invalid Query:** This command only supports `SELECT` statements.")

    try:
        with get_session() as session:
            result = session.exec(text(query))
            rows = result.fetchall()
            keys = result.keys()

        if not rows:
            return await event.reply("✅ Query executed successfully, but returned no rows.")

        # Format results as a CSV file
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(keys)
        writer.writerows(rows)
        output.seek(0)
        
        await event.client.send_file(
            event.chat_id,
            file=output.getvalue().encode('utf-8'),
            attributes=[types.DocumentAttributeFilename(file_name='query_results.csv')],
            caption=f"✅ Query executed successfully. {len(rows)} rows returned."
        )
    except Exception as e:
        await event.reply(f"❌ **Query Failed:**\n`{e}`")

@owner_only
async def db_stats_handler(event):
    """Fetches and displays the total size of the PostgreSQL database."""
    try:
        with get_session() as session:
            query = text("SELECT pg_size_pretty(pg_database_size(current_database()));")
            size = session.exec(query).scalar_one()
        
        await event.reply(f"📊 **Database Stats**\n\n**Total Size:** `{size}`")
    except Exception as e:
        await event.reply(f"❌ **Could not fetch stats:**\n`{e}`")

@admin_only
async def all_analytics_handler(event):
    """Displays aggregated transfer stats for all users."""
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    
    with get_session() as session:
        stats = session.exec(
            select(
                JobAnalytics.user_id,
                func.count(JobAnalytics.id),
                func.sum(JobAnalytics.data_transferred_bytes)
            ).where(JobAnalytics.completed_at > thirty_days_ago)
             .group_by(JobAnalytics.user_id)
             .order_by(func.sum(JobAnalytics.data_transferred_bytes).desc())
        ).all()

    if not stats:
        return await event.reply("No analytics data found for the last 30 days.")
    
    parts = ["**📈 User Analytics (Last 30 Days)**\n"]
    for user_id, job_count, total_data in stats:
        parts.append(f"- `ID {user_id}`: **{job_count}** jobs, **{humanbytes(total_data or 0)}**")
        
    await event.reply("\n".join(parts))


@admin_only
async def priority_handler(event):
    """Changes the priority of a job in the queue."""
    try:
        _, job_id_str, priority_str = event.text.split()
        job_id, new_priority = int(job_id_str), int(priority_str)

        if not 0 <= new_priority <= 10:
            return await event.reply("⚠️ Priority must be between 0 (lowest) and 10 (highest).")

        with get_session() as session:
            job = session.get(Job, job_id)
            if not job:
                return await event.reply(f"❌ Job #{job_id} not found.")
            if job.status != 'queued':
                return await event.reply(f"❌ Job #{job_id} is already running or finished.")
            
            job.priority = new_priority
            session.add(job)
            session.commit()
        
        await event.reply(f"✅ Priority for Job #{job_id} set to **{new_priority}**.")

    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/priority <job_id> <level>` (e.g., `/priority 21 5`)")

@owner_only
async def broadcast_handler(event):
    """Sends a message to all users in the database."""
    message_to_send = event.text.split('/broadcast', 1)[1].strip()
    if not message_to_send:
        return await event.reply("⚠️ Please provide a message to broadcast. Usage: `/broadcast <message>`")

    with get_session() as session:
        all_user_ids = session.exec(select(BotUser.id)).all()
    
    await event.reply(f"📢 **Starting broadcast to {len(all_user_ids)} users...**")
    
    success_count, failed_count = 0, 0
    for user_id in all_user_ids:
        try:
            await config.client.send_message(user_id, message_to_send)
            success_count += 1
            await asyncio.sleep(0.1) # Small delay to avoid rate limits
        except Exception:
            failed_count += 1
            
    await event.reply(f"✅ **Broadcast Complete!**\n- Sent successfully to: {success_count}\n- Failed to send to: {failed_count}")

@owner_only
async def dashboard_handler(event):
    """Displays the owner analytics dashboard."""
    text, buttons = get_dashboard_menu()
    await event.reply(text, buttons=buttons)

@admin_only
async def ban_handler(event):
    """Bans a user, with an optional duration and reason."""
    try:
        parts = event.text.split()
        if len(parts) < 2:
            raise ValueError("User ID is required.")

        user_id = int(parts[1])
        duration_str = None
        reason = "No reason provided."
        
        # Check for duration and reason
        if len(parts) > 2:
            # Check if the third part is a duration (e.g., 7d, 12h)
            if parts[2][-1] in 'dhm':
                duration_str = parts[2]
                reason = " ".join(parts[3:]) if len(parts) > 3 else "No reason provided."
            else: # Assume no duration, rest is the reason
                reason = " ".join(parts[2:])
        
        expiry_date = None
        if duration_str:
            duration_val = int(duration_str[:-1])
            duration_unit = duration_str[-1].lower()
            if duration_unit == 'd': expiry_date = datetime.utcnow() + timedelta(days=duration_val)
            elif duration_unit == 'h': expiry_date = datetime.utcnow() + timedelta(hours=duration_val)
            elif duration_unit == 'm': expiry_date = datetime.utcnow() + timedelta(minutes=duration_val)

        with get_session() as session:
            user = get_or_create_user(session, user_id)
            if user.role in ['Owner', 'Admin']:
                return await event.reply("❌ Admins and Owners cannot be banned.")
            
            user.role = 'Banned'
            user.ban_expiry_date = expiry_date
            user.ban_reason = reason # <-- Save the reason
            session.add(user)
            session.commit()

        expiry_msg = f" until {expiry_date.strftime('%Y-%m-%d %H:%M')}" if expiry_date else " permanently"
        await event.reply(f"✅ User `{user_id}` has been banned{expiry_msg}.\n**Reason:** {reason}")

    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/ban <user_id> [duration] [reason]`")

@admin_only
async def unban_handler(event):
    """Unbans a user by reverting their role to 'User'."""
    try:
        _, user_id_str = event.text.split()
        user_id = int(user_id_str)
        
        with get_session() as session:
            user = session.get(BotUser, user_id)
            if not user or user.role != 'Banned':
                return await event.reply(f"⚠️ User `{user_id}` is not currently banned.")
            
            user.role = 'User'
            user.ban_expiry_date = None
            session.add(user)
            session.commit()
            
        await event.reply(f"✅ User `{user_id}` has been unbanned.")
        
    except (ValueError, IndexError):
        await event.reply("⚠️ **Invalid format.** Use: `/unban <user_id>`")

#@owner_only
#async def dashboard_handler(event):
#    """Displays the owner analytics dashboard."""
#    text, buttons = get_dashboard_menu()
#    await event.reply(text, buttons=buttons)

@owner_only
async def all_status_handler(event):
    """Displays a universal, live status of all bot activity."""
    status_text = await generate_universal_status_text()
    await event.reply(status_text)

async def generate_universal_status_text():
    """Generates the universal status message with full, correct details."""
    parts = ["**🤖 Universal Bot Status**\n"]
    active_job_count = 0
    
    # --- 1. Active Jobs ---
    if not config.user_data_cache:
        parts.append("*(No active jobs)*")
    else:
        with get_session() as session:
            for user_id, jobs in config.user_data_cache.items():
                for job_id, job_data in jobs.items():
                    status = job_data.get('status')
                    if not status or not status.get("active"):
                        continue
                    
                    active_job_count += 1
                    
                    # --- Correctly calculate all metrics from the status dict ---
                    total = status.get('total', 0)
                    processed = status.get('processed', 0)
                    pct = int((processed / total) * 100) if total > 0 else 0
                    elapsed = time.time() - status.get('batch_start_time', 0)
                    total_data_size = status.get('analytics', {}).get('total_size', 0)
                    speed = total_data_size / elapsed if elapsed > 1 else 0
                    rate = processed / elapsed if elapsed > 1 else 0
                    eta = (total - processed) / rate if rate > 0 else 0

                    job_details = session.get(Job, job_id)
                    user = session.get(BotUser, user_id)
                    
                    parts.append(f"\n---")
                    parts.append(f"**Job #{job_id}** (User: `{user_id}`)")
                    parts.append(f"`[{'█' * (pct // 10)}{'░' * (10 - pct // 10)}]` {pct}%")
                    
                    if job_details:
                        parts.append(f"┠ **Task:** {job_details.source_channel_title or 'Unknown'}")
                    parts.append(f"┠ **ETA:** {format_duration(eta)}")
                    parts.append(f"┠ **Speed:** {humanbytes(speed)}/s")
                    if user:
                        parts.append(f"┠ **User Plan:** {user.plan.capitalize()}")
                    
                    # Add the cancel command link
                    parts.append(f"┖ /admin_cancel_job_{job_id}")

    # CORRECTED: This part is now outside the loop
    parts.append(f"\n---\n**Total Active Jobs:** {active_job_count}")

    # --- 2. Queued Jobs ---
    with get_session() as session:
        queued_count = session.exec(select(func.count(Job.id)).where(Job.status == 'queued')).one()
    parts.append(f"**Queued Jobs:** {queued_count} waiting")

    # --- 3. System Stats ---
    cpu_usage = psutil.cpu_percent()
    ram_usage = psutil.virtual_memory().percent
    parts.append("\n**⌬ Bot Stats**")
    parts.append(f"┠ **CPU:** {cpu_usage}% | **RAM:** {ram_usage}%")
    
    return "\n".join(parts)

@owner_only
async def admin_cancel_job_handler(event):
    """Allows an owner to cancel any user's active job."""
    try:
        _, job_id_str = event.text.split('_', 2)[-1]
        job_id = int(job_id_str)
    except (ValueError, IndexError):
        return await event.reply("⚠️ Invalid job ID format.")

    # Find which user this job belongs to
    user_id = None
    for uid, jobs in config.user_data_cache.items():
        if job_id in jobs:
            user_id = uid
            break
            
    if not user_id:
        return await event.reply(f"❌ Job #{job_id} is not currently active.")

    # Now use the user's own cancel logic, but triggered by an admin
    # This requires refactoring the core cancel logic into a utility function.
    # For now, a simplified version:
    main_task = config.main_task_per_user.get(user_id)
    if main_task:
        main_task.cancel()
        # Also update DB status
        with get_session() as session:
            job = session.get(Job, job_id)
            if job:
                job.status = 'cancelled'
                session.commit()
        await event.reply(f"✅ Cancel signal sent to Job #{job_id} (User: {user_id}).")
    else:
        await event.reply("❌ Could not find the main task to cancel.")

@owner_only
@log_command
async def set_credits_handler(event):
    """
    Sets the credit balance for a specific user.
    This version is fully instrumented with traceable logging for all paths.
    """
    trace_id = getattr(event, 'trace_id', 'N/A')
    config.logger.info(f"TRACE [{trace_id}] - Handler 'set_credits_handler' entered.")

    try:
        parts = event.text.split()
        config.logger.info(f"TRACE [{trace_id}] - Command parts parsed: {parts}")

        if len(parts) != 3:
            config.logger.warning(f"TRACE [{trace_id}] - Validation failed: Incorrect argument count. Expected 3, got {len(parts)}.")
            return await event.reply("⚠️ **Invalid format.** Use: `/set_credits <user_id> <amount>`")

        target_user_id_str = parts[1]
        amount_str = parts[2]

        if not target_user_id_str.isdigit() or not amount_str.isdigit():
            config.logger.warning(f"TRACE [{trace_id}] - Validation failed: User ID or amount is not a valid integer.")
            return await event.reply("⚠️ **Invalid format.** User ID and amount must be whole numbers.")

        target_user_id = int(target_user_id_str)
        amount = int(amount_str)

        if amount < 0:
            config.logger.warning(f"TRACE [{trace_id}] - Validation failed: Amount {amount} is negative.")
            return await event.reply("⚠️ **Invalid format.** Amount cannot be negative.")

        config.logger.info(f"TRACE [{trace_id}] - Validation successful. Attempting to set credits for user {target_user_id} to {amount}.")

        with get_session() as session:
            user_to_update = get_or_create_user(session, target_user_id)
            user_to_update.credits = amount
            session.add(user_to_update)
            session.commit()

        success_message = f"✅ **Credits Updated!** User `ID {target_user_id}` now has **{amount}** credits."
        config.logger.info(f"TRACE [{trace_id}] - SUCCESS: Credits for {target_user_id} set to {amount}. Replying to user.")
        await send_traceable_reply(event, success_message)

    except Exception as e:
        config.logger.exception(f"TRACE [{trace_id}] - An unexpected exception occurred in set_credits_handler: {e}")
        await event.reply(f"❌ An unexpected server error occurred. Trace ID: `{trace_id}`.")


import json
from .. import config # Ensure 'config' from 'bot' is imported
# In bot/handlers/admin_commands.py
import json
from .. import config # <-- Ensure this import exists

@owner_only
@log_command
async def set_universal_credit_handler(event):
    """
    Sets the universal credit limit.
    This version has the correct object reference and is fully instrumented.
    """
    trace_id = getattr(event, 'trace_id', 'N/A')
    
    config.logger.info(f"TRACE [{trace_id}] - Handler 'set_universal_credit_handler' entered.")

    try:
        parts = event.text.split()
        if len(parts) != 2:
            config.logger.warning(f"TRACE [{trace_id}] - Validation failed: Incorrect argument count.")
            return await event.reply("⚠️ **Invalid format.** Use: `/set_universal_credit <amount>`")
            
        amount = int(parts[1])
        if amount < 0: raise ValueError("Amount cannot be negative.")

    except (ValueError, IndexError):
        config.logger.warning(f"TRACE [{trace_id}] - Validation failed: Amount is not a valid positive integer.")
        return await event.reply("⚠️ **Invalid format.** Amount must be a positive whole number.")

    # --- Core Logic ---
    try:
        # --- DEFINITIVE FIX: Use the imported 'config' module directly ---
        config.UNIVERSAL_CREDIT_LIMIT = amount
        
        with open('persistent_config.json', 'w') as f:
            json.dump({'universal_credit_limit': amount}, f)
        
        success_message = f"✅ **Universal Credit Limit Updated!** Free users will now be refilled to **{amount}** credits."
        config.logger.info(f"TRACE [{trace_id}] - SUCCESS: Universal credit limit set to {amount}. Replying to user.")
        await send_traceable_reply(event, success_message)

    except Exception as e:
        config.logger.exception(f"TRACE [{trace_id}] - CRITICAL FAILURE during file write or config setting: {e}")
        await event.reply(f"❌ **Critical Error!** Could not save setting. Trace ID: `{trace_id}`")

