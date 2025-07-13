#==== user_commands.py ====

# Standard library imports
import asyncio
import functools
import os
import re
import shutil
import time
import secrets
import urllib.parse
from datetime import datetime, timedelta

# Third-party imports
from sqlmodel import delete, func, select

from google_auth_oauthlib.flow import Flow

from telethon import Button, TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.custom import Message
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.errors import ChannelPrivateError
from telethon.errors.rpcerrorlist import (
    ChatAdminRequiredError,
    FloodWaitError,
    MessageNotModifiedError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
    UserNotParticipantError,
)

# Local application imports
from .. import config
from ..core import process_queue_for_user
from ..database import (
    get_or_create_user,
    get_session,
    get_user_role,
    log_audit_event,
)
from ..models import BotUser, FailedItem, Job, JobAnalytics

from ..utils import (
    check_channel_membership,
    format_duration,
    get_job_preview,
    get_user_client,
    humanbytes,
    login_manager,
    parse_telegram_message_link,
)

from ..ui import get_manage_menu, get_main_settings_menu

from .decorators import (
    admin_only,
    check_banned,
    owner_only,
    subscriber_only,
)

# In bot/handlers/user_commands.py

# ... (other imports)
from .decorators import check_banned, subscriber_only, check_if_logged_in # <-- ADD IT HERE
# ...


# In bot/handlers/user_commands.py

# Standard library imports
import asyncio
import functools
import re
import os
from datetime import datetime, timedelta

# Third-party imports
from google_auth_oauthlib.flow import Flow
from sqlmodel import func, select
from telethon import Button
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon import TelegramClient

# Local application imports
from .. import config
from ..core import process_queue_for_user
from ..database import get_or_create_user, get_session
from ..models import BotUser, Job
from ..utils import (check_channel_membership, get_user_client, login_manager,
                     send_traceable_reply, send_traceable_edit)

# --- DEFINITIVE FIX: Import all required decorators ---
from .decorators import check_banned, check_if_logged_in, log_command, owner_only, subscriber_only





# ==============================================================================
# Login Rate Limiting Class
# ==============================================================================

class LoginAttemptManager:
    """A class to manage login attempts and enforce rate limits."""
    def __init__(self, max_attempts=3, cooldown_period_mins=30):
        self.attempts = {}
        self.max_attempts = max_attempts
        self.cooldown_period_seconds = cooldown_period_mins * 60

    def can_attempt(self, user_id):
        now = time.time()
        if user_id in self.attempts:
            self.attempts[user_id] = [t for t in self.attempts[user_id] if now - t < self.cooldown_period_seconds]
        
        if len(self.attempts.get(user_id, [])) >= self.max_attempts:
            last_attempt_time = self.attempts[user_id][-1]
            wait_time = self.cooldown_period_seconds - (now - last_attempt_time)
            return False, max(0, wait_time)
        return True, 0

    def record_failure(self, user_id):
        if user_id not in self.attempts:
            self.attempts[user_id] = []
        self.attempts[user_id].append(time.time())

    def clear_failures(self, user_id):
        if user_id in self.attempts:
            del self.attempts[user_id]

# Create a single global instance for the bot to use
login_manager = LoginAttemptManager()

# ==============================================================================
# Command Handlers
# ==============================================================================
@check_banned
async def start_handler(event):
    """
    Handles the /start command, providing a rich, personalized welcome message.
    """
    user_id = event.sender_id
    sender = await event.get_sender()
    pfp_path = None
    
    try:
        # Fetch user's role from our database
        role = get_user_role(user_id)
        
        # Build the detailed welcome message
        welcome_text = [
            f"**Hello there, [{sender.first_name}](tg://user?id={user_id})!**\n",
            f"Welcome to the **UnLock Vault Bot** 🚀\n",
            "Here are your details:",
            f"  - **┌ ID:** `{sender.id}`",
            f"  - **├ Name:** {sender.first_name or ''} {sender.last_name or ''}",
            f"  - **├ Username:** @{sender.username or 'Not set'}",
            f"  - **└ Bot Role:** `{role.capitalize()}`\n",
            "Type `/help` to see the main commands or use `/settings` to get started."
        ]
        
        # Try to download the user's profile photo
        pfp_path = await event.client.download_profile_photo(sender, file=f"/tmp/{user_id}_pfp.jpg")
        
        if pfp_path:
            await event.client.send_file(event.chat_id, file=pfp_path, caption="\n".join(welcome_text), parse_mode='md')
        else:
            # Fallback to a text-only message if no profile photo
            await event.reply("\n".join(welcome_text), parse_mode='md')

    except Exception as e:
        config.logger.error(f"Error in start_handler for user {user_id}: {e}")
        await event.reply("Welcome! Something went wrong while fetching your details, but the bot is online. Use `/help` for commands.")
    finally:
        # Clean up the downloaded photo
        if pfp_path and os.path.exists(pfp_path):
            os.remove(pfp_path)

@check_banned
async def features_handler(event):
    """Provides an up-to-date overview of the bot's features."""
    features_text = (
        "✨ **Project Gamma Feature List** ✨\n\n"
        "Here's what this bot can do:\n\n"
        "--- \n\n"
        "**🚀 Core Engine**\n"
        "  - **Concurrent Processing**: Handles multiple jobs in parallel based on your plan.\n"
        "  - **Persistent & Prioritized Queue**: Jobs are saved to a database and can be prioritized.\n"
        "  - **Live Status Updates**: Use `/status` for a real-time progress report.\n\n"
        "**🧠 Intelligent Input**\n"
        "  - **Link Parsing**: Simply paste a message link to set your source or range.\n"
        "  - **Flexible Destination**: Send files to a channel, directly to you via PM, or to your Saved Messages.\n\n"
        "**🎨 Total Customization (Premium Features)**\n"
        "  - **Powerful Renaming**: Add prefixes/suffixes and use find-and-replace rules (including Regex).\n"
        "  - **Dynamic Captions**: Create custom caption templates with placeholders.\n"
        "  - **Custom Thumbnails**: Apply a personal thumbnail to files.\n"
        "  - **Zip & Unzip**: Automatically compress files into password-protected archives or extract them upon arrival.\n\n"
        "--- \n\n"
        "➡️ **Ready to get started?**\n"
        "Use `/login` to connect your account and `/settings` to configure your first transfer!"
    )
    await event.reply(features_text, parse_mode='md', link_preview=False)
    

@check_banned
async def login_handler(event):
    """
    The final, production-grade login handler that correctly checks for
    existing sessions before initiating a new flow.
    """
    user_id = event.sender_id
    temp_client = None
        
    # --- Check 1: Is user ALREADY logged in (from file or cache)? ---
    # We use the same robust utility as the /check command.
    existing_client = await get_user_client(user_id)
    if existing_client and await existing_client.is_user_authorized():
        return await event.reply("✅ You're already logged in and ready to go!")

    # --- Check 2: Rate Limiting for NEW login attempts ---
    can_attempt, wait_time = login_manager.can_attempt(user_id)
    if not can_attempt:
        wait_minutes = int(wait_time / 60)
        return await event.reply(
            f"⏳ **Rate Limited**\n\nToo many failed attempts. Please wait **{wait_minutes} minutes**."
        )
    
    # --- Check 3: Channel membership verification ---
    if not await check_channel_membership(event):
        return

    # --- Begin Conversation ---
    config.CONVERSATION_STATE[user_id] = {'step': 'login_in_progress'}
    sender = await event.get_sender()
    
    # --- If all checks pass, begin the login conversation ---
    config.CONVERSATION_STATE[user_id] = {'step': 'login_in_progress'}
    sender = await event.get_sender() 

    try:
        # --- Step 5: Welcome and instruction message ---
        welcome_msg = (
            "🔐 **Welcome to Secure Login Process**\n\n"
            "I'll guide you through each step to ensure a smooth login experience.\n"
            "This process is completely secure and your credentials are never stored.\n\n"
            "**What you'll need:**\n"
            "• Your phone number with country code\n"
            "• Access to your Telegram account for verification\n"
            "• Two-factor password (if enabled)\n\n"
            "**Ready to begin?** ✨"
        )
        await event.reply(welcome_msg)
        
        # --- Main Login Conversation ---
        temp_client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
        await temp_client.connect()
        
        async with event.client.conversation(event.chat_id, timeout=300) as conv:
            # --- Step 1: Phone Number Collection ---
            phone_instruction = (
                "📱 *Step 1: Enter Your Phone Number*\n\n"
                "Please send your phone number with the country code\\.\n"
                "You can also copy your phone number directly from Telegram settings:\n"
                "🔗 **[Open Settings](tg://settings)**\n\n"
                "*Examples:*\n"
                "• `+1234567890` \\(USA\\)\n"
                "• `+44123456789` \\(UK\\)\n"
                "• `+91123456789` \\(India\\)\n\n"
                "*⚠️ Important:*\n"
                "• Make sure to include the `+` symbol and the correct country code\\.\n"
                "• Do *NOT* send without country code — it will be rejected\\.\n\n"
                "❌ Type `/cancel` anytime to stop the login process."
            )
            await conv.send_message(phone_instruction)
            phone_response = await conv.get_response()
            
            if not phone_response or not phone_response.text:
                return await conv.send_message("❌ Login cancelled. No phone number received.")
            
            if phone_response.text.strip().lower() == '/cancel':
                return await conv.send_message("❌ Login cancelled by user.")
            
            phone_number = phone_response.text.strip()
            
            # Validate phone number format
            if not phone_number.startswith('+') or len(phone_number) < 8:
                return await conv.send_message(
                    "❌ **Invalid phone number format**\n\n"
                    "Please make sure your phone number:\n"
                    "• Starts with `+`\n"
                    "• Includes country code\n"
                    "• Has at least 7 digits\n\n"
                    "Try `/login` again with the correct format."
                )
            
            # --- Step 2: Sending Verification Code ---
            status_msg = await conv.send_message(
                f"📤 **Step 2: Sending Verification Code**\n\n"
                f"Sending code to: `{phone_number}`\n"
                f"Please wait a moment... ⏳"
            )
            
            try:
                code_request = await temp_client.send_code_request(phone_number)
            except Exception as e:
                return await status_msg.edit(
                    f"❌ **Failed to send verification code**\n\n"
                    f"Error: `{str(e)}`\n\n"
                    f"Please check your phone number and try `/login` again."
                )
            
            # --- Step 3: Code Input Instructions ---
            code_instruction = (
                "✅ *Step 2 Complete: Code Sent Successfully\\!*\\n\\n"
                "🔐 *Step 3: Enter Verification Code*\\n\\n"
                "📩 *Check your Telegram messages* for a 5\\-digit verification code\\.\\n"
                "🔗 **[Click here to get the code](https://t.me/+42777)** and copy it\\.\\n\\n"
                "*How to enter the code correctly:*\\n"
                "• You *MUST* send the verification code with spaces *between each digit*\\.\\n"
                "• Example: If the code is `12345`, send it as `1 2 3 4 5`\\n"
                "• ❗ Only the spaced format will work\\.\\n\\n"
                "*Didn’t receive the code?*\\n"
                "• Wait 1–2 minutes and check again\\n"
                "• Check your other Telegram sessions\\/devices\\n"
                "• Type `/cancel` to restart the process\\n\\n"
                "📥 *Please send your verification code now:*"
)
            await status_msg.edit(code_instruction)
            code_response = await conv.get_response()
            
            if not code_response or not code_response.text:
                return await conv.send_message("❌ Login cancelled. No verification code received.")
            
            if code_response.text.strip().lower() == '/cancel':
                return await conv.send_message("❌ Login cancelled by user.")

            # --- Step 4: Code Processing ---
            processing_msg = await conv.send_message("🔄 **Verifying your code...** Please wait.")
            
            # Automatic code formatting and validation
            code_digits = re.sub(r'[^0-9]', '', code_response.text.strip())
            
            if len(code_digits) != 5:
                return await processing_msg.edit(
        "❌ *Invalid verification code*\n\n"
        "The code should be exactly *5 digits* and sent *with spaces between each digit*\\.\\n\\n"
        "Did you enter the code *with proper spacing*\\?\\n"
        "➡️ Example: `1 2 3 4 5`\\n\\n"
        "🔁 Please try again and send the code in the correct format\\.\n\n"
        "You can also type `/login` to restart the process\\."
    )
                        
            formatted_code = " ".join(list(code_digits))

            try:
                await temp_client.sign_in(phone_number, formatted_code, phone_code_hash=code_request.phone_code_hash)
                await processing_msg.edit("✅ **Verification successful!** Setting up your session...")
                
            except SessionPasswordNeededError:
                # --- Step 5: Two-Factor Authentication ---
                two_fa_instruction = (
                    "🔒 **Step 4: Two-Factor Authentication**\n\n"
                    "Your account has additional security enabled.\n"
                    "Please enter your **Two-Factor Authentication password**.\n\n"
                    "**Security Notes:**\n"
                    "• Your password is transmitted securely\n"
                    "• It's never stored or logged\n"
                    "• Type `/cancel` to stop if needed\n\n"
                    "**Please enter your 2FA password:**"
                )
                
                await processing_msg.edit(two_fa_instruction)
                pw_response = await conv.get_response()
                
                if not pw_response or not pw_response.text:
                    return await conv.send_message("❌ Login cancelled. No 2FA password received.")
                
                if pw_response.text.strip().lower() == '/cancel':
                    return await conv.send_message("❌ Login cancelled by user.")
                
                final_processing = await conv.send_message("🔄 **Verifying 2FA password...** Almost done!")
                
                try:
                    await temp_client.sign_in(password=pw_response.text.strip())
                    await final_processing.edit("✅ **2FA verification successful!** Finalizing login...")
                except Exception as e:
                    return await final_processing.edit(
                        f"❌ **2FA verification failed**\n\n"
                        f"The password appears to be incorrect.\n"
                        f"Please try `/login` again with the correct password."
                    )
            except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
                error_message = (
                    "❌ **Verification Code Error**\n\n"
                    "The verification code you entered is either:\n"
                    "• Incorrect\n"
                    "• Expired (codes expire after 5 minutes)\n\n"
                    "**What to do next:**\n"
                    "• Try `/login` again to get a new code\n"
                    "• Make sure to enter the code quickly\n"
                    "• Check all your Telegram sessions for the code"
                )
                await event.reply(error_message)
                return

            # --- Step 6: Session Setup and Success ---
            session_string = temp_client.session.save()
            session_file_path = os.path.join(config.SESSIONS_DIR, f"user_{user_id}.session_string")
            os.makedirs(config.SESSIONS_DIR, exist_ok=True)
            
            with open(session_file_path, 'w') as f:
                f.write(session_string)
            
            config.logger.info(f"💾 Session saved for user {user_id} at: {session_file_path}")
            
            config.active_clients[str(user_id)] = temp_client
            config.logger.info(f"✅ Client for user {user_id} is now active.")
            
            # --- Update Database Record on Success ---
            with get_session() as session:
                user = get_or_create_user(session, user_id)
                user.last_login = datetime.utcnow()
                session.add(user)
                session.commit()
            
            # --- Clear Failed Attempts on Success ---
            login_manager.clear_failures(user_id)
            
            me = await temp_client.get_me()
            sender_name = getattr(sender, 'first_name', 'User')
            
            
            success_message = (
                f"👋 *Hello there,* [{sender.first_name}](tg://user\\?id={user_id})\\!\\n\\n"
                f"🎉 *Login Successful\\!*\\n\\n"
                f"*Your account is now connected\\:*\\n"
                f"├─ *Phone*: `{phone_number}`\\n"
                f"├─ *Name*: `{me.first_name or 'N/A'} {me.last_name or ''}`\\n"
                f"├─ *Username*: @{me.username or 'NotSet'}\\n"
                f"├─ *Language*: `{me.language_code or 'Unknown'}`\\n"
                f"├─ *DC ID*: `{getattr(me, 'dc_id', 'N/A')}`\\n"
                f"└─ *Premium User*: `{getattr(me, 'is_premium', False)}`\\n\\n"

                f"✨ *You can now use all bot features\\!*\\n"
                f"Type `/help` to see available commands\\."
            )

            await conv.send_message("🎉 **Login Successful!**")
            config.logger.info(f"🎉 LOGIN SUCCESS: User {user_id}")
            temp_client = None  # Prevent cleanup in finally block

    except Exception as e:
        # --- DEFINITIVE FIX: Unified Error Handling ---
        # This single block now handles all possible exceptions correctly.
        login_manager.record_failure(user_id) # Corrected: Only pass user_id
        config.logger.error(f"❌ LOGIN FAILED for user {user_id}: {e}", exc_info=True)
        error_message = (
            "❌ **Login Error**\n\n"
            "An unexpected error occurred during the login process.\n\n"
            "**What you can try:**\n"
            "• Wait a few minutes and try `/login` again\n"
            "• Make sure your internet connection is stable\n"
            "• Contact support if the problem persists\n\n"
            f"**Error details:** `{str(e)}`"
        )
        
        # Try to send error via conversation, fall back to event.reply
        try:
            await conv.send_message(error_message)
        except (AttributeError, TypeError): # conv might not exist if conversation failed to start
            await event.reply(error_message)
        
    finally:
        # --- Definitive Cleanup ---
        if user_id in config.CONVERSATION_STATE:
            del config.CONVERSATION_STATE[user_id]
        if temp_client and temp_client.is_connected():
            await temp_client.disconnect()

@check_banned       
async def logout_handler(event):
    user_id = str(event.sender_id)
    if user_id in config.active_clients:
        try:
            await config.active_clients[user_id].disconnect()
        except: pass
        del config.active_clients[user_id]

    session_file = os.path.join(config.SESSIONS_DIR, f"user_{user_id}.session_string")
    if os.path.exists(session_file):
        os.remove(session_file)
    await event.reply("✅ You have been successfully logged out.")

@check_banned
def track_login_failure(user_id, error_type):
    """Track login failures to implement progressive delays."""
    # CORRECTED: Use config.recent_failures
    if user_id not in config.recent_failures:
        config.recent_failures[user_id] = []
    
    config.recent_failures[user_id].append({
        'time': time.time(),
        'error': error_type
    })
    
    cutoff = time.time() - 86400
    config.recent_failures[user_id] = [
        f for f in config.recent_failures[user_id] if f['time'] > cutoff
    ]

@check_banned
async def check_handler(event):
    """Checks and reports the user's login status for both Telegram and Google Drive."""
    user_id = event.sender_id
    await event.reply("🔎 Checking your login status...")
    
    # 1. Check Telegram login status
    tg_client = await get_user_client(user_id)
    tg_status = "✅ Logged In" if tg_client and await tg_client.is_user_authorized() else "❌ Not Logged In"

    # 2. Check Google Drive login status
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        gdrive_status = "✅ Connected" if user.gdrive_creds_json else "❌ Not Connected"

    # 3. Build the final message
    status_message = (
        f"**🔎 Your Login Status**\n\n"
        f"**📲 Telegram Account:** {tg_status}\n"
        f"**☁️ Google Drive:** {gdrive_status}\n\n"
        f"Use `/login` or `/gdrive_login` to connect."
    )
    
    await event.reply(status_message)

@check_banned
async def myplan_handler(event):
    """Displays the user's current plan and credit balance."""
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)

    # Case 1: Handle Owner and Admin roles
    if user.role in ['Owner', 'Admin']:
        await event.reply(f"👑 **Your Plan:** You are an **{user.role}**.\nYour access does not expire.")
    
    # Case 2: Handle active Subscribers
    elif user.role == 'Subscriber' and user.expiry_date and user.expiry_date > datetime.utcnow():
        remaining = user.expiry_date - datetime.utcnow()
        plan = user.plan.capitalize()
        await event.reply(
            f"👤 **Your Plan:** You are a **{plan} Subscriber**.\n"
            f"**Time Remaining:** {remaining.days} days, {remaining.seconds // 3600} hours."
        )
    
    # Case 3: Handle everyone else (Free users, expired subscribers)
    else:
        buttons = [[Button.inline("➡️ Browse Subscription Plans", b"view_plans")]]
        
        # Handle expired subscribers
        if user.role == 'Subscriber':
             await event.reply("❌ **Your subscription has expired.**\nPlease select a new plan to continue.", buttons=buttons)
        
        # ✅ NEW: Handle Free Users and show their credits
        else:
             credit_info = f"**Credits Remaining:** {user.credits} / 100"
             reply_text = (
                 f"ℹ️ You are on the **Free Plan**.\n\n"
                 f"{credit_info}\n"
                 f"*(Credits refill every 4 hours)*"
             )
             await event.reply(reply_text, buttons=buttons) 

# In bot/handlers/user_commands.py

@check_if_logged_in
@log_command
async def start_queue_handler(event):
    """
    Pushes all 'queued' jobs for the user into the main Redis job queue.
    """
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        # Find all jobs for this user that are ready to go
        jobs_to_start = session.exec(
            select(Job).where(Job.user_id == user_id, Job.status == 'queued')
        ).all()

        if not jobs_to_start:
            return await event.reply("✅ Your queue is empty. Nothing to start.")

        # --- REFACTORED LOGIC ---
        # Instead of creating local tasks, push job IDs to the Redis list
        for job in jobs_to_start:
            job.status = 'in_queue' # New status to prevent re-queuing
            session.add(job)
            bot.config.redis_client.lpush('job_queue', job.id)
        
        session.commit()
        
    await event.reply(f"✅ **Sent {len(jobs_to_start)} job(s) to the processing queue.**\n\n"
                      f"You can now safely close Termux/Colab. The workers will process the jobs in the background.")

    
async def queue_batch_handler(event):
    """
    Validates user settings and shows a pre-flight summary for confirmation.
    """
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)

        # --- Step 1: Perform all validation checks ---
        if not all([user.source_channel_id, user.message_range]):
            return await event.reply("⚠️ **Config Incomplete!** Please set a Source and Range in /settings.")

        if user.zip_files and not user.zip_password:
            return await event.reply("❌ **Zip Password Missing!** Please set a password in Zip Settings.")

        queued_count = session.exec(select(func.count(Job.id)).where(Job.user_id == user_id, Job.status == "queued")).one()
        if queued_count >= user.max_queue_limit and user.role != 'Owner':
            return await event.reply(f"❌ **Queue Full!** Your plan allows for {user.max_queue_limit} queued job(s).")
    
    # --- Step 2: Show the preview ---
    msg_ids = list(range(user.message_range[0], user.message_range[1] + 1))
    preview = get_job_preview(user_id, len(msg_ids))

    # Set a state to validate the confirmation click
    config.CONVERSATION_STATE[user_id] = {'step': 'confirm_queue_action'}
    
    text = (
        f"**✅ Job Preview & Confirmation**\n\n"
        f"**Source:** `{user.source_channel_title or user.source_channel_id}`\n"
        f"**Destination:** {'GDrive' if user.upload_to_gdrive else 'Koofr' if user.upload_to_koofr else 'Telegram'}\n"
        f"**Files to Process:** {preview['file_count']:,}\n"
        f"**Estimated Time:** ~{preview['estimated_duration']}\n\n"
        "**Add this job to the queue?**"
    )
    buttons = [
        [Button.inline("✅ Yes, Add to Queue", b"confirm_queue_yes")],
        [Button.inline("❌ No, Cancel", b"close_menu")]
    ]
    
    # This must be event.edit() if it's from a button, or event.reply() if from a command.
    # Assuming it's a button press from the main settings menu.
    await event.edit(text, buttons=buttons)

async def view_queue_handler(event, edit=False):
    """Displays the user's job queue, showing channel titles correctly."""
    user_id = event.sender_id
    with get_session() as session:
        queued_jobs = session.exec(
            select(Job)
            .where(Job.user_id == user_id, Job.status == "queued")
            .order_by(Job.priority.desc(), Job.queued_at.asc())
        ).all()

    if not queued_jobs:
        # Use answer for callbacks, reply for commands
        return await event.answer("📭 Your queue is empty.", alert=True) if edit else await event.reply("📭 Your queue is empty.")
    
    parts = ["**📑 Your Pending Transfer Queue**\n"]
    buttons = []
    for i, job in enumerate(queued_jobs, 1):
        prio_icon = "🔥" * job.priority
        s, e = job.message_range
        
        # --- CORRECTED LOGIC ---
        # Use the stored titles, fall back to ID if title is not available
        source_title = job.source_channel_title or job.source_channel_id
        if job.target_channel_id is None:
            dest_title = "Bot PM (Default)"
        elif job.target_channel_id == 777:
            dest_title = "Saved Messages"
        else:
            dest_title = job.target_channel_title or job.target_channel_id
        
        parts.append(f"\n**{i}. Job #{job.id}** {prio_icon}\n  📥 `{source_title}`\n  ➡️ `{dest_title}`\n  📊 Range: `{s}`-`{e}`")
        buttons.append([Button.inline(f"Bump Prio {prio_icon}⬆️", f"q_bump:{job.id}")])

    action = event.edit if edit else event.reply
    await action("\n".join(parts), buttons=buttons)

async def clear_queue_handler(event):
    user_id = event.sender_id
    if config.main_task_per_user.get(user_id) and not config.main_task_per_user[user_id].done():
        return await event.reply("⚠️ Cannot clear queue while a transfer is active. Use `/cancel` first.")

    with get_session() as session:
        jobs_to_delete = session.exec(
            select(Job).where(Job.user_id == user_id, Job.status == "queued")
        ).all()
        
        count = len(jobs_to_delete)
        if count == 0:
            return await event.reply("✅ Your queue is already empty.")

        for job in jobs_to_delete:
            session.delete(job)
        
        session.commit()

    log_audit_event(user_id, "QUEUE_CLEARED", f"Removed {count} jobs.")
    await event.reply(f"✅ Your queue has been cleared. Removed {count} job(s).")
    
async def status_handler(event):
    """Displays a live status message for active transfers."""
    user_id = event.sender_id
    
    # CORRECTED: Use the config prefix
    if config.user_live_status_tasks.get(user_id) and not config.user_live_status_tasks[user_id].done():
        return await event.reply("ℹ️ A live status message is already active for you.")
        
    status_msg = await event.reply(await generate_status_text(user_id), parse_mode='md')
    
    # CORRECTED: Use the config prefix
    if config.main_task_per_user.get(user_id) and not config.main_task_per_user[user_id].done():
        task = asyncio.create_task(update_live_status(event.client, user_id, event.chat_id, status_msg.id))
        config.user_live_status_tasks[user_id] = task

async def generate_status_text(user_id: int):
    """
    Generates a detailed and compact status message for all currently implemented features.
    """
    user_jobs_cache = config.user_data_cache.get(user_id, {})

    with get_session() as session:
        user = get_or_create_user(session, user_id)
        queued_jobs = session.exec(
            select(Job).where(Job.user_id == user_id, Job.status == "queued")
            .order_by(Job.priority.desc(), Job.queued_at.asc())
        ).all()
        queued_jobs_count = len(queued_jobs)
        recent_failures = session.exec(
            select(func.count(FailedItem.id)).where(
                FailedItem.user_id == user_id,
                FailedItem.failed_at > datetime.utcnow() - timedelta(hours=1)
            )
        ).one()

    # --- IDLE STATE ---
    if not user_jobs_cache:
        # CORRECTED: The initial definition of idle_text was restored.
        idle_text = [
            f"💤 **Bot Status: IDLE**",
            f"⚙️ **Workers Ready:** {user.worker_limit} (DL/UL)",
            ""
        ]
        
        if queued_jobs:
            next_job = queued_jobs[0]
            start, end = next_job.message_range
            count = end - start + 1
            idle_text.extend([
                f"📋 **Queue Status:** {queued_jobs_count} job(s) waiting",
                f"🎯 **Next Job:** #{next_job.id}",
                f"  └─ From: {next_job.source_channel_title or f'ID:{next_job.source_channel_id}'}",
                ""
            ])
            idle_text.append("🚀 Use `/start_queue` to begin processing")
        else:
            idle_text.extend([
                "📭 **Queue:** Empty",
                "➕ Use `/queue_batch` to add transfers"
            ])
        
        if recent_failures > 0:
            idle_text.extend([
                "",
                f"⚠️ {recent_failures} failures in last hour. Use `/failed` to review."
            ])
        
        return "\n".join(idle_text)

    # --- ACTIVE STATE ---
    active_jobs_count = len([j for j in user_jobs_cache.values() if j.get('status', {}).get("active")])
    final_parts = [
        f"🚀 **Bot Status: ACTIVE** ({active_jobs_count} concurrent job(s))",
        f"📋 **Queue:** +{queued_jobs_count} job(s) waiting",
        ""
    ]
    
    with get_session() as session:
        job_counter = 0
        for job_id, job_data in user_jobs_cache.items():
            user_status = job_data.get('status', {})
            if not user_status.get("active"):
                continue

            current_job = session.get(Job, job_id)
            if not current_job:
                continue

            job_counter += 1
            
            # --- Enhanced Job Details Section ---
            state = user_status.get('state', 'running').upper()
            
            # State indicators with modern emojis
            state_icons = {
                'RUNNING': '🚀',
                'PAUSED': '⏸️',
                'CANCELLING': '🛑',
                'DOWNLOADING': '📥',
                'UPLOADING': '📤'
            }
            state_icon = state_icons.get(state, '❓')
            
            # Core metrics
            total = user_status.get('total', 0)
            processed = user_status.get('processed', 0)
            uploaded = user_status.get('uploaded', 0)
            skipped = user_status.get('skipped', 0)
            failed_ids = user_status.get('failed_ids', [])
            failed_count = len(failed_ids)
            
            # Time and performance calculations
            start_time = user_status.get('batch_start_time', time.time())
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 1 else 0
            remaining = total - processed
            eta = remaining / rate if rate > 0 else 0
            
            # Enhanced progress calculations
            progress_pct = int((processed / total) * 100) if total > 0 else 0
            upload_pct = int((uploaded / total) * 100) if total > 0 else 0
            
            # Modern progress bar with upload indication
            progress_bar = "█" * (progress_pct // 5) + "▓" * ((upload_pct - progress_pct) // 5) + "░" * (20 - upload_pct // 5)
            
            # Analytics data
            analytics = user_status.get('analytics', {})
            total_data_size = analytics.get('total_size', 0)
            avg_file_size = analytics.get('avg_file_size', 0)
            
            # Worker information
            gov_dl = current_job.download_workers if current_job else user.worker_limit
            gov_ul = current_job.upload_workers if current_job else user.worker_limit
            
            job_block = [
                f"{'═' * 50}",
                f"{state_icon} **JOB #{job_id}** `{state}`"
            ]
            
            # Job details section
            if current_job:
                job_start, job_end = current_job.message_range
                job_block.extend([
                    f"🎯 **Job Details:**",
                    f"  ├ 🆔 Job #{current_job.id} (Priority: {current_job.priority})",
                    f"  ├ 📥 From: {current_job.source_channel_title or f'ID:{current_job.source_channel_id}'}",
                    f"  ├ 📤 To: {current_job.target_channel_title or f'ID:{current_job.target_channel_id}'}",
                    f"  └ 📊 Range: {job_start:,} → {job_end:,}",
                    ""
                ])
            
            # Enhanced progress section
            job_block.extend([
                f"📈 **Progress Overview:**",
                f"  `[{progress_bar}]` {progress_pct}%",
                f"  ├ 🔄 Processed: {processed:,}/{total:,}",
                f"  ├ ✅ Uploaded: {uploaded:,} ({upload_pct}%)",
                f"  ├ ⏭️ Skipped: {skipped:,}",
                f"  └ ❌ Failed: {failed_count:,}",
                ""
            ])
            
            # Enhanced performance metrics
            job_block.extend([
                f"⚡ **Performance Metrics:**",
                f"  ├ 🚀 Speed: {rate:.2f} msg/s",
                f"  ├ ⏱️ Elapsed: {format_duration(elapsed)}",
                f"  ├ 🕐 ETA: {format_duration(eta)}",
                f"  ├ 💾 Total Data: {humanbytes(total_data_size)}",
                f"  ├ 📦 Avg File: {humanbytes(avg_file_size)}",
                f"  └ 👥 Workers: 📥{gov_dl}/📤{gov_ul}",
                ""
            ])
            
            # Enhanced active downloads section
            downloading = user_status.get(config.STATE_DOWNLOADING, {})
            if downloading:
                job_block.append(f"📥 **Active Downloads** ({len(downloading)}):")
                for i, (file_id, item) in enumerate(list(downloading.items())[:3]):
                    filename = item.get('filename', 'Unknown')[:25]
                    current_size = item.get('current', 0)
                    total_size = item.get('total', 0)
                    percentage = item.get('percentage', 0)
                    speed = item.get('speed', 0)
                    eta_item = item.get('eta', 0)
                    
                    # Mini progress bar for each file
                    file_prog = "█" * (percentage // 10) + "░" * (10 - percentage // 10)
                    
                    job_block.extend([
                        f"  {i+1}. `{filename}...`",
                        f"     [{file_prog}] {percentage}%",
                        f"     {humanbytes(current_size)}/{humanbytes(total_size)} @ {humanbytes(speed)}/s"
                    ])
                
                if len(downloading) > 3:
                    job_block.append(f"     ... and {len(downloading)-3} more")
                job_block.append("")

            # Enhanced active uploads section
            uploading = user_status.get(config.STATE_UPLOADING, {})
            if uploading:
                job_block.append(f"📤 **Active Uploads** ({len(uploading)}):")
                for i, (upload_id, item) in enumerate(list(uploading.items())[:3]):
                    filename = item.get('filename', 'Unknown')[:25]
                    current_size = item.get('current', 0)
                    total_size = item.get('total', 0)
                    percentage = item.get('percentage', 0)
                    speed = item.get('speed', 0)
                    
                    # Mini progress bar
                    file_prog = "█" * (percentage // 10) + "░" * (10 - percentage // 10)
                    
                    job_block.extend([
                        f"  {i+1}. `{filename}...`",
                        f"     [{file_prog}] {percentage}%",
                        f"     {humanbytes(current_size)}/{humanbytes(total_size)} @ {humanbytes(speed)}/s"
                    ])
                
                if len(uploading) > 3:
                    job_block.append(f"     ... and {len(uploading)-3} more")
                job_block.append("")
            
            final_parts.extend(job_block)
            
    # --- FOOTER SECTION ---
    footer_parts = ["", "═" * 25]
    if recent_failures > 0:
        footer_parts.append(f"⚠️ {recent_failures} failures in last hour. Use `/failed`.")
    if queued_jobs:
        footer_parts.append(f"📋 **Up Next:** Job #{queued_jobs[0].id}")
    
    footer_parts.append("🎮 **Controls:** `/pause`, `/resume`, `/cancel`")
    footer_parts.append("📊 **Info:** `/failed`, `/view_queue`")
    
    final_parts.extend(footer_parts)
    return "\n".join(final_parts)

async def update_live_status(client, user_id, chat_id, msg_id):
    """The helper function that provides live updates for a status message."""
    update_count = 0
    
    # CORRECTED: Use the config prefix
    while user_id in config.main_task_per_user and not config.main_task_per_user[user_id].done():
        try:
            status_text = await generate_status_text(user_id)
            
            update_count += 1
            heartbeat = "💚" if update_count % 2 == 0 else "🤍"
            status_text += f"\n\n{heartbeat} `Update #{update_count} • {datetime.utcnow().strftime('%H:%M:%S')}`"
            
            await client.edit_message(chat_id, msg_id, status_text, parse_mode='md')
            await asyncio.sleep(5) # Consistent 5-second updates for simplicity
            
        except MessageNotModifiedError:
            await asyncio.sleep(5)
        except (asyncio.CancelledError, ConnectionError):
            break
        except Exception as e:
            config.logger.warning(f"Live status update for user {user_id} failed: {e}")
            await asyncio.sleep(20)
    
    # Final status update
    try:
        final_text = await generate_status_text(user_id)
        final_text += f"\n\n✅ `Process Finished • {datetime.utcnow().strftime('%H:%M:%S')}`"
        
        # CORRECTED: Use the config prefix
        if user_id in config.user_live_status_tasks:
            del config.user_live_status_tasks[user_id]
        
        await client.edit_message(chat_id, msg_id, final_text, parse_mode='md', buttons=None)
    except Exception as e:
        config.logger.error(f"Final status update failed for user {user_id}: {e}")

async def pause_resume_job_handler(event, action):
    """Pauses or resumes a specific active job for a user."""
    user_id = event.sender_id
    try:
        _, job_id_str = event.text.split()
        job_id = int(job_id_str)
    except (ValueError, IndexError):
        return await event.reply(f"⚠️ **Invalid format.** Use: `/{action}_job <job_id>`")

    job_cache = config.user_data_cache.get(user_id, {}).get(job_id)
    if not job_cache:
        return await event.reply(f"⚠️ Job #{job_id} is not active or does not exist.")

    user_status = job_cache.get('status', {})
    pause_event = job_cache.get('pause_event')

    if action == 'pause' and user_status.get('state') == config.STATE_RUNNING:
        pause_event.clear()
        user_status['state'] = config.STATE_PAUSED
        await event.reply(f"⏸️ **Job #{job_id} is now paused.**")
    elif action == 'resume' and user_status.get('state') == config.STATE_PAUSED:
        pause_event.set()
        user_status['state'] = config.STATE_RUNNING
        await event.reply(f"▶️ **Job #{job_id} has been resumed.**")
    else:
        await event.reply(f"⚠️ Job #{job_id} is not in a state to be {action}d.")

@subscriber_only
async def clone_job_handler(event):
    """Clones an existing job's configuration into a new queued job."""
    user_id = event.sender_id
    try:
        _, job_id_to_clone_str = event.text.split()
        job_id_to_clone = int(job_id_to_clone_str)
    except (ValueError, IndexError):
        return await event.reply("⚠️ **Invalid format.** Use: `/clone_job <job_id>`")

    with get_session() as session:
        original_job = session.get(Job, job_id_to_clone)
        if not original_job or original_job.user_id != user_id:
            return await event.reply(f"❌ Job #{job_id_to_clone} not found.")

        # Create a new Job by copying settings from the original
        job_data = original_job.model_dump(exclude={'id', 'queued_at', 'status', 'priority'})
        cloned_job = Job.model_validate(job_data)
        cloned_job.status = 'queued'
        cloned_job.priority = 1 # Set a default priority
        
        session.add(cloned_job)
        session.commit()
        session.refresh(cloned_job)

    await event.reply(f"✅ **Job Cloned!**\nNew Job ID: #{cloned_job.id} has been added to your queue.")

async def pause_resume_handler(event, action):
    """Pauses or resumes a specific user's transfer."""
    user_id = event.sender_id
    
    # CORRECTED: Use config prefix for all cache access
    user_cache = config.user_data_cache.get(user_id, {})
    
    # This logic now needs to iterate through potential concurrent jobs
    # For simplicity, this will pause/resume the FIRST active job it finds.
    # A more advanced `/pause <job_id>` command could be a future feature.
    active_job_id = next((job_id for job_id, data in user_cache.items() if data.get('status', {}).get('active')), None)

    if not active_job_id:
        return await event.reply("⚠️ You have no active transfer to control.")

    job_cache = user_cache.get(active_job_id, {})
    user_status = job_cache.get('status', {})
    pause_event = job_cache.get('pause_event')

    if action == 'pause' and user_status.get('state') == config.STATE_RUNNING:
        pause_event.clear()
        user_status['state'] = config.STATE_PAUSED
        await event.reply(f"⏸️ **Job #{active_job_id} is now paused.** Use `/resume` to continue.")
    elif action == 'resume' and user_status.get('state') == config.STATE_PAUSED:
        pause_event.set()
        user_status['state'] = config.STATE_RUNNING
        await event.reply(f"▶️ **Job #{active_job_id} has been resumed.**")
    else:
        await event.reply(f"⚠️ Job #{active_job_id} is not in a state to be {action}d.")

async def cancel_handler(event):
    """Cancels all of a user's running transfers."""
    user_id = event.sender_id
    main_task = config.main_task_per_user.get(user_id)
    
    if not main_task or main_task.done():
        return await event.reply("⚠️ You have no active transfers to cancel.")

    try:
        with get_session() as session:
            # CORRECTED: Find and update ALL running jobs for the user
            running_jobs = session.exec(
                select(Job).where(Job.user_id == user_id, Job.status == "running")
            ).all()

            for job in running_jobs:
                job.status = "cancelled"
                session.add(job)
            
            if running_jobs:
                session.commit()
                log_audit_event(user_id, "JOBS_CANCELLED", f"Cancelled {len(running_jobs)} job(s).")
    except Exception as e:
        config.logger.error(f"Failed to update cancelled job statuses in DB for user {user_id}: {e}")

    # Cancel the main asyncio tasks
    if user_id in config.user_live_status_tasks:
        config.user_live_status_tasks[user_id].cancel()
    main_task.cancel()
        
    await event.reply(f"❌ **All active transfers have been cancelled.**")

async def failed_log_handler(event):
    """Displays the user's failed items log."""
    user_id = event.sender_id
    with get_session() as session:
        failed_items = session.exec(select(FailedItem).where(FailedItem.user_id == user_id).order_by(FailedItem.failed_at.desc())).all()

    if not failed_items:
        return await event.reply("✅ Your failed items log is empty.")

    total_failures = len(failed_items)
    
    # Always build the preview message for the 5 most recent items
    preview_parts = [f"**📋 Your Failed Items Log ({total_failures} total)**\n*Showing up to 5 most recent.*"]
    for item in failed_items[:5]:
        fail_time = item.failed_at.strftime('%Y-%m-%d %H:%M')
        reason_preview = item.reason[:100] + '...' if len(item.reason) > 100 else item.reason
        preview_parts.append(f"\n- ID `{item.message_id}` (Attempts: {item.attempt_count})\n  Reason: `{reason_preview}`")
    
    preview_parts.append("\n\nUse `/retry` to re-queue all failed items, or `/failed clear` to erase this log.")
    preview_text = "\n".join(preview_parts)
    
    if total_failures > 5:
        # Create the full log file
        log_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"failed_log_{user_id}.txt")
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"Complete Failed Items Report for User ID: {user_id} ({total_failures} items)\n\n")
            for item in failed_items:
                f.write(f"Message ID: {item.message_id}\nChannel ID: {item.channel_id}\n")
                f.write(f"Last Failed: {item.failed_at.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Attempts: {item.attempt_count}\nReason: {item.reason}\n-----------------\n")
        
        # 1. Send the preview text as a standalone message.
        preview_message = await event.reply(preview_text)
        
        # 2. Send the full log file as a reply to the preview message.
        await event.client.send_file(
            event.chat_id,
            log_path,
            reply_to=preview_message.id,
            caption=f"Full log of all {total_failures} failed items."
        )
        os.remove(log_path)
    else:
        # For 5 or fewer failures, just send the message
        await event.reply(preview_text)

@subscriber_only
async def retry_handler(event):
    """Re-queues failed items from the database."""
    user_id = event.sender_id
    with get_session() as session:
        failed_items = session.exec(select(FailedItem).where(FailedItem.user_id == user_id)).all()
        if not failed_items:
            return await event.answer("✅ Your failed items log is empty.", alert=True)
            
        user = get_or_create_user(session, user_id)
        
        jobs_to_create = {}
        for item in failed_items:
            if item.channel_id not in jobs_to_create:
                jobs_to_create[item.channel_id] = []
            jobs_to_create[item.channel_id].append(item.message_id)
        
        new_job_count = 0
        for channel_id, ids in jobs_to_create.items():
            # CORRECTED: Manually create the Job object from the user's current settings
            new_job = Job(
                user_id=user.id,
                source_channel_id=channel_id,
                target_channel_id=user.target_channel_id,
                message_range=[min(ids), max(ids)],
                download_workers=user.worker_limit,
                upload_workers=user.worker_limit,
                priority=5, # Give retries high priority
                # Copy the rest of the settings
                rename_prefix=user.rename_prefix,
                rename_suffix=user.rename_suffix,
                custom_caption=user.custom_caption,
                filename_replace_rules=user.filename_replace_rules,
                caption_replace_rules=user.caption_replace_rules,
                stream_videos=user.stream_videos,
                thumbnail_for_all=user.thumbnail_for_all,
                thumbnail_url=user.thumbnail_url,
                zip_files=user.zip_files,
                unzip_files=user.unzip_files,
                zip_password=user.zip_password
            )
            session.add(new_job)
            new_job_count += 1
       
        session.exec(delete(FailedItem).where(FailedItem.user_id == user_id))
        session.commit()

    log_audit_event(user_id, "RETRY_ALL", f"Re-queued {len(failed_items)} items into {new_job_count} new jobs.")
    await event.reply(f"✅ **Re-Queued:** Moved {len(failed_items)} items into {new_job_count} new high-priority job(s).")
    
    # CORRECTED: Use the config prefix
    if not config.main_task_per_user.get(user_id) or config.main_task_per_user[user_id].done():
        await event.reply("Use `/start_queue` to begin processing.")

async def analytics_handler(event):
    """Displays a user's personal transfer stats for the last 30 days."""
    user_id = event.sender_id
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    
    with get_session() as session:
        stats = session.exec(
            select(
                func.count(JobAnalytics.id),
                func.sum(JobAnalytics.data_transferred_bytes),
                func.sum(JobAnalytics.file_count)
            ).where(
                JobAnalytics.user_id == user_id,
                JobAnalytics.completed_at > thirty_days_ago
            )
        ).first()

    # Correctly handle the case where stats might be (None, None, None)
    total_jobs = stats[0] if stats and stats[0] is not None else 0
    total_data = stats[1] if stats and stats[1] is not None else 0
    total_files = stats[2] if stats and stats[2] is not None else 0
    
    message = (
        f"📊 **Your Analytics (Last 30 Days)**\n\n"
        f"**Jobs Completed:** {total_jobs:,}\n"
        f"**Files Transferred:** {total_files:,}\n"
        f"**Data Transferred:** {humanbytes(total_data)}"
    )
    await event.reply(message)

@subscriber_only
async def gdrive_login_handler(event):
    """Starts the Google Drive OAuth 2.0 login flow, with a check for existing sessions."""
    user_id = event.sender_id
    
    # --- NEW: Check if user is already connected ---
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        if user.gdrive_creds_json:
            return await event.reply(
                "✅ You are already connected to a Google Drive account.\n"
                "Use `/gdrive_logout` to disconnect it first."
            )
    
    # Complete rclone client configuration with actual client_secret
    rclone_client_config = {
        "installed": {
            "client_id": "202264815644.apps.googleusercontent.com",
            "client_secret": "X4Z3ca8xfWDb1Voo-F9a7ZxJ",  # rclone's actual client secret
            "project_id": "rclone-project-113303646427",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost:1"]
        }
    }
     
    scopes = ['https://www.googleapis.com/auth/drive']
    
    flow = Flow.from_client_config(
        rclone_client_config, 
        scopes=scopes, 
        redirect_uri='http://localhost:1'
    )
   
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')

    text = (
        "**☁️ Connect Google Drive (via rclone)**\n\n"
        "1. Click the link below to authorize.\n"
        "2. Grant permission to **'rclone'**.\n"
        "3. Copy the authorization code from the URL in your browser.\n"
        "4. Send just the code back to me.\n\n"
        f"➡️ [Authorize on Google]({auth_url})"
    )
    
    config.CONVERSATION_STATE[user_id] = {'step': 'gdrive_code', 'flow': flow}
    await event.reply(text, link_preview=False)

@subscriber_only
async def gdrive_logout_handler(event):
    """Disconnects a user's Google Drive account."""
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        
        if not user.gdrive_creds_json:
            return await event.reply("⚠️ You are not connected to a Google Drive account.")
            
        # Clear the stored credentials
        user.gdrive_creds_json = None
        
        # Also reset their destination if it was set to GDrive
        if user.upload_to_gdrive:
            user.upload_to_gdrive = False
            
        session.add(user)
        session.commit()
        
    await event.reply("✅ Your Google Drive account has been successfully disconnected.")

@check_banned
async def unified_login_handler(event):
    """
    A single, smart handler for all login, logout, and status check actions.
    """
    user_id = event.sender_id
    
    # 1. Get status for all services
    tg_client = await get_user_client(user_id)
    tg_status = "✅ Logged In" if tg_client and await tg_client.is_user_authorized() else "❌ Not Logged In"
    
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        gdrive_status = "✅ Connected" if user.gdrive_creds_json else "❌ Not Connected"
        # --- NEW: Check Koofr Status ---
        koofr_status = "✅ Connected" if user.koofr_email else "❌ Not Connected"

    # 2. Build the dynamic message
    text = (
        f"**📲 Account & Connection Manager**\n\n"
        f"Here is your current connection status:\n\n"
        f"**Telegram Account:** {tg_status}\n"
        f"**Google Drive:** {gdrive_status}\n"
        f"**Koofr:** {koofr_status}\n\n" # <-- NEW
        f"Use the buttons below to manage your connections."
    )

    # 3. Build the dynamic buttons
    buttons = []
    # Telegram
    buttons.append([Button.inline("🚪 Logout from Telegram", b"logout_tg")] if tg_client else [Button.inline("📲 Login to Telegram", b"login_tg")])
    # Google Drive
    buttons.append([Button.inline("☁️ Logout from Google Drive", b"logout_gdrive")] if user.gdrive_creds_json else [Button.inline("☁️ Login to Google Drive", b"login_gdrive")])
    # --- NEW: Koofr Button ---
    buttons.append([Button.inline("🗄️ Logout from Koofr", b"logout_koofr")] if user.koofr_email else [Button.inline("🗄️ Login to Koofr", b"login_koofr")])
    
    buttons.append([Button.inline("Done", b"close_menu")])

    await event.reply(text, buttons=buttons)

@subscriber_only
async def koofr_login_handler(event):
    """Starts the Koofr login conversation."""
    user_id = event.sender_id
    # Set the first step of the conversation
    config.CONVERSATION_STATE[user_id] = {'step': 'koofr_email'}
    await event.reply(
        "**🗄️ Connect Koofr Account**\n\n"
        "Please send your Koofr account email address."
    )

@subscriber_only
async def koofr_logout_handler(event):
    """Logs the user out of their Koofr account by clearing credentials."""
    user_id = event.sender_id
    with get_session() as session:
        user = get_or_create_user(session, user_id)
        user.koofr_email = None
        user.koofr_app_password = None
        session.commit()
    await event.reply("✅ **Koofr account has been disconnected.**")
