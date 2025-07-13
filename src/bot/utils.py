#==== utils.py ====

# Standard library imports
import os
import re
import time
from typing import List, Optional
from datetime import datetime, timedelta
# Third-party imports
from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.errors.rpcerrorlist import UserNotParticipantError, ChatAdminRequiredError

from sqlmodel import select
from sqlalchemy import func

# Local application imports
from . import config
from .models import BotUser

# Corrected version
from .database import get_session
from .models import JobAnalytics


class LoginAttemptManager:
    """
    Manages user login attempts to prevent spam and rate-limit abuse.
    """
    def __init__(self, max_attempts: int = 5, cooldown_period_seconds: int = 600):
        self.max_attempts = max_attempts
        self.cooldown_period = timedelta(seconds=cooldown_period_seconds)
        self.failed_attempts: dict[int, list[datetime]] = {}

    def record_failure(self, user_id: int):
        """Records a failed login attempt for a user."""
        now = datetime.utcnow()
        if user_id not in self.failed_attempts:
            self.failed_attempts[user_id] = []
        self.failed_attempts[user_id].append(now)

    def can_attempt(self, user_id: int) -> tuple[bool, int]:
        """
        Checks if a user is allowed to attempt a login.
        Returns (can_attempt, time_to_wait_seconds).
        """
        if user_id not in self.failed_attempts:
            return True, 0

        # Filter out old attempts
        now = datetime.utcnow()
        recent_failures = [t for t in self.failed_attempts[user_id] if now - t < self.cooldown_period]
        self.failed_attempts[user_id] = recent_failures

        if len(recent_failures) >= self.max_attempts:
            time_since_first_fail = now - recent_failures[0]
            wait_time = self.cooldown_period - time_since_first_fail
            return False, int(wait_time.total_seconds())

        return True, 0

    def clear_failures(self, user_id: int):
        """Clears all recorded failures for a user upon successful login."""
        if user_id in self.failed_attempts:
            del self.failed_attempts[user_id]

# Create a single, global instance of the manager
login_manager = LoginAttemptManager()


def get_credentials():
    """
    Loads all necessary credentials from the detected environment.
    """
    source_name = ""
    if 'google.colab' in sys.modules:
        from google.colab import userdata
        bot.config.logger.info("Colab environment detected. Loading secrets.")
        source_name = "Colab Secrets"
        source = userdata.get
    else:
        from dotenv import load_dotenv
        load_dotenv()
        bot.config.logger.info("Local environment detected. Loading secrets from .env file.")
        source_name = ".env file"
        source = os.environ.get

    required_keys = [
        'API_ID', 'API_HASH', 'BOT_TOKEN', 'ADMIN_ID', 'DUMP_CHANNEL_ID',
        'SUPABASE_URL', 'SUPABASE_SERVICE_KEY', 'POSTGRES_DATABASE_URL',
        'REQUIRED_CHANNEL_USERNAME', 'PAYMENT_INFO_URL', 'OWNER_USERNAME', 
        'UPSTASH_REDIS_URL'
    ]
    
    optional_keys = ['OWNER_SESSION_STRING']
    
    creds = {key: source(key) for key in required_keys + optional_keys} 

    missing_keys = [key for key in required_keys if not creds.get(key)]
    if missing_keys:
        bot.config.logger.critical(f"CRITICAL: Missing required secrets in {source_name}: {missing_keys}")
        return None

    bot.config.logger.info("✅ All credentials loaded successfully.")
    return creds



def humanbytes(size):
    if not isinstance(size, (int, float)) or size < 0:
        return "0B"
    if size == 0:
        return "0B"
        
    power, n = 1024, 0
    power_dict = {0: "B", 1: "KB", 2: "MB", 3: "GB", 4: "TB"}
    while size >= power and n < 4:
        size /= power
        n += 1
    return f"{size:.2f} {power_dict[n]}"

def format_duration(seconds):
    if seconds is None or seconds < 0: return "N/A"
    seconds = int(seconds)
    h, m, s = seconds // 3600, (seconds % 3600) // 60, seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def apply_find_replace(text, rules_string):
    if not rules_string or not text: return text
    try:
        for rule in rules_string.split('|'):
            if not (rule := rule.strip()): continue
            if rule.lower().startswith('regex:'):
                parts = rule.split(':', 2)
                if len(parts) == 3:
                    _, find, replace = parts
                    text = re.sub(find, replace, text, flags=re.IGNORECASE)
                elif len(parts) == 2:
                    _, find = parts
                    text = re.sub(find, "", text, flags=re.IGNORECASE)
            else:
                parts = rule.split(':', 1)
                find = re.escape(parts[0].strip())
                replace = parts[1].strip() if len(parts) > 1 else ""
                text = re.sub(find, replace, text, flags=re.IGNORECASE)
    except Exception as e:
        config.logger.exception(f"Error applying rule '{rule}': {e}")
    return ' '.join(text.split())

def apply_renaming_rules(original_filename: str, settings: BotUser) -> str:
    if not original_filename: return "unnamed_file"
    name, ext = os.path.splitext(original_filename)
    name = apply_find_replace(name, settings.filename_replace_rules)
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    final_name = f"{settings.rename_prefix}{name}{settings.rename_suffix}{ext}"
    if len(final_name.encode('utf-8')) > 250:
        max_len = 250 - len(ext.encode('utf-8')) - len(settings.rename_prefix.encode('utf-8')) - len(settings.rename_suffix.encode('utf-8'))
        name = name.encode('utf-8')[:max_len].decode('utf-8', 'ignore')
        final_name = f"{settings.rename_prefix}{name}{settings.rename_suffix}{ext}"
    return final_name
    
async def get_user_client(user_id):
    """Creates a user client using the shared config."""
    user_id_str = str(user_id)
    
    # Check for an active, cached client first
    if user_id_str in config.active_clients:
        client = config.active_clients[user_id_str]
        try:
            if client.is_connected() and await client.is_user_authorized():
                return client
        except Exception:
            pass # Client is invalid, will proceed to create a new one
        
        try:
            await client.disconnect()
        except Exception:
            pass
        del config.active_clients[user_id_str]

    # --- OWNER-SPECIFIC LOGIC ---
    if user_id == config.OWNER_ID:
        # The session string is now loaded into the environment by get_credentials()
        owner_session_string = os.environ.get('OWNER_SESSION_STRING')
        if owner_session_string:
            config.logger.info(f"Attempting to create client for Owner {user_id}...")
            try:
                client = TelegramClient(StringSession(owner_session_string), config.API_ID, config.API_HASH)
                await client.connect()
                if await client.is_user_authorized():
                    config.active_clients[user_id_str] = client
                    return client
                else:
                    config.logger.warning("Owner session string from environment is invalid or expired.")
            except Exception as e:
                config.logger.error(f"Failed to create client from owner session string: {e}")

    # --- REGULAR USER LOGIC (from session file) ---
    session_file_path = os.path.join(config.SESSIONS_DIR, f"user_{user_id_str}.session_string")
    if os.path.exists(session_file_path):
        try:
            with open(session_file_path, 'r') as f:
                session_string = f.read().strip()
            client = TelegramClient(StringSession(session_string), config.API_ID, config.API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                config.active_clients[user_id_str] = client
                return client
        except Exception as e:
            config.logger.error(f"Failed to load session file for user {user_id_str}: {e}")
    
    return None


def is_blacklisted(text, blacklist):
    if not text or not blacklist: return False
    text_lower = text.lower()
    for keyword in blacklist:
        if keyword in text_lower:
            config.logger.warning(f"BLACKLIST HIT: '{keyword}' in '{text}'")
            return True
    return False


async def format_dump_caption(user_id, source_message, bot_client):
    try:
        user_entity = await bot_client.get_entity(user_id)
        mention = f"@{user_entity.username}" if user_entity.username else f"ID{user_id}"
    except:
        mention = f"ID{user_id}"
    
    link = f"https://t.me/c/{abs(source_message.chat.id)}/{source_message.id}" if hasattr(source_message.chat, 'id') and not getattr(source_message.chat, 'username', None) else f"https://t.me/{getattr(source_message.chat, 'username', '')}/{source_message.id}"
    return f"**Leech Started:**\n├ **User:** {mention} (`#ID{user_id}`)\n├ **Message:** [Link]({link})\n└ **Source:** `{getattr(source_message.chat, 'title', 'Unknown')}`"


async def format_caption(template: str, original_message, final_filename: str, settings: BotUser) -> str:
    caption = template if template else (original_message.text or "")
    if original_message.file:
        caption = caption.replace('{size}', humanbytes(original_message.file.size))
    caption = caption.replace('{filename}', final_filename or "")
    duration = "N/A"
    if original_message.document:
        for attr in original_message.document.attributes:
            if isinstance(attr, (types.DocumentAttributeVideo, types.DocumentAttributeAudio)) and attr.duration:
                duration = format_duration(attr.duration)
                break
    caption = caption.replace('{duration}', duration)
    return apply_find_replace(caption, settings.caption_replace_rules)
    
def truncate_text(text, max_length=30):
    if not text: return "🚫 Not Set"
    return f"`{text}`" if len(text) <= max_length else f"`{text[:max_length-3]}...`"
    
def format_rules(rules, max_length=40):
    if not rules: return "🚫 Not Set"
    display_rules = rules.replace('|', ' → ')
    return f"`{display_rules}`" if len(display_rules) <= max_length else f"`{display_rules[:max_length-3]}...`"

def parse_telegram_message_link(link: str):
    """
    Parses a Telegram message link and returns (channel_id, message_id).
    Handles both regular links, supergroup topic links, and public channel links.
    """
    # Remove query parameters like ?single, ?comment=123, etc.
    clean_link = link.split('?')[0]
    
    # Pattern for private channels: t.me/c/1234567890/123 or t.me/c/1234567890/456/123 (with topic)
    private_pattern = re.compile(r"t\.me/c/(\d+)(?:/\d+)?/(\d+)$")
    private_match = private_pattern.search(clean_link)
    
    if private_match:
        channel_part, message_id = private_match.groups()
        message_id = int(message_id)
        channel_id = int(f"-100{channel_part}")
        return channel_id, message_id
    
    # Pattern for public channels: t.me/channelname/123
    public_pattern = re.compile(r"t\.me/([a-zA-Z0-9_]{5,})/(\d+)$")
    public_match = public_pattern.search(clean_link)
    
    if public_match:
        channel_username, message_id = public_match.groups()
        message_id = int(message_id)
        # For public channels, we return the username as-is
        # Telethon can handle usernames directly
        return channel_username, message_id
    
    return None, None

async def check_channel_membership(event):
    """
    Checks if the event sender is in the required channel defined in the config.
    """
    sender = await event.get_sender()
    try:
        # Uses the channel username from your config
        await event.client.get_permissions(config.REQUIRED_CHANNEL, sender)
        return True
    except UserNotParticipantError:
        join_button = Button.url("Join Channel", f"https://t.me/{config.REQUIRED_CHANNEL}")
        check_button = Button.inline("I Have Joined, Check Again", b"check_join")
        await event.reply(
            "**Access Requirement**\nTo use this bot, you must first join our channel.",
            buttons=[[join_button], [check_button]]
        )
        return False
    except Exception as e:
        config.logger.error(f"Could not check channel membership: {e}")
        await event.reply("Sorry, I couldn't verify channel membership at this time.")
        return False

# In bot/utils.py
def check_and_deduct_credits(user: BotUser, cost: int) -> bool:
    """
    Checks if a user has enough credits, refills them if needed, and deducts the cost.
    """
    if user.plan == 'free':
        if not user.last_credit_refresh or (datetime.utcnow() - user.last_credit_refresh > timedelta(hours=4)):
            # CORRECTED: Use the global config variable instead of a hardcoded value
            user.credits = config.UNIVERSAL_CREDIT_LIMIT
            user.last_credit_refresh = datetime.utcnow()
            config.logger.info(f"Refilled credits for free user {user.id} to {config.UNIVERSAL_CREDIT_LIMIT}")

    if user.credits >= cost:
        user.credits -= cost
        return True
    else:
        # User does not have enough credits
        return False

def get_job_preview(user_id: int, message_count: int) -> dict:
    """Estimates job duration based on user's historical performance."""
    with get_session() as session:
        # Get the user's average time per file from past jobs
        stats = session.exec(
            select(
                func.sum(JobAnalytics.duration_seconds),
                func.sum(JobAnalytics.file_count)
            ).where(JobAnalytics.user_id == user_id)
        ).first()

        total_duration, total_files = stats or (0, 0)
        
        # Use a default of 5 seconds/file if no history exists
        avg_time_per_file = (total_duration / total_files) if total_files else 5
        
    estimated_seconds = int(avg_time_per_file * message_count)
    
    return {
        "file_count": message_count,
        "estimated_duration": format_duration(estimated_seconds)
    }

# In bot/utils.py
import uuid

def generate_trace_id():
    """Generates a unique identifier for a single request trace."""
    return str(uuid.uuid4()).split('-')[0]


async def send_traceable_reply(event, message, buttons=None, parse_mode='md'):
    """A centralized reply function that includes traceable logging."""
    trace_id = getattr(event, 'trace_id', 'N/A')
    config.logger.info(f"TRACE [{trace_id}] - REPLYING with: '{message.splitlines()[0]}...'")
    try:
        return await event.reply(message, buttons=buttons, parse_mode=parse_mode)
    except Exception as e:
        config.logger.error(f"TRACE [{trace_id}] - Failed to send reply: {e}")

async def send_traceable_edit(event, message, buttons=None, parse_mode='md'):
    """A centralized edit function that includes traceable logging."""
    trace_id = getattr(event, 'trace_id', 'N/A')
    config.logger.info(f"TRACE [{trace_id}] - EDITING with: '{message.splitlines()[0]}...'")
    try:
        return await event.edit(message, buttons=buttons, parse_mode=parse_mode)
    except Exception as e:
        config.logger.error(f"TRACE [{trace_id}] - Failed to edit message: {e}")
