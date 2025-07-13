#==== main.py ====

# Standard library imports
import asyncio
import functools
import logging
import os
import re
import sys
import redis
from datetime import datetime, timedelta

# Third-party imports
from supabase import create_client
from sqlmodel import create_engine, select
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Local application imports
import bot.config
from bot.database import create_db_and_tables, get_session
from bot.models import BotUser, Job
from bot.handlers.admin_commands import *
from bot.handlers.settings_handler import *
from bot.handlers.user_commands import *
from bot.ui import *

from bot.handlers.decorators import log_command

# Add this new function to main.py
async def catch_all_handler(event):
    # This uses CRITICAL level to make sure it stands out.
    bot.config.logger.critical(
        f"!!!!!! CATCH-ALL HANDLER TRIGGERED BY MESSAGE: '{event.raw_text}' !!!!!!"
    )

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
bot.config.logger = logging.getLogger("bot")
logging.getLogger('telethon').setLevel(logging.WARNING)


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
        'REQUIRED_CHANNEL_USERNAME', 'PAYMENT_INFO_URL', 'OWNER_USERNAME' 
    ]
    
    optional_keys = ['OWNER_SESSION_STRING']
    
    creds = {key: source(key) for key in required_keys + optional_keys} 

    missing_keys = [key for key in required_keys if not creds.get(key)]
    if missing_keys:
        bot.config.logger.critical(f"CRITICAL: Missing required secrets in {source_name}: {missing_keys}")
        return None

    bot.config.logger.info("✅ All credentials loaded successfully.")
    return creds


def register_handlers(client: TelegramClient, command_handlers: dict):
    """Registers all bot event handlers with the client in the correct order."""
    bot.config.logger.info("Registering event handlers...")
    
    # 1. Register all /command handlers
    for cmd, handler in command_handlers.items():
        client.add_event_handler(
            handler, 
            events.NewMessage(
                pattern=re.compile(f'^{re.escape(cmd)}(?: |$)'),
                func=lambda e: e.is_private
            )
        )
    
    # 2. Register other primary event handlers
    client.add_event_handler(callback_query_handler, events.CallbackQuery())
    
    def is_private_reply(e):
        is_command = e.text and e.text.startswith('/')
        return e.is_private and not is_command

    client.add_event_handler(response_handler, events.NewMessage(func=is_private_reply))
    bot.config.logger.info("✅ All event handlers registered.")

async def main():
    """Initializes and runs the bot, handling the full application lifecycle."""
    bot.config.logger.info("🤖 Project Gamma v5 (Production) starting up...")

    creds = get_credentials()
    if not creds: return
    
    bot.config.supabase_client = create_client(creds['SUPABASE_URL'], creds['SUPABASE_SERVICE_KEY'])
    
    bot.config.engine = create_engine(
        creds['POSTGRES_DATABASE_URL'], 
        echo=False, 
        pool_recycle=300,
        pool_pre_ping=True 
    )
    
    # --- NEW: Redis Client Initialization ---
    redis_url = creds.get('UPSTASH_REDIS_URL')
    if not redis_url:
        bot.config.logger.critical("UPSTASH_REDIS_URL not found in environment. The bot cannot queue jobs.")
        return
    
    bot.config.redis_client = redis.from_url(redis_url)
    bot.config.logger.info("✅ Redis client initialized.")
    
    create_db_and_tables()
    
    try:
        with get_session() as session:
            stuck_jobs = session.exec(select(Job).where(Job.status == 'running')).all()
            if stuck_jobs:
                bot.config.logger.warning(f"Found {len(stuck_jobs)} stuck job(s). Resetting to 'queued'.")
                for job in stuck_jobs:
                    job.status = 'queued'
                session.commit()
    except Exception as e:
        bot.config.logger.error(f"Could not reset stuck jobs during startup: {e}")
    
    bot.config.logger.info("✅ Database and Supabase clients initialized.")
    
    bot.config.API_ID = int(creds['API_ID'])
    bot.config.API_HASH = creds['API_HASH']
    bot_token = creds['BOT_TOKEN']
    bot.config.OWNER_ID = int(creds['ADMIN_ID'])
    bot.config.DUMP_CHANNEL_ID = int(creds['DUMP_CHANNEL_ID']) if creds.get('DUMP_CHANNEL_ID') else None
    
    bot.config.REQUIRED_CHANNEL = creds['REQUIRED_CHANNEL_USERNAME']
    bot.config.PAYMENT_INFO_URL = creds['PAYMENT_INFO_URL']
    bot.config.OWNER_USERNAME = creds['OWNER_USERNAME']

    # --- Load persistent settings ---
    try:
        # First, check if the file is empty before trying to load it.
        if os.path.getsize('persistent_config.json') > 0:
            with open('persistent_config.json', 'r') as f:
                p_config = json.load(f)
                bot.config.UNIVERSAL_CREDIT_LIMIT = p_config.get('universal_credit_limit', 100)
        else:
            # File is empty, log a warning and use defaults.
            bot.config.logger.warning("persistent_config.json is empty. Using default values.")
            
    except FileNotFoundError:
        # This is expected on the first run.
        pass 
    except json.JSONDecodeError:
        # This handles cases where the file is corrupt.
        bot.config.logger.error("persistent_config.json is corrupt. Using default values.")

    with get_session() as session:
        owner_user = get_or_create_user(session, bot.config.OWNER_ID)
        if owner_user.role != 'Owner':
            bot.config.logger.info(f"Setting user {bot.config.OWNER_ID} as Owner.")
            owner_user.role = 'Owner'
            session.add(owner_user)
            session.commit()
    
    client = TelegramClient(StringSession(), bot.config.API_ID, bot.config.API_HASH)
    bot.config.client = client
    
    help_text = (
        "**🤖 Welcome to Project Gamma!**\n\n"
        "This bot helps you manage and process your media with a powerful set of tools.\n\n"
        "**--- Key Commands ---**\n\n"
        "🔹 **/settings** - Configure your transfers.\n"
        "🔹 **/manage** - Manage your job queue.\n"
        "🔹 **/status** - Get a live status of active jobs.\n\n"
        "**--- Account Commands ---**\n"
        "🔹 `/login` - Connect your Telegram account.\n"
        "🔹 `/myplan` - Check your subscription.\n"
        "🔹 `/logout` - Disconnect your account."
    )

    command_handlers = {
        # === User Commands ===
        '/start': log_command(start_handler),
        '/help': log_command(lambda e: e.reply(help_text, parse_mode='md')),
        '/features': log_command(features_handler),
        '/login': unified_login_handler,
        '/logout': logout_handler,
        '/check': check_handler,
        '/gdrive_login': gdrive_login_handler,
        '/gdrive_logout': gdrive_logout_handler,
        '/koofr_login': koofr_login_handler,
        '/koofr_logout': koofr_logout_handler,
        '/myplan': log_command(myplan_handler),
        '/set_credits': log_command(set_credits_handler),
        '/set_universal_credit': log_command(set_universal_credit_handler),
        '/start_queue': log_command(start_queue_handler),
        '/status': log_command(status_handler),
        '/pause_job': log_command(functools.partial(pause_resume_job_handler, action='pause')),
        '/resume_job': log_command(functools.partial(pause_resume_job_handler, action='resume')),
        '/cancel': log_command(cancel_handler),
        '/analytics': log_command(analytics_handler),
        '/clone_job': log_command(clone_job_handler),

        # === Menu Commands ===
        '/settings': log_command(settings_handler),
        '/manage': log_command(manage_handler),
        '/admin': log_command(admin_handler),
        '/dashboard': log_command(dashboard_handler),

        # === Admin & Owner Commands ===
        '/addsubscriber': log_command(add_subscriber_handler),
        '/removeuser': log_command(remove_user_handler),
        '/subscribers': log_command(subscribers_handler),
        '/add_blacklist': log_command(add_blacklist_handler),
        '/remove_blacklist': log_command(remove_blacklist_handler),
        '/view_blacklist': log_command(view_blacklist_handler),
        '/priority': log_command(priority_handler),
        '/broadcast': log_command(broadcast_handler),
        '/addadmin': log_command(add_admin_handler),
        '/removeadmin': log_command(remove_admin_handler),
        '/sql': log_command(sql_query_handler),
        '/db_stats': log_command(db_stats_handler),
        '/ban': log_command(ban_handler),
        '/unban': log_command(unban_handler),
        '/all_analytics': log_command(all_analytics_handler),
        '/all_status': log_command(all_status_handler),
        '/admin_cancel_job': log_command(admin_cancel_job_handler),
        '/shutdown': log_command(shutdown_handler),
        '/test': log_command(test_handler),
}


    register_handlers(client, command_handlers)

    try:
        try:
            await client.start(bot_token=creds['BOT_TOKEN'])
        except FloodWaitError as fwe:
            bot.config.logger.critical(f"STARTUP FAILED: Flood-limited by Telegram. Must wait {fwe.seconds} seconds.")
            return # Exit gracefully
            
        bot.config.logger.info("✅ Bot connected and online.")
        await client.send_message(bot.config.OWNER_ID, "🤖 **Project Gamma Is Online**")
        await client.run_until_disconnected()


    except Exception as e: 
        bot.config.logger.critical(f"A fatal error occurred during execution: {e}", exc_info=True)
    finally:
        await shutdown_handler(None)
        if 'client' in locals() and client.is_connected():
            await client.disconnect()
        bot.config.logger.info("🔴 Bot shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped by user interrupt.")
    except Exception as e:
        logging.exception(f"💥 FATAL ERROR IN MAIN EXECUTION LOOP: {e}")
