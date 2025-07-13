#==== decorators.py ====

import functools

# Note the '..' which means "go up one directory" to find these files
from ..database import get_session, get_user_role, get_or_create_user
from ..utils import get_user_client
from .. import config
from telethon import Button

def owner_only(func):
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        if get_user_role(event.sender_id) == 'Owner':
            return await func(event, *args, **kwargs)
        await event.reply("❌ **Access Denied:** This command is for the Owner only.")
    return wrapper

def admin_only(func):
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        if get_user_role(event.sender_id) in ['Owner', 'Admin']:
            return await func(event, *args, **kwargs)
        await event.reply("❌ **Access Denied:** This command requires Admin privileges.")
    return wrapper

def subscriber_only(func):
    """
    Decorator to restrict a handler to users with an active 'Subscriber', 
    'Admin', or 'Owner' role. It now correctly checks for plan expiry.
    """
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        user_id = event.sender_id
        with get_session() as session:
            user = get_or_create_user(session, user_id)
            
            # Check for a valid, unexpired subscription or admin/owner role
            is_premium = (user.role == 'Subscriber' and user.expiry_date and user.expiry_date > datetime.utcnow())
            is_admin_or_owner = user.role in ['Admin', 'Owner']

            if is_premium or is_admin_or_owner:
                return await func(event, *args, **kwargs)
            else:
                await event.reply(
                    "❌ **Premium Feature**\n\nThis action requires an active subscription.",
                    buttons=[[Button.inline("➡️ View Plans", b"view_plans")]]
                )
                return
    return wrapper

def check_banned(func):
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        with get_session() as session:
            user = get_or_create_user(session, event.sender_id)

            # Check if user is banned
            if user.role == 'Banned':
                # Check if the ban is temporary and has expired
                if user.ban_expiry_date and user.ban_expiry_date < datetime.utcnow():
                    user.role = 'User' # Unban them automatically
                    user.ban_expiry_date = None
                    session.add(user)
                    session.commit()
                    # Allow the command to proceed now that they are unbanned
                    return await func(event, *args, **kwargs)
                else:
                    # User is currently banned, send message and stop
                    expiry_msg = f" until {user.ban_expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')}" if user.ban_expiry_date else " permanently"
                    await event.reply(f"❌ You are currently banned{expiry_msg}.")
                    return

        # If not banned, proceed with the original command
        return await func(event, *args, **kwargs)
    return wrapper

# In bot/handlers/decorators.py
from ..utils import generate_trace_id

def log_command(func):
    """A decorator that logs command triggers with a unique trace_id."""
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        trace_id = generate_trace_id()
        # Add trace_id to the event object itself so downstream functions can access it
        event.trace_id = trace_id 
        
        config.logger.info(
            f"TRACE [{trace_id}] - COMMAND: User {event.sender_id} triggered '{event.raw_text}'"
        )
        try:
            # Pass the trace_id to the handler
            return await func(event, *args, **kwargs)
        except Exception as e:
            config.logger.exception(
                f"TRACE [{trace_id}] - FATAL in handler for '{event.raw_text}': {e}"
            )
            await event.reply(f"❌ Server Error: Trace ID `{trace_id}`. The administrator has been notified.")
    return wrapper

# In bot/handlers/decorators.py

def log_callback(func):
    """A decorator that logs callback queries with a unique trace_id."""
    @functools.wraps(func)
    async def wrapper(event, *args, **kwargs):
        trace_id = generate_trace_id()
        event.trace_id = trace_id

        config.logger.info(
            f"TRACE [{trace_id}] - CALLBACK: User {event.sender_id} pressed button with data '{event.data.decode('utf-8')}'"
        )
        try:
            return await func(event, *args, **kwargs)
        except Exception as e:
            config.logger.exception(
                f"TRACE [{trace_id}] - FATAL in callback for '{event.data.decode('utf-8')}': {e}"
            )
            await event.answer(f"❌ Server Error: Trace ID `{trace_id}`. Please report this.", alert=True)
    return wrapper
