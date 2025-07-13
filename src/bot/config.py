#==== config.py ====

import asyncio
import logging
from collections import deque
from typing import Optional, Dict
from supabase import Client
from sqlalchemy.engine import Engine
from telethon import TelegramClient

logger: Optional[logging.Logger] = None

# --- Path & File Constants ---
SESSIONS_DIR = "/tmp/sessions"
TEMP_DOWNLOAD_DIR = "/tmp/temp_downloads"
VIDEO_EXTENSIONS = ('.mp4', '.mkv', '.mov', '.avi', '.webm')
REQUIRED_CHANNEL = "MyLearningList" 
UNIVERSAL_CREDIT_LIMIT = 100 # The default value

# --- State Constants ---
STATE_DOWNLOADING = 'downloading'
STATE_UPLOADING = 'uploading'
STATE_RUNNING = 'running'
STATE_PAUSED = 'paused'
STATE_CANCELLING = 'cancelling'

# --- Global Client Placeholders ---
engine: Optional[Engine] = None
supabase_client: Optional[Client] = None
client: Optional[TelegramClient] = None

# --- Global State & Caches ---
_blacklist_cache: Dict = {'words': [], 'last_updated': 0}
active_clients: Dict = {}
user_data_cache: Dict = {}
main_task_per_user: Dict = {}
user_live_status_tasks: Dict = {}
CONVERSATION_STATE: Dict = {}
performance_metrics: Dict = {'flood_waits': deque(maxlen=10), 'queue_fullness': deque(maxlen=50)}
login_attempt_tracker: Dict = {}
recent_failures: Dict = {}

# In bot/core.py, at the top level
GLOBAL_THUMBNAIL_CACHE = {}
THUMBNAIL_CACHE_LOCK = asyncio.Lock()

# --- Global Telegram Credentials ---
OWNER_ID: Optional[int] = None
DUMP_CHANNEL_ID: Optional[int] = None
API_ID: Optional[int] = None
API_HASH: Optional[str] = None

REQUIRED_CHANNEL: Optional[str] = None
PAYMENT_INFO_URL: Optional[str] = None
OWNER_USERNAME: Optional[str] = None
