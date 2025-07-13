#==== models.py ====

from datetime import datetime
from typing import Optional

from datetime import datetime
from typing import List, Optional

from sqlalchemy import BigInteger, JSON, Column, ForeignKey, TEXT
from sqlmodel import Field, SQLModel

class BotUser(SQLModel, table=True):
    # --- Existing Fields ---
    id: int = Field(sa_column=Column(BigInteger, primary_key=True))
    role: str = Field(default='User', index=True)
    plan: str = Field(default='free', index=True)
    expiry_date: Optional[datetime] = None
    
    source_channel_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
    target_channel_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
    source_channel_title: Optional[str] = None
    target_channel_title: Optional[str] = None
    
    message_range: Optional[List[int]] = Field(default=None, sa_column=Column(JSON))
    rename_prefix: str = Field(default="")
    rename_suffix: str = Field(default="")
    custom_caption: str = Field(default="")
    filename_replace_rules: str = Field(default="")
    caption_replace_rules: str = Field(default="")
    stream_videos: bool = Field(default=True)
    thumbnail_for_all: bool = Field(default=False)
    thumbnail_url: Optional[str] = None
    auto_thumbnail: bool = Field(default=False)
    thumbnail_timestamp: int = Field(default=10) 
        
    # --- NEW: Plan & Limit Fields ---
    max_concurrent_jobs: int = Field(default=1)
    max_queue_limit: int = Field(default=1)
    worker_limit: int = Field(default=3) 
    max_batch_size: int = Field(default=5) 
    
    # --- NEW: Feature Flags ---
    allow_batch: bool = Field(default=False)
    allow_renaming: bool = Field(default=False)
    allow_thumbnails: bool = Field(default=False)
    allow_zip: bool = Field(default=False) 
    allow_unzip: bool = Field(default=False) 
    zip_files: bool = Field(default=False)
    unzip_files: bool = Field(default=False)
    zip_password: Optional[str] = Field(default=None)
    upload_to_gdrive: bool = Field(default=False)
    upload_to_koofr: bool = Field(default=False)
    last_login: Optional[datetime] = Field(default=None)
    ban_expiry_date: Optional[datetime] = None
    ban_reason: Optional[str] = None
    data_cap_gb: int = Field(default=5)
    credits: int = Field(default=100) 
    last_credit_refresh: Optional[datetime] = None

    # This field must be present and defined as TEXT
    gdrive_creds_json: Optional[str] = Field(default=None, sa_column=Column(TEXT))
    # --- NEW KOOFR FIELDS ---
    koofr_email: Optional[str] = None
    koofr_app_password: Optional[str] = None

    # ✅ --- NEW: Add these three permission flags ---
    allow_gdrive: bool = Field(default=False)
    allow_koofr: bool = Field(default=False)
    allow_auto_thumb: bool = Field(default=False)

class Job(SQLModel, table=True):
    __tablename__ = "job"
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # ✅ DEFINITIVE FIX: This line must use the ForeignKey object.
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("botuser.id"), index=True))
    
    # --- Snapshot of settings for this specific job ---
    source_channel_id: int = Field(sa_column=Column(BigInteger))
    target_channel_id: Optional[int] = Field(default=None, sa_column=Column(BigInteger))
    source_channel_title: Optional[str] = None
    target_channel_title: Optional[str] = None
    message_range: List[int] = Field(sa_column=Column(JSON))
    download_workers: int
    upload_workers: int
    rename_prefix: str
    rename_suffix: str
    custom_caption: str
    filename_replace_rules: str
    caption_replace_rules: str
    stream_videos: bool
    thumbnail_for_all: bool
    thumbnail_url: Optional[str] 
    auto_thumbnail: bool = Field(default=False)
    thumbnail_timestamp: int = Field(default=10)

    # --- NEW: Add Zip settings to the job snapshot ---
    zip_files: bool = Field(default=False)
    zip_password: Optional[str] = None
    unzip_files: bool = Field(default=False)
    upload_to_gdrive: bool = Field(default=False)
    upload_to_koofr: bool = Field(default=False)

    # --- Job status ---
    queued_at: datetime = Field(default_factory=datetime.utcnow)
    priority: int = Field(default=0)
    status: str = Field(default="queued", index=True)
    
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)


class FailedItem(SQLModel, table=True):
    __tablename__ = "faileditem"
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(sa_column=Column(BigInteger, ForeignKey("botuser.id"), index=True))
    
    # These should also be BIGINT to be safe
    message_id: int = Field(sa_column=Column(BigInteger))
    channel_id: int = Field(sa_column=Column(BigInteger))

    reason: str
    attempt_count: int = Field(default=1)
    failed_at: datetime = Field(default_factory=datetime.utcnow)

class BlacklistedWord(SQLModel, table=True):
    __tablename__ = "blacklistedword"
    id: Optional[int] = Field(default=None, primary_key=True)
    word: str = Field(unique=True, index=True)
    
class AuditLog(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(sa_column=Column(BigInteger))
    timestamp: datetime = Field(default_factory=datetime.utcnow, index=True)
    action: str = Field(index=True)
    details: Optional[str] = None 

class JobAnalytics(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(index=True)
    user_id: int = Field(sa_column=Column(BigInteger, index=True))
    completed_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    duration_seconds: int
    data_transferred_bytes: int
    file_count: int
