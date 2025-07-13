#==== database.py ====

import time
from contextlib import contextmanager
from datetime import datetime
from typing import List

from sqlmodel import Session, SQLModel, select

from . import config
from .models import BotUser, BlacklistedWord, FailedItem, AuditLog

# In bot/database.py

# ... other imports
from sqlmodel import create_engine, select, func, Session, SQLModel

# === DEFINITIVE FIX: Add the model imports ===
from .models import BotUser, FailedItem, Job, JobAnalytics 


# In bot/database.py

# Add 'select' and 'func' to the sqlmodel import line
from sqlmodel import create_engine, select, func, Session, SQLModel

# ... (the rest of your file's code)


@contextmanager
def get_session():
    """Provides a transactional scope around a series of operations."""
    with Session(config.engine) as session:
        yield session

def create_db_and_tables():
    """Initializes the database by creating all tables from the models."""
    config.logger.info("Initializing database and creating tables...")
    SQLModel.metadata.create_all(config.engine)
    config.logger.info("✅ Database setup complete.")

def get_or_create_user(session: Session, user_id: int) -> 'BotUser':
    """Fetches a user from the DB or creates one with defaults if not found."""
    user = session.get(BotUser, user_id)
    if not user:
        config.logger.info(f"User {user_id} not found in DB. Creating new record...")
        # The new BotUser model now correctly uses the defaults we set
        user = BotUser(id=user_id)
        session.add(user)
        session.commit()
        session.refresh(user)
        config.logger.info(f"New user {user_id} created successfully.")
    return user

def get_user_role(user_id: int) -> str:
    """Gets a user's role, handling expired subscriptions."""
    with get_session() as session:
        user = session.get(BotUser, user_id)
        if not user:
            return 'User'
        if user.role == 'Subscriber' and user.expiry_date and user.expiry_date < datetime.now():
            user.role = 'User'
            user.plan = 'standard'
            session.add(user)
            session.commit()
            return 'User'
        return user.role

def get_blacklist(session: Session, max_age_seconds: int = 300) -> List[str]:
    """Retrieves the blacklist from the DB, with in-memory caching."""
    now = time.time()
    # CORRECTED: Use config._blacklist_cache
    if now - config._blacklist_cache['last_updated'] < max_age_seconds:
        return config._blacklist_cache['words']
    
    config._blacklist_cache['words'] = [w.lower() for w in session.exec(select(BlacklistedWord.word)).all()]
    config._blacklist_cache['last_updated'] = now
    config.logger.info("Blacklist cache refreshed from database.")
    return config._blacklist_cache['words']

def clear_blacklist_cache():
    """Resets the blacklist cache to force a DB refresh on next call."""
    # CORRECTED: Use config._blacklist_cache
    config._blacklist_cache['last_updated'] = 0

def log_failure_to_db(user_id: int, message_id: int, channel_id: int, reason: str):
    """Logs a failed item to the database or increments the attempt count."""
    try:
        with get_session() as session:
            fail = session.exec(select(FailedItem).where(FailedItem.user_id == user_id, FailedItem.message_id == message_id)).first()
            if fail:
                fail.attempt_count += 1
                fail.reason = str(reason)[:500]
                fail.failed_at = datetime.utcnow()
                session.add(fail)
            else:
                session.add(FailedItem(user_id=user_id, message_id=message_id, channel_id=channel_id, reason=str(reason)[:500]))
            session.commit()
    except Exception as e:
        # CORRECTED: Use config.logger
        config.logger.error(f"CRITICAL: Could not log failed item to DB: {e}")

def log_audit_event(user_id: int, action: str, details: str = ""):
    """Logs a new audit event to the database."""
    try:
        with get_session() as session:
            new_log = AuditLog(user_id=user_id, action=action, details=details)
            session.add(new_log)
            session.commit()
    except Exception as e:
        config.logger.error(f"FATAL: Database audit log failed! Action: {action}, Error: {e}")

def get_overview_stats(session: Session) -> dict:
    """Fetches high-level stats for the entire bot."""
    total_users = session.exec(select(func.count(BotUser.id))).one()
    total_data = session.exec(select(func.sum(JobAnalytics.data_transferred_bytes))).scalar_one() or 0
    total_jobs = session.exec(select(func.count(JobAnalytics.id))).one()
    return {
        "total_users": total_users,
        "total_data_gb": total_data / (1024**3), 
        "total_jobs": total_jobs
    }

def get_plan_breakdown(session: Session) -> list:
    """Fetches user counts and data usage per plan."""
    # This is a more advanced query that joins and groups data.
    statement = (
        select(
            BotUser.plan,
            func.count(BotUser.id),
            func.sum(JobAnalytics.data_transferred_bytes)
        )
        .join(JobAnalytics, BotUser.id == JobAnalytics.user_id, isouter=True)
        .group_by(BotUser.plan)
        .order_by(func.count(BotUser.id).desc())
    )
    return session.exec(statement).all()

def get_recent_errors(session: Session, limit: int = 5) -> list:
    """Fetches the most common recent errors."""
    statement = (
        select(FailedItem.reason, func.count(FailedItem.id))
        .group_by(FailedItem.reason)
        .order_by(func.count(FailedItem.id).desc())
        .limit(limit)
    )
    return session.exec(statement).all()


# In bot/database.py

def get_overview_stats(session: Session) -> dict:
    """Gathers high-level statistics for the dashboard overview."""
    
    # === DEFINITIVE FIX: Calculate the total data transferred ===
    total_bytes = session.exec(select(func.sum(JobAnalytics.data_transferred_bytes))).one() or 0
    # Convert bytes to gigabytes for display
    total_data_gb = total_bytes / (1024**3)

    return {
        "total_users": session.exec(select(func.count(BotUser.id))).one(),
        "total_jobs": session.exec(select(func.count(Job.id))).one(),
        "completed_jobs": session.exec(select(func.count(Job.id)).where(Job.status == 'completed')).one(),
        "failed_jobs": session.exec(select(func.count(Job.id)).where(Job.status == 'failed')).one(),
        "total_data_gb": total_data_gb, # Add the calculated value to the dictionary
    }

def get_plan_breakdown(session: Session) -> list:
    """Gathers a breakdown of users by their subscription plan."""
    return session.exec(
        select(BotUser.plan, func.count(BotUser.id))
        .group_by(BotUser.plan)
        .order_by(func.count(BotUser.id).desc())
    ).all()


def get_recent_errors(session: Session, limit: int = 5) -> list:
    """Fetches the most recent failed items from the database."""
    # --- DEFINITIVE FIX: Use the correct 'failed_at' attribute ---
    return session.exec(
        select(FailedItem).order_by(FailedItem.failed_at.desc()).limit(limit)
    ).all()
