#==== core.py ====

from typing import Optional
import asyncio
import os
import shutil
import time
import functools
import py7zr
import httpx
import subprocess
from datetime import datetime, timedelta
from sqlmodel import select, func
from telethon import TelegramClient
from telethon.tl import types
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential

# Local application imports
from . import config
from .database import get_session, get_blacklist, log_failure_to_db, log_audit_event, get_or_create_user
from .models import Job, BotUser
from .utils import apply_renaming_rules, format_caption, format_dump_caption, humanbytes, is_blacklisted, format_duration, check_and_deduct_credits

# --- DEFINITIVE IMPORT FIX ---
from .models import Job, BotUser, JobAnalytics
from .utils import (apply_renaming_rules, format_caption, format_dump_caption, humanbytes, 
                    is_blacklisted, format_duration, check_and_deduct_credits, get_user_client)



async def progress_callback(user_status, lock, file_id, description, current, total):
    async with lock:
        if user_status.get('state') != config.STATE_RUNNING or file_id not in user_status.get(description, {}):
            return
            
        now = time.time()
        status = user_status[description][file_id]
        
        if now - status.get('last_update', 0) < 1.0 and current < total:
            return
            
        status['last_update'] = now
        
        if 'start_time' not in status:
            status['start_time'] = now
            
        elapsed = now - status['start_time']
        if elapsed > 1:
            speed = current / elapsed
            status.update({'speed': speed, 'eta': (total - current) / speed if speed > 0 else 0})
        
        status.update({'current': current, 'total': total, 'percentage': int((current / total) * 100)})
        
        if 'analytics' not in user_status:
            user_status['analytics'] = {'total_size': 0, 'avg_file_size': 0, 'file_count': 0, 'types': {}}
        
        analytics = user_status['analytics']
        if current == total:
            analytics['total_size'] += total
            analytics['file_count'] += 1
            analytics['avg_file_size'] = analytics['total_size'] / analytics['file_count']
            
            
async def run_job_worker(client: "TelegramClient", bot_client: "TelegramClient", job_id: int):
    """
    The definitive job orchestrator, with the final fix for the DetachedInstanceError
    by managing a persistent session scope.
    """
    # Create a temporary directory for this job's downloads
    temp_dir = os.path.join(config.TEMP_DOWNLOAD_DIR, f"job_{job_id}")
    if os.path.exists(temp_dir): 
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    
    # --- BUG FIX: The entire worker logic must be inside the session scope ---
    with get_session() as session:
        job = session.get(Job, job_id)
        if not job:
            config.logger.error(f"Job #{job_id} could not be found.")
            return

        user_id = job.user_id
        job.status = "running"
        job.started_at = datetime.utcnow()
        session.add(job)
        session.commit()
        session.refresh(job) # Ensure all attributes are loaded

        # --- Setup for this specific job ---
        user_status = {
            "active": True, "total": len(range(job.message_range[0], job.message_range[1] + 1)),
            "batch_start_time": time.time(), "state": config.STATE_RUNNING,
            "processed": 0, "uploaded": 0, "skipped": 0, "failed_ids": [],
            config.STATE_DOWNLOADING: {}, config.STATE_UPLOADING: {}
        }
        lock = asyncio.Lock()
        pause_event = asyncio.Event()
        pause_event.set()
        
        upload_queue = asyncio.Queue(maxsize=job.upload_workers * 2)
        download_semaphore = asyncio.Semaphore(job.download_workers)
        msg_ids = range(job.message_range[0], job.message_range[1] + 1)

        producer_tasks, consumer_tasks = [], []
        job_failed = False

        try:
            user_client = await get_user_client(user_id)
            if not user_client: 
                raise ConnectionError("Could not get user client for job worker.")

            dump_channel_id_to_use = config.DUMP_CHANNEL_ID

            # The 'job' object is now safe to pass because the session is still open.
            producer_tasks = [
                asyncio.create_task(producer(upload_queue, download_semaphore, mid, user_client, job, user_id, user_status, lock, pause_event, temp_dir)) 
                for mid in msg_ids
            ]
            consumer_tasks = [
                asyncio.create_task(consumer(upload_queue, i, user_client, user_id, user_status, lock, pause_event, bot_client, dump_channel_id_to_use)) 
                for i in range(job.upload_workers)
            ]

            await asyncio.gather(*producer_tasks)
            await upload_queue.join()

        except Exception as e:
            config.logger.error(f"[Job #{job_id}] A critical error occurred in the worker: {e}", exc_info=True)
            job_failed = True
        finally:
            for _ in consumer_tasks: 
                await upload_queue.put(None)
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

            # --- Final update happens within the same, still-open session ---
            if job_failed or len(user_status['failed_ids']) > 0:
                job.status = 'failed'
            else:
                job.status = 'completed'
            
            job.completed_at = datetime.utcnow()
            session.add(job)
            
            analytics = JobAnalytics(
                user_id=user_id, 
                job_id=job_id, 
                file_count=user_status['processed'],
                data_transferred_bytes=0,
                duration_seconds=(job.completed_at - job.started_at).total_seconds(), 
                status=job.status
            )
            session.add(analytics)
            
            # The final commit happens here, before the 'with' block closes the session.
            session.commit()
            
            config.logger.info(f"✅ Job #{job_id} finished with status '{job.status}' and analytics saved.")

    # --- Final Cleanup (outside the session) ---
    if os.path.exists(temp_dir): 
        shutil.rmtree(temp_dir)


async def process_queue_for_user(user_id: int, client: TelegramClient, bot_client: TelegramClient):
    """The definitive persistent queue processor. This function is correct and requires no changes."""
    if user_id in config.main_task_per_user and not config.main_task_per_user[user_id].done():
        config.logger.warning(f"Queue processor for user {user_id} is already running.")
        return

    config.logger.info(f"✅ Queue processor started for user {user_id}.")
    
    async def processor_loop():
        while True:
            try:
                with get_session() as session:
                    user = get_or_create_user(session, user_id)
                    running_count = session.exec(select(func.count(Job.id)).where(Job.user_id == user_id, Job.status == "running")).one()
                    
                    if running_count < user.max_concurrent_jobs:
                        job_to_start = session.exec(select(Job).where(Job.user_id == user_id, Job.status == "queued").order_by(Job.priority.desc(), Job.queued_at.asc()).limit(1)).first()
                        if job_to_start:
                            # Mark job as running immediately to prevent race conditions
                            job_to_start.status = "running"
                            session.add(job_to_start)
                            session.commit()
                            # Start the definitive job worker
                            asyncio.create_task(run_job_worker(client, bot_client, job_to_start.id))
                await asyncio.sleep(10) # Poll for new jobs every 10 seconds
            except Exception as e:
                config.logger.error(f"Error in queue processor for user {user_id}: {e}", exc_info=True)
                await asyncio.sleep(30) # Wait longer on error
    
    config.main_task_per_user[user_id] = asyncio.create_task(processor_loop())


async def producer(queue, semaphore, msg_id, client, job: Job, user_id, user_status, lock, pause_event, temp_dir: str):
    start_time = time.time()
    file_id = f"dl_{msg_id}"
    log_prefix = f"[Job {job.id}][Msg {msg_id}]"

    config.logger.info(f"{log_prefix} PRODUCER: Starting processing.")
    
    async with semaphore:
        try:
            # --- Message Fetching & Validation ---
            config.logger.debug(f"{log_prefix} PRODUCER: Attempting to fetch message.")
            message = await client.get_messages(job.source_channel_id, ids=msg_id)
            
            if not message or not (message.media or message.text):
                config.logger.warning(f"{log_prefix} PRODUCER: Invalid or empty message. Skipping.")
                async with lock: user_status["skipped"] += 1
                return
            
            config.logger.info(f"{log_prefix} PRODUCER: Message fetched. Preparing for processing.")

            # --- NEW: Credit Check ---
            with get_session() as session:
                user_for_credits = session.get(BotUser, user_id)
                # Let's say each file costs 1 credit to process
                if not check_and_deduct_credits(user_for_credits, cost=1):
                    # Silently skip if out of credits, or you can notify the user
                    config.logger.warning(f"User {user_id} is out of credits. Skipping message {msg_id}.")
                    return
                session.commit() # Save the new credit balance
            # --- End Credit Check ---

            # --- File/Text Preparation ---
            filename = apply_renaming_rules(getattr(message.file, 'name', None) or f"text_{msg_id}.txt", job)
            final_caption = await format_caption(job.custom_caption, message, filename, job)
            
            # --- Blacklist Check ---
            with get_session() as session:
                if is_blacklisted(filename, get_blacklist(session)) or is_blacklisted(final_caption, get_blacklist(session)):
                    config.logger.warning(f"{log_prefix} PRODUCER: Blacklist hit for '{filename}'. Skipping.")
                    async with lock: user_status["skipped"] += 1
                    return

            # --- Download/Queue Logic ---
            async with lock:
                user_status[config.STATE_DOWNLOADING][file_id] = {'filename': filename,'start_time': time.time(),'total': getattr(message.file, 'size', 0)}

            if message.media:
                config.logger.info(f"{log_prefix} PRODUCER: Starting download for '{filename}'.")
                downloaded_path = await client.download_media(message, file=os.path.join(temp_dir, filename), 
                                                               progress_callback=functools.partial(progress_callback, user_status, lock, file_id, config.STATE_DOWNLOADING))
                if not downloaded_path: raise IOError("Download failed - no path returned.")
                
                # Zipping Logic
                if job.zip_files and job.zip_password:
                    archive_path = f"{downloaded_path}.7z"
                    config.logger.info(f"Zipping file for Job #{job.id}: {archive_path}")
                    with py7zr.SevenZipFile(archive_path, 'w', password=job.zip_password) as archive:
                        archive.write(downloaded_path, arcname=os.path.basename(downloaded_path))
                    os.remove(downloaded_path)
                    final_path_for_queue = archive_path
                    final_filename_for_queue = f"{filename}.7z"
                else:
                    final_path_for_queue = downloaded_path
                    final_filename_for_queue = filename
                
                await queue.put((final_path_for_queue, message, final_filename_for_queue, final_caption, job))
                config.logger.info(f"{log_prefix} PRODUCER: Added '{final_filename_for_queue}' to processing queue.")
            
            elif message.text:
                await queue.put(("text_only", message, None, final_caption, job))
                config.logger.info(f"{log_prefix} PRODUCER: Added text-only message to queue.")

        except Exception as e:
            config.logger.exception(f"{log_prefix} PRODUCER: CRITICAL ERROR: {e}")
            async with lock: user_status["failed_ids"].append(msg_id)
            log_failure_to_db(user_id, msg_id, job.source_channel_id, str(e))
        finally:
            async with lock:
                if file_id in user_status.get(config.STATE_DOWNLOADING, {}):
                    del user_status[config.STATE_DOWNLOADING][file_id]
                user_status["processed"] += 1
            config.logger.info(f"{log_prefix} PRODUCER: Finished processing in {time.time() - start_time:.2f}s.")


# Global cache for thumbnails per job (outside the consumer function)
GLOBAL_THUMBNAIL_CACHE = {}
THUMBNAIL_CACHE_LOCK = asyncio.Lock()


async def consumer(queue, worker_id, client, user_id, user_status, lock, pause_event, bot_client, dump_channel_id):
    while True:
        file_info, extract_dir, conf_path, local_thumb_path = None, None, None, None
        upload_id = None
        try:
            await pause_event.wait()
            if config.STATE_CANCELLING in user_status.get('state', ''): break
            
            file_info = await asyncio.wait_for(queue.get(), timeout=2.0)
            if file_info is None: break

            file_path, message, filename, final_caption, job = file_info
            upload_id = f"ul_{filename or f'text_{message.id}'}"
            log_prefix = f"[Job {job.id}][Msg {message.id}][Worker {worker_id}]"
            config.logger.info(f"{log_prefix} CONSUMER: Got '{filename or 'text message'}' from queue.")
            
            with get_session() as session:
                user = session.get(BotUser, user_id)
            if not user: continue
            
            sent_message = False

            # --- Path 1: Google Drive Upload via rclone ---
            if job.upload_to_gdrive:
                config.logger.info(f"{log_prefix} CONSUMER: Starting upload to GDrive for '{filename}'.")
                if not user.gdrive_creds_json: raise ValueError("User not auth'd")
                
                config.logger.info(f"{log_prefix} CONSUMER: Starting upload to GDrive for '{filename}'.")
                
                conf_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"rclone_{job.id}_{worker_id}.conf")
                

                try:
                    # --- DEFINITIVE: Add client_id and client_secret to the config ---
                    rclone_config_content = (
                        f"[gdrive]\n"
                        f"type = drive\n"
                        f"scope = drive\n"
                        f"token = {user.gdrive_creds_json}\n"
                        # This tells rclone to use the same app identity that created the token
                        f"client_id = 202264815644.apps.googleusercontent.com\n"
                        f"client_secret = X4Z3ca8xfWDb1Voo-F9a7ZxJ\n"
                    )
                    
                    with open(conf_path, "w") as f:
                        f.write(rclone_config_content)

                    
                    rclone_cmd = ['rclone', 'copyto', '--config', conf_path, '--progress', file_path, f'gdrive:{filename}']
                    process = await asyncio.create_subprocess_exec(*rclone_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    _, stderr = await process.communicate()

                    if process.returncode == 0:
                        config.logger.info(f"{log_prefix} CONSUMER: ✅ GDrive upload successful.")
                        log_audit_event(user_id, "UPLOAD_SUCCESS", f"Dest: GDrive, File: {filename}")
#                        sent_message = True 
                        continue
                    else:
                        raise IOError(f"rclone failed: {stderr.decode()}")
                finally:
                    if os.path.exists(conf_path): os.remove(conf_path)
                    
            # --- Path 2: Koofr Upload Path (Definitive File-Based Fix) ---
            elif job.upload_to_koofr:
                if not (user.koofr_email and user.koofr_app_password):
                    raise ValueError("User has not configured Koofr credentials.")

                log_prefix = f"[Job {job.id}][Msg {message.id if message else 'N/A'}]"
                config.logger.info(f"{log_prefix} ▶ Uploading {filename} to Koofr")
                conf_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"koofr_{job.id}_{worker_id}.conf")

                try:
                    # --- Step 1: Obscure the password ---
                    obscure_proc = await asyncio.create_subprocess_exec(
                        'rclone', 'obscure', user.koofr_app_password,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    obscured_pass_bytes, obscure_err = await obscure_proc.communicate()
                    if obscure_proc.returncode != 0:
                        raise IOError(f"Failed to obscure Koofr password: {obscure_err.decode()}")
                    
                    obscured_password = obscured_pass_bytes.decode().strip()

                    # --- Step 2: Write the temporary config file with the CORRECT key ---
                    rclone_config = (
                        f"[koofr]\n"
                        f"type = koofr\n"
                        f"user = {user.koofr_email}\n"
                        f"password = {obscured_password}\n"  # <-- The correct key is 'password'
                    )
                    with open(conf_path, "w") as f:
                        f.write(rclone_config)

                    # --- Step 3: Build and run the command, explicitly pointing to the config ---
                    cmd = [
                        'rclone', 'copyto',
                        '--config', conf_path,  # <-- Explicitly tell rclone which config to use
                        '--progress',
                        file_path,
                        f"koofr:{filename}"
                    ]
                    
                    upload_proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    _, upload_stderr = await upload_proc.communicate()

                    if upload_proc.returncode != 0:
                        raise IOError(upload_stderr.decode(errors='ignore').strip())
                    
                    # --- Success ---
                    log_audit_event(user_id, "UPLOAD_SUCCESS", f"Dest: Koofr, File: {filename}")
                    config.logger.info(f"{log_prefix} ✅ Koofr upload succeeded")
                    sent_message = True
                    continue

                finally:
                    # Securely clean up the temporary config file
                    if os.path.exists(conf_path):
                        os.remove(conf_path)

            # --- Path 3: All Telegram Uploads ---
            else:
                destination_entity = 'me' if job.target_channel_id == 777 else (job.target_channel_id or user_id)
                upload_client = bot_client if job.target_channel_id is None else client
                config.logger.info(f"{log_prefix} CONSUMER: Preparing upload to Telegram entity '{destination_entity}'.")            
            
                # --- Thumbnail Preparation (with Caching) ---
                local_thumb_path = None
                async with THUMBNAIL_CACHE_LOCK:
                    if job.id not in GLOBAL_THUMBNAIL_CACHE:
                        has_thumb_permission = user.allow_thumbnails or user.role == 'Owner'
                        
                        # Priority 1: Check for a manual thumbnail URL
                        if has_thumb_permission and job.thumbnail_url:
                            thumb_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"thumb_job_{job.id}.jpg")
                            config.logger.info(f"Attempting to download manual thumbnail for job {job.id}")
                            try:
                                async with httpx.AsyncClient() as http_client:
                                    r = await http_client.get(job.thumbnail_url)
                                    r.raise_for_status()
                                    with open(thumb_path, "wb") as f: 
                                        f.write(r.content)
                                GLOBAL_THUMBNAIL_CACHE[job.id] = thumb_path
                            except Exception as e:
                                config.logger.warning(f"Failed to download manual thumbnail: {e}")
                                GLOBAL_THUMBNAIL_CACHE[job.id] = None
                        
                        # Priority 2: Check for auto-generation if no manual thumb was found
                        elif has_thumb_permission and job.auto_thumbnail and is_video(filename):
                            thumb_path = os.path.join(config.TEMP_DOWNLOAD_DIR, f"auto_thumb_{job.id}.jpg")
                            config.logger.info(f"Auto-generating thumbnail for job {job.id}")
                            try:
                                process = await asyncio.create_subprocess_exec(
                                    'ffmpeg', '-i', file_path, '-ss', str(job.thumbnail_timestamp), 
                                    '-vframes', '1', '-q:v', '2', '-y', thumb_path,
                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                                )
                                _, stderr = await process.communicate()
                                if process.returncode == 0:
                                    GLOBAL_THUMBNAIL_CACHE[job.id] = thumb_path
                                else:
                                    config.logger.error(f"ffmpeg failed: {stderr.decode()}")
                                    GLOBAL_THUMBNAIL_CACHE[job.id] = None
                            except Exception as e:
                                config.logger.error(f"Failed to run ffmpeg: {e}")
                                GLOBAL_THUMBNAIL_CACHE[job.id] = None
                        
                        else:
                            # No thumbnail available for this job
                            GLOBAL_THUMBNAIL_CACHE[job.id] = None
                            
                    local_thumb_path = GLOBAL_THUMBNAIL_CACHE.get(job.id)

                # --- Unzip or Single File Upload Logic ---
                if job.unzip_files and job.zip_password and filename and (filename.endswith(('.7z', '.zip'))):
                    config.logger.info(f"{log_prefix} CONSUMER: Starting unzip for '{filename}'.")
                    extract_dir = os.path.join(config.TEMP_DOWNLOAD_DIR, f"job_{job.id}_extracted_{int(time.time())}")
                    os.makedirs(extract_dir, exist_ok=True)

                    try:
                        # Check if file exists before trying to extract
                        if not os.path.exists(file_path):
                            raise FileNotFoundError(f"Archive file not found: {file_path}")
                        
                        # Extract the archive
                        if filename.endswith('.7z'):
                            import py7zr
                            with py7zr.SevenZipFile(file_path, 'r', password=job.zip_password) as archive:
                                archive.extractall(path=extract_dir)
                        elif filename.endswith('.zip'):
                            import zipfile
                            with zipfile.ZipFile(file_path, 'r') as archive:
                                archive.extractall(path=extract_dir, pwd=job.zip_password.encode() if job.zip_password else None)
                        
                        config.logger.info(f"Extraction completed. Contents: {os.listdir(extract_dir)}")
                        
                        # Upload each extracted file
                        extracted_files = []
                        for root, dirs, files in os.walk(extract_dir):
                            for file in files:
                                full_path = os.path.join(root, file)
                                relative_path = os.path.relpath(full_path, extract_dir)
                                extracted_files.append((full_path, relative_path))
                        
                        # Sort by relative path for consistent ordering
                        extracted_files.sort(key=lambda x: x[1])
                        
                        upload_count = 0
                        for full_path, relative_path in extracted_files:
                            if os.path.exists(full_path) and os.path.isfile(full_path):
                                try:
                                    config.logger.info(f"Uploading extracted file {upload_count + 1}/{len(extracted_files)}: {relative_path}")
                                    
                                    # Create a caption with the relative path
                                    file_caption = relative_path if len(relative_path) <= 1024 else os.path.basename(relative_path)
                                    
                                    # Determine if this extracted file should get a thumbnail
                                    is_video_file = relative_path.lower().endswith(config.VIDEO_EXTENSIONS)
                                    should_use_thumb = local_thumb_path and (is_video_file or job.thumbnail_for_all)
                                    
                                    config.logger.info(f"File: {relative_path}, Is video: {is_video_file}, Using thumbnail: {bool(should_use_thumb)}")
                                    
                                    await upload_client.send_file(
                                        destination_entity, 
                                        full_path, 
                                        caption=file_caption,
                                        parse_mode='html',
                                        thumb=local_thumb_path if should_use_thumb else None
                                    )
                                    upload_count += 1
                                    
                                    # Small delay to avoid rate limiting
                                    await asyncio.sleep(0.5)
                                    
                                except Exception as upload_error:
                                    config.logger.error(f"Failed to upload extracted file {relative_path}: {upload_error}")
                                    continue
                            else:
                                config.logger.warning(f"Skipping non-existent or non-file: {full_path}")
                        
                        if upload_count > 0:
                            sent_message = True  # Ensure it sets sent_message = True on success
                            config.logger.info(f"Successfully uploaded {upload_count} extracted files from {filename}")
                        else:
                            config.logger.warning(f"No files were successfully uploaded from {filename}")
                            
                    except Exception as extract_error:
                        config.logger.error(f"Failed to extract archive {filename}: {extract_error}")
                        # Fall back to uploading the original archive
                        config.logger.info(f"Falling back to uploading original archive: {filename}")
                        try:
                            # Use thumbnail for fallback upload if appropriate
                            is_video = filename.lower().endswith(config.VIDEO_EXTENSIONS)
                            should_use_thumb = local_thumb_path and (is_video or job.thumbnail_for_all)
                            
                            sent_message = await upload_client.send_file(
                                destination_entity, 
                                file_path, 
                                caption=f"⚠️ Failed to extract: {filename}\n{final_caption or ''}", 
                                parse_mode='html',
                                thumb=local_thumb_path if should_use_thumb else None
                            )
                        except Exception as fallback_error:
                            config.logger.error(f"Fallback upload also failed: {fallback_error}")
                            raise fallback_error
                            
                elif file_path == "text_only":
                    config.logger.info(f"{log_prefix} CONSUMER: Sending text-only message.")
                    sent_message = await upload_client.send_message(
                        destination_entity, 
                        final_caption or message.text, 
                        parse_mode='html'
                        )
                else: # Regular single file
                    config.logger.info(f"{log_prefix} CONSUMER: Starting single file upload for '{filename}'.")
                        
                    # Initialize upload tracking
                    async with lock: 
                        user_status[config.STATE_UPLOADING][upload_id] = {
                            'filename': filename, 
                            'start_time': time.time(), 
                            'total': os.path.getsize(file_path)
                        }
                    
                    is_video = (filename or "").lower().endswith(config.VIDEO_EXTENSIONS)
                    config.logger.info(f"Regular file upload - Is video: {is_video}")
                    
                    # Determine if thumbnail should be used for this file
                    should_use_thumb = local_thumb_path and (is_video or job.thumbnail_for_all)
                    config.logger.info(f"Final thumbnail decision for regular upload: {'SET' if should_use_thumb else 'NOT SET'}")
                    
                    # Prepare upload parameters
                    attrs = None
                    if is_video and hasattr(message, 'document') and message.document:
                        attrs = [a for a in getattr(message.document, 'attributes', []) 
                                if isinstance(a, types.DocumentAttributeVideo)]
                    
                    user_progress_callback = functools.partial(
                        progress_callback, user_status, lock, upload_id, config.STATE_UPLOADING
                    )
                    
                    # Upload the file
                    sent_message = await upload_client.send_file(
                        destination_entity, 
                        file_path, 
                        caption=final_caption, 
                        parse_mode='html',
                        force_document=not (is_video and job.stream_videos), 
                        attributes=attrs, 
                        thumb=local_thumb_path if should_use_thumb else None, 
                        progress_callback=user_progress_callback
                    )

            # --- Unified Audit & Dump Logic ---
            if sent_message:
                destination_desc = "GDrive" if job.target_channel_id == 'gdrive' else f"Telegram:{job.target_channel_id or user_id}"
                log_audit_event(user_id, "UPLOAD_SUCCESS", f"Dest: {destination_desc}, File: {filename}")
                
            if dump_channel_id and sent_message and job.target_channel_id != 'gdrive':
                try:
                    dump_caption = await format_dump_caption(user_id, message, bot_client)
                    admin_info = f"\n\n**🛡️ Admin Info:**\n├ **Plan:** {user.plan.title()}\n├ **Role:** {user.role}\n└ **File:** `{filename or 'text_message'}`"
                    full_dump_caption = dump_caption + admin_info
                    
                    if file_path == "text_only":
                        await bot_client.send_message(dump_channel_id, full_dump_caption)
                    else:
                        # For extracted files, we don't dump each individual file
                        if not (job.unzip_files and job.zip_password and filename and 
                               (filename.endswith('.7z') or filename.endswith('.zip'))):
                            await bot_client.send_file(dump_channel_id, file_path, 
                                                     caption=full_dump_caption, parse_mode='md')
                except Exception as dump_error:
                    config.logger.warning(f"Direct file dump failed: {dump_error}")

        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break          
        except Exception as e:
            config.logger.exception(f"Consumer error for user {user_id}: {e}")
            if file_info:
                async with lock: 
                    user_status["failed_ids"].append(message.id)
                log_failure_to_db(user_id, message.id, job.source_channel_id, f"Consumer-side: {e}")

        finally:
            # --- Definitive Cleanup ---
            if file_info:
                file_path, _, _, _, _ = file_info
                if file_path and file_path != "text_only" and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception as cleanup_error:
                        config.logger.warning(f"Failed to remove file {file_path}: {cleanup_error}")
                
                if extract_dir and os.path.exists(extract_dir):
                    try:
                        shutil.rmtree(extract_dir)
                    except Exception as cleanup_error:
                        config.logger.warning(f"Failed to remove extract directory {extract_dir}: {cleanup_error}")
                
                # Update status dictionaries
                async with lock:
                    if upload_id and upload_id in user_status.get(config.STATE_UPLOADING, {}): 
                        del user_status[config.STATE_UPLOADING][upload_id]
                    user_status["uploaded"] += 1
                
                queue.task_done()


def is_video(filename: Optional[str]) -> bool:
    """Checks if a filename corresponds to a video extension."""
    if not filename:
        return False
    return filename.lower().endswith(config.VIDEO_EXTENSIONS)

                
def _upload_to_gdrive_blocking(creds_json, file_path, filename):
    """Synchronous function to handle the GDrive upload."""
    creds = Credentials.from_authorized_user_info(json.loads(creds_json))
    service = build('drive', 'v3', credentials=creds)
    
    media = MediaFileUpload(file_path, resumable=True)
    request = service.files().create(
        body={'name': filename},
        media_body=media,
        fields='id'
    )
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            config.logger.info(f"GDrive Upload: {int(status.progress() * 100)}%")
    return response.get('id')

async def cleanup_job_thumbnail(job_id):
    """Safely cleans up a job's cached thumbnail from memory and disk."""
    async with THUMBNAIL_CACHE_LOCK:
        if job_id in GLOBAL_THUMBNAIL_CACHE:
            thumb_path = GLOBAL_THUMBNAIL_CACHE.pop(job_id, None)
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except OSError as e:
                    config.logger.warning(f"Could not remove thumbnail file {thumb_path}: {e}")


