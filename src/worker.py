#==== worker.py ====

import asyncio
import os
import redis
import logging
from telethon import TelegramClient
from telethon.sessions import StringSession

# Import the necessary components from your existing bot code
from bot import config
from bot.core import run_job_worker
from bot.database import get_session
from bot.utils import get_user_client
from bot.handlers.user_commands import get_credentials # We can reuse this
from bot.utils import get_credentials 
from bot.database import get_session, Job 


# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
config.logger = logging.getLogger("worker")

async def muscle_worker(worker_id: int, redis_client):
    """The main loop for a single 'Muscle' worker."""
    config.logger.info(f"💪 Muscle Worker #{worker_id} started. Waiting for jobs...")
    
    # Use a blocking pop with a timeout to wait for new jobs from the Redis queue
    # The queue is a simple Redis List named 'job_queue'
    while True:
        try:
            # This will wait up to 60 seconds for a job, then loop.
            # 'brpop' is "blocking right pop", a standard queue pattern.
            _, job_id_bytes = redis_client.brpop('job_queue', timeout=60)
            if not job_id_bytes:
                continue

            job_id = int(job_id_bytes.decode('utf-8'))
            config.logger.info(f"💪 Worker #{worker_id} received Job #{job_id}. Starting...")
            
            # We need a bot client and a user client to run the job
            creds = get_credentials()
            bot_client = TelegramClient(StringSession(), int(creds['API_ID']), creds['API_HASH'])
            await bot_client.start(bot_token=creds['BOT_TOKEN'])

            # Determine which user this job belongs to
            with get_session() as session:
                job = session.get(Job, job_id)
                user_id = job.user_id if job else None

            if not user_id:
                config.logger.error(f"Could not find user for Job #{job_id}. Skipping.")
                continue

            user_client = await get_user_client(user_id)

            if not user_client:
                config.logger.error(f"Could not get user client for Job #{job_id}. Skipping.")
                continue

            # Execute the existing, powerful job worker
            await run_job_worker(user_client, bot_client, job_id)
            
            config.logger.info(f"💪 Worker #{worker_id} finished Job #{job_id}.")
            await bot_client.disconnect()

        except Exception as e:
            config.logger.exception(f"💪 Worker #{worker_id} encountered a fatal error: {e}")
            await asyncio.sleep(10) # Wait before retrying


async def main():
    """Starts up the Muscle workers."""
    creds = get_credentials()
    if not creds:
        return
        
    redis_url = creds.get('UPSTASH_REDIS_URL')
    if not redis_url:
        config.logger.critical("UPSTASH_REDIS_URL not found in environment. Cannot start workers.")
        return

    redis_client = redis.from_url(redis_url)

    # You can run multiple workers in parallel on the same machine
    num_workers = 3
    worker_tasks = [muscle_worker(i, redis_client) for i in range(num_workers)]
    await asyncio.gather(*worker_tasks)


if __name__ == "__main__":
    asyncio.run(main())

