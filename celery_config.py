#!/usr/bin/env python3

import os
from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create Celery app
app = Celery('reddit_scraper')

# Construct Redis connection URL based on environment
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = os.getenv('REDIS_PORT', '6379')
redis_password = os.getenv('REDIS_PASSWORD', '')

# Check if we're using local Redis or cloud Redis
if redis_password:
    # Cloud Redis with SSL (like Upstash)
    connection_link = f"rediss://:{redis_password}@{redis_host}:{redis_port}?ssl_cert_reqs=none"
else:
    # Local Redis without SSL
    connection_link = f"redis://{redis_host}:{redis_port}"

# Function to generate beat schedule from subreddit configs
def generate_beat_schedule():
    """Generate beat schedule from individual subreddit configurations"""
    try:
        from subreddit_config import get_enabled_scheduled_configs
        
        beat_schedule = {}
        enabled_configs = get_enabled_scheduled_configs()
        
        for i, config in enumerate(enabled_configs):
            schedule = config.get('schedule')
            if schedule:
                task_name = f"config_{i}_{config['name']}_{config['category']}"
                beat_schedule[task_name] = {
                    'task': 'tasks.scheduled_scrape_task',
                    'schedule': schedule,
                    'args': [i]  # Pass config ID as argument
                }
        
        return beat_schedule
        
    except ImportError:
        # Fallback schedule if config can't be imported
        return {}

# Celery configuration
app.conf.update(
    # Broker settings (using Upstash Redis)
    # Construct Redis URL from environment variables
    broker_url=connection_link,
    result_backend=connection_link,

    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task tracking and visibility settings
    task_track_started=True,
    task_send_sent_event=True,
    worker_send_task_events=True,
    task_ignore_result=False,
    result_expires=3600,  # Results expire after 1 hour
    
    # Worker settings for better monitoring
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_disable_rate_limits=False,

    # Reddit API configuration
    reddit_client_id=os.getenv('REDDIT_CLIENT_ID'),
    reddit_client_secret=os.getenv('REDDIT_CLIENT_SECRET'),

    # Beat schedule - dynamically generated from subreddit configs
    beat_schedule=generate_beat_schedule(),

    # Remove task routing to use default queue
    # task_routes={
    #     'tasks.scrape_and_upload_to_r2': {'queue': 'reddit_scraper'},
    #     'tasks.test_scrape_task': {'queue': 'reddit_scraper'},
    # },
)

# Auto-discover tasks
app.autodiscover_tasks(['tasks'])

if __name__ == '__main__':
    app.start()
