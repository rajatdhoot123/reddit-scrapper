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

    # Beat schedule for periodic tasks
    beat_schedule={
        # Legacy daily scrape (for backward compatibility)
        'daily-reddit-scrape': {
            'task': 'tasks.scrape_and_upload_to_r2',
            'schedule': crontab(hour=23, minute=30),  # 11:30 PM UTC daily
        },
        
        # Enhanced scheduled tasks
        'daily-scrapes': {
            'task': 'tasks.daily_scrape_task',
            'schedule': crontab(hour=23, minute=30),  # 11:30 PM UTC daily
        },
        
        'weekly-scrapes': {
            'task': 'tasks.weekly_scrape_task',
            'schedule': crontab(hour=2, minute=0, day_of_week=1),  # Monday 2:00 AM UTC
        },
        
        'hourly-hot-scrapes': {
            'task': 'tasks.hourly_hot_scrape_task',
            'schedule': crontab(minute=0),  # Every hour at minute 0
        },
        
        'custom-interval-scrapes': {
            'task': 'tasks.custom_interval_scrape_task',
            'schedule': crontab(minute=0, hour='*/6'),  # Every 6 hours
        },
    },

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
