#!/usr/bin/env python3

from celery.schedules import crontab
from datetime import timedelta

# Global scraping control settings
GLOBAL_SCRAPING_CONFIG = {
    "master_enabled": True,  # Master switch to enable/disable all scraping
    "scheduled_scraping_enabled": True,  # Enable/disable all scheduled tasks
    "manual_scraping_enabled": True,  # Enable/disable manual scraping
    "comment_scraping_globally_enabled": True,  # Global toggle for comment scraping
    "upload_to_r2_enabled": True,  # Enable/disable R2 uploads
    "create_archives_enabled": True,  # Enable/disable archive creation
}

# Enhanced subreddit configurations with scheduling support
SUBREDDIT_SCHEDULES = {
    # Daily scrapes
    'daily_scrapes': {
        'enabled': True,  # Toggle to enable/disable this entire schedule
        'schedule': crontab(hour=23, minute=30),  # 11:30 PM UTC daily
        'subreddits': [
            {
                "name": "CreditCardsIndia",
                "category": "t",  # top posts
                "n_results": 25,
                "time_filter": "day",  # day, week, month, year, all
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "rules": False,
                    "auto_confirm": True
                }
            },
            {
                "name": "LifeProTips",
                "category": "t",
                "n_results": 25,
                "time_filter": "day",
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "rules": False,
                    "auto_confirm": True
                }
            }
        ]
    },

    # Weekly comprehensive scrapes
    'weekly_scrapes': {
        'enabled': False,  # Toggle to enable/disable this entire schedule
        # Monday 2:00 AM UTC
        'schedule': crontab(hour=2, minute=0, day_of_week=1),
        'subreddits': [
            {
                "name": "CreditCardsIndia",
                "category": "t",  # top posts
                "n_results": 100,
                "time_filter": "week",
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "rules": True,
                    "auto_confirm": True
                }
            },
            {
                "name": "IndiaInvestments",
                "category": "t",
                "n_results": 50,
                "time_filter": "week",
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "auto_confirm": True
                }
            },
            {
                "name": "PersonalFinanceIndia",
                "category": "t",
                "n_results": 50,
                "time_filter": "week",
                "enabled": False,  # Example: Disabled subreddit config
                "options": {
                    "csv": False,
                    "auto_confirm": True
                }
            }
        ]
    },

    # Hourly hot posts for active monitoring
    'hourly_hot_scrapes': {
        # Example: Disabled schedule (too frequent for some use cases)
        'enabled': True,
        'schedule': crontab(minute=0),  # Every hour at minute 0
        'subreddits': [
            {
                "name": "CreditCardsIndia",
                "category": "h",  # hot posts
                "n_results": 10,
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "auto_confirm": True
                }
            }
        ]
    },

    # Custom interval scrapes
    'custom_interval_scrapes': {
        'enabled': False,  # Toggle to enable/disable this entire schedule
        'schedule': timedelta(hours=6),  # Every 6 hours
        'subreddits': [
            {
                "name": "CreditCardsIndia",
                "category": "n",  # new posts
                "n_results": 30,
                "enabled": True,  # Toggle for individual subreddit config
                "options": {
                    "csv": False,
                    "auto_confirm": True
                }
            }
        ]
    }
}

# Additional subreddit configurations for manual/on-demand scraping
MANUAL_SUBREDDIT_CONFIGS = [
    {
        "name": "CreditCardsIndia",
        "category": "s",  # search
        "keywords": "cashback rewards points",
        "time_filter": "month",
        "enabled": True,  # Toggle for individual manual config
        "options": {
            "csv": False,
            "rules": False,
            "auto_confirm": True
        }
    },
    {
        "name": "IndiaInvestments",
        "category": "c",  # controversial
        "n_results": 25,
        "time_filter": "week",
        "enabled": True,  # Toggle for individual manual config
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    {
        "name": "PersonalFinanceIndia",
        "category": "r",  # rising
        "n_results": 20,
        "enabled": False,  # Example: Disabled manual config
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    }
]

# Comment scraping configurations
COMMENT_SCRAPING_CONFIG = {
    "default_n_comments": 0,  # 0 means all comments
    "max_comments_per_post": 500,  # Limit for performance
    # Random delay between comment scraping (seconds)
    "comment_delay_range": (3, 8),
    "enable_comment_scraping": True
}

# Archive and upload configurations
ARCHIVE_CONFIG = {
    "create_daily_archives": True,
    "create_weekly_archives": True,
    "compress_level": 6,  # ZIP compression level (0-9)
    "include_metadata": True
}

# Retry and error handling configurations
TASK_CONFIG = {
    "max_retries": 3,
    "retry_delay": 300,  # 5 minutes
    "timeout": 300,  # 5 minutes per scraping operation
    "max_concurrent_tasks": 2
}
