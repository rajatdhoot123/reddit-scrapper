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

# Subreddit configurations with individual scheduling
SUBREDDIT_CONFIGS = [
    # Daily top posts - CreditCardsIndia
    {
        "name": "CreditCardsIndia",
        "category": "t",  # top posts
        "n_results": 25,
        "time_filter": "day",  # day, week, month, year, all
        "enabled": True,  # Toggle for individual subreddit config
        "schedule": crontab(hour=23, minute=30),  # 11:30 PM UTC daily
        "options": {
            "csv": False,
            "rules": False,
            "auto_confirm": True
        }
    },
    
    # Daily top posts - LifeProTips
    {
        "name": "LifeProTips",
        "category": "t",
        "n_results": 25,
        "time_filter": "day",
        "enabled": True,
        "schedule": crontab(hour=23, minute=45),  # 11:45 PM UTC daily (15 min after CreditCardsIndia)
        "options": {
            "csv": False,
            "rules": False,
            "auto_confirm": True
        }
    },
    
    # Weekly comprehensive scrape - CreditCardsIndia
    {
        "name": "CreditCardsIndia",
        "category": "t",
        "n_results": 100,
        "time_filter": "week",
        "enabled": True,
        "schedule": crontab(hour=2, minute=0, day_of_week=1),  # Monday 2:00 AM UTC
        "options": {
            "csv": False,
            "rules": True,
            "auto_confirm": True
        }
    },
    
    # Weekly scrape - IndiaInvestments
    {
        "name": "IndiaInvestments",
        "category": "t",
        "n_results": 50,
        "time_filter": "week",
        "enabled": True,
        "schedule": crontab(hour=2, minute=30, day_of_week=1),  # Monday 2:30 AM UTC
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Weekly scrape - PersonalFinanceIndia (disabled example)
    {
        "name": "PersonalFinanceIndia",
        "category": "t",
        "n_results": 50,
        "time_filter": "week",
        "enabled": False,  # Disabled subreddit config
        "schedule": crontab(hour=3, minute=0, day_of_week=1),  # Monday 3:00 AM UTC
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Hourly hot posts - CreditCardsIndia
    {
        "name": "CreditCardsIndia",
        "category": "h",  # hot posts
        "n_results": 10,
        "enabled": True,
        "schedule": crontab(minute=0),  # Every hour at minute 0
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Custom interval using timedelta - CreditCardsIndia new posts
    {
        "name": "CreditCardsIndia",
        "category": "n",  # new posts
        "n_results": 30,
        "enabled": False,  # Disabled for now
        "schedule": timedelta(hours=6),  # Every 6 hours
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Multiple daily scrapes for different subreddits at different times
    {
        "name": "MachineLearning",
        "category": "t",
        "n_results": 20,
        "time_filter": "day",
        "enabled": True,
        "schedule": crontab(hour=8, minute=0),  # 8:00 AM UTC
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    {
        "name": "Python",
        "category": "h",
        "n_results": 15,
        "enabled": True,
        "schedule": crontab(hour=12, minute=30),  # 12:30 PM UTC
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Weekend-only scrapes
    {
        "name": "WeekendWarrior",
        "category": "t",
        "n_results": 30,
        "time_filter": "day",
        "enabled": True,
        "schedule": crontab(hour=10, minute=0, day_of_week=[6, 0]),  # Saturday and Sunday 10:00 AM
        "options": {
            "csv": False,
            "auto_confirm": True
        }
    },
    
    # Monthly deep dive
    {
        "name": "DataScience",
        "category": "t",
        "n_results": 200,
        "time_filter": "month",
        "enabled": True,
        "schedule": crontab(hour=4, minute=0, day_of_month=1),  # 1st day of every month at 4:00 AM
        "options": {
            "csv": True,  # Enable CSV for monthly comprehensive scrapes
            "rules": True,
            "auto_confirm": True
        }
    }
]

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

# Helper function to get enabled scheduled configs
def get_enabled_scheduled_configs():
    """Return only enabled subreddit configurations that have schedules."""
    return [config for config in SUBREDDIT_CONFIGS if config.get('enabled', False) and 'schedule' in config]

# Helper function to get configs by schedule type
def get_configs_by_schedule_pattern(pattern_func):
    """
    Get configs that match a specific schedule pattern.
    pattern_func should take a crontab/timedelta and return True/False
    """
    enabled_configs = get_enabled_scheduled_configs()
    return [config for config in enabled_configs if pattern_func(config['schedule'])]

# Helper function to get unique subreddit names
def get_unique_subreddit_names():
    """Get all unique subreddit names from enabled configs."""
    enabled_configs = get_enabled_scheduled_configs()
    return list(set(config['name'] for config in enabled_configs))
