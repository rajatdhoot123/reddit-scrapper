# Enhanced Reddit Scraping System

This enhanced version of the Reddit scraping system provides comprehensive automation with multiple scheduling options, advanced URS features, and flexible configuration management.

## Features

### 🚀 Enhanced URS Options Support
- **All URS categories**: `h` (hot), `n` (new), `c` (controversial), `t` (top), `r` (rising), `s` (search)
- **Time filters**: `day`, `week`, `month`, `year`, `all` (for top and controversial)
- **Output formats**: JSON and CSV support with `--csv` flag
- **Subreddit rules**: Include rules with `--rules` flag
- **Auto-confirmation**: Automated execution with `-y` flag

### 📅 Multiple Scheduling Options
- **Daily scrapes**: Comprehensive daily data collection
- **Weekly scrapes**: In-depth weekly analysis across multiple subreddits
- **Hourly hot posts**: Real-time monitoring of trending content
- **Custom intervals**: Flexible 6-hour interval scraping
- **Manual execution**: On-demand scraping with custom parameters

### 🎯 Advanced Configuration
- **Subreddit-specific scheduling**: Different subreddits can be crawled at different times
- **Flexible parameters**: Support for search keywords, result limits, and time filters
- **Comment scraping control**: Configurable comment limits and delays
- **Archive management**: Enhanced compression and metadata inclusion
- **Error handling**: Robust retry logic and failure recovery
- **Enable/Disable Controls**: Granular on/off switches for all scraping components

## File Structure

```
├── tasks.py                 # Enhanced task definitions
├── subreddit_config.py      # Configuration management with enable/disable controls
├── celery_config.py         # Updated Celery configuration
├── scrape_manager.py        # CLI management tool
├── config_editor.py         # Configuration editor for enable/disable toggles
└── README_ENHANCED.md       # This documentation
```

## Configuration

### Global Controls

The system includes comprehensive enable/disable controls at multiple levels:

```python
# Global scraping control settings
GLOBAL_SCRAPING_CONFIG = {
    "master_enabled": True,  # Master switch to enable/disable all scraping
    "scheduled_scraping_enabled": True,  # Enable/disable all scheduled tasks
    "manual_scraping_enabled": True,  # Enable/disable manual scraping
    "comment_scraping_globally_enabled": True,  # Global toggle for comment scraping
    "upload_to_r2_enabled": True,  # Enable/disable R2 uploads
    "create_archives_enabled": True,  # Enable/disable archive creation
}
```

### Subreddit Schedules (`subreddit_config.py`)

The system supports multiple predefined schedules with individual enable/disable controls:

#### Daily Scrapes
```python
'daily_scrapes': {
    'enabled': True,  # Toggle to enable/disable this entire schedule
    'schedule': crontab(hour=23, minute=30),  # 11:30 PM UTC daily
    'subreddits': [
        {
            "name": "CreditCardsIndia",
            "category": "t",  # top posts
            "n_results": 25,
            "time_filter": "day",
            "enabled": True,  # Toggle for individual subreddit config
            "options": {
                "csv": True,
                "rules": False,
                "auto_confirm": True
            }
        }
    ]
}
```

#### Weekly Comprehensive Scrapes
```python
'weekly_scrapes': {
    'enabled': True,  # Toggle to enable/disable this entire schedule
    'schedule': crontab(hour=2, minute=0, day_of_week=1),  # Monday 2:00 AM UTC
    'subreddits': [
        {
            "name": "CreditCardsIndia",
            "category": "t",
            "n_results": 100,
            "time_filter": "week",
            "enabled": True,  # Toggle for individual subreddit config
            "options": {"csv": True, "rules": True, "auto_confirm": True}
        },
        {
            "name": "IndiaInvestments",
            "category": "t",
            "n_results": 50,
            "time_filter": "week",
            "enabled": True,  # Toggle for individual subreddit config
            "options": {"csv": True, "auto_confirm": True}
        }
    ]
}
```

#### Hourly Hot Posts Monitoring
```python
'hourly_hot_scrapes': {
    'enabled': False,  # Example: Disabled schedule (too frequent for some use cases)
    'schedule': crontab(minute=0),  # Every hour
    'subreddits': [
        {
            "name": "CreditCardsIndia",
            "category": "h",
            "n_results": 10,
            "enabled": True,  # Toggle for individual subreddit config
            "options": {"csv": False, "auto_confirm": True}
        }
    ]
}
```

### Manual Configurations

Predefined configurations for on-demand scraping:

```python
MANUAL_SUBREDDIT_CONFIGS = [
    {
        "name": "CreditCardsIndia",
        "category": "s",  # search
        "keywords": "cashback rewards points",
        "time_filter": "month",
        "enabled": True,  # Toggle for individual manual config
        "options": {"csv": True, "rules": False, "auto_confirm": True}
    }
]
```

## Usage

### CLI Management Tool

The `scrape_manager.py` provides a comprehensive command-line interface:

#### Check System Status
```bash
python scrape_manager.py status
```

#### Manual Scraping
```bash
# Scrape top 25 posts from today
python scrape_manager.py manual CreditCardsIndia t 25 --time-filter day --csv

# Search for specific keywords
python scrape_manager.py manual CreditCardsIndia s "cashback rewards" --time-filter month

# Scrape hot posts without comments
python scrape_manager.py manual CreditCardsIndia h 50 --no-comments

# Include subreddit rules
python scrape_manager.py manual IndiaInvestments t 30 --rules --time-filter week
```

#### Run Scheduled Tasks
```bash
# Run daily scrapes
python scrape_manager.py schedule daily_scrapes

# Run weekly comprehensive scrapes
python scrape_manager.py schedule weekly_scrapes

# Run hourly hot posts monitoring
python scrape_manager.py schedule hourly_hot_scrapes
```

#### Predefined Manual Configurations
```bash
python scrape_manager.py manual-config
```

#### List All Configurations
```bash
python scrape_manager.py list-configs
```

#### Test Connection
```bash
python scrape_manager.py test
```

### Configuration Management

#### View Configuration Status
```bash
# List all configurations with their enable/disable status
python scrape_manager.py list-configs

# Show configuration management help
python scrape_manager.py config-help
```

#### Toggle Settings with Config Editor
```bash
# Disable all scraping globally
python config_editor.py global master_enabled false

# Disable comment scraping globally
python config_editor.py global comment_scraping_globally_enabled false

# Disable hourly scrapes
python config_editor.py schedule hourly_hot_scrapes false

# Enable weekly scrapes
python config_editor.py schedule weekly_scrapes true

# List available settings
python config_editor.py list
```

### Configuration Management

#### View Configuration Status
```bash
# List all configurations with their enable/disable status
python scrape_manager.py list-configs

# Show configuration management help
python scrape_manager.py config-help
```

#### Toggle Settings with Config Editor
```bash
# Disable all scraping globally
python config_editor.py global master_enabled false

# Disable comment scraping globally
python config_editor.py global comment_scraping_globally_enabled false

# Disable hourly scrapes
python config_editor.py schedule hourly_hot_scrapes false

# Enable weekly scrapes
python config_editor.py schedule weekly_scrapes true

# List available settings
python config_editor.py list
```

### Programmatic Usage

#### Manual Scraping
```python
from tasks import manual_scrape_subreddit

# Scrape with custom options
result = manual_scrape_subreddit.apply_async(
    args=["CreditCardsIndia", "t", 25],
    kwargs={
        "time_filter": "day",
        "options": {"csv": True, "rules": False},
        "scrape_comments": True
    }
).get()
```

#### Scheduled Tasks
```python
from tasks import scheduled_scrape_task

# Run a specific schedule
result = scheduled_scrape_task.apply_async(
    args=["daily_scrapes"]
).get()
```

## URS Command Mapping

The enhanced system supports all URS options:

| Category | Description | Time Filter Support | Example |
|----------|-------------|-------------------|---------|
| `h` | Hot posts | No | `python scrape_manager.py manual CreditCardsIndia h 50` |
| `n` | New posts | No | `python scrape_manager.py manual CreditCardsIndia n 100` |
| `t` | Top posts | Yes | `python scrape_manager.py manual CreditCardsIndia t 25 --time-filter week` |
| `r` | Rising posts | No | `python scrape_manager.py manual CreditCardsIndia r 20` |
| `c` | Controversial | Yes | `python scrape_manager.py manual CreditCardsIndia c 15 --time-filter month` |
| `s` | Search | Optional | `python scrape_manager.py manual CreditCardsIndia s "credit card"` |

### Additional Options

| Option | Description | Usage |
|--------|-------------|-------|
| `--csv` | Output in CSV format | `--csv` (default) or `--no-csv` |
| `--rules` | Include subreddit rules | `--rules` |
| `--time-filter` | Time filter for top/controversial | `--time-filter day/week/month/year/all` |
| `--no-comments` | Skip comment scraping | `--no-comments` |

## Scheduling

### Celery Beat Schedule

The system automatically runs the following scheduled tasks:

- **Daily scrapes**: Every day at 11:30 PM UTC
- **Weekly scrapes**: Every Monday at 2:00 AM UTC
- **Hourly hot scrapes**: Every hour at minute 0
- **Custom interval scrapes**: Every 6 hours

### Custom Scheduling

You can modify schedules in `subreddit_config.py`:

```python
# Custom schedule example
'custom_schedule': {
    'schedule': crontab(hour=12, minute=0, day_of_week=[1, 3, 5]),  # Mon, Wed, Fri at noon
    'subreddits': [
        {
            "name": "YourSubreddit",
            "category": "h",
            "n_results": 20,
            "options": {"csv": True, "auto_confirm": True}
        }
    ]
}
```

## Configuration Options

### Comment Scraping
```python
COMMENT_SCRAPING_CONFIG = {
    "default_n_comments": 0,  # 0 means all comments
    "max_comments_per_post": 500,  # Performance limit
    "comment_delay_range": (3, 8),  # Random delay between scraping
    "enable_comment_scraping": True
}
```

### Archive Settings
```python
ARCHIVE_CONFIG = {
    "create_daily_archives": True,
    "create_weekly_archives": True,
    "compress_level": 6,  # ZIP compression level (0-9)
    "include_metadata": True
}
```

### Task Configuration
```python
TASK_CONFIG = {
    "max_retries": 3,
    "retry_delay": 300,  # 5 minutes
    "timeout": 300,  # 5 minutes per operation
    "max_concurrent_tasks": 2
}
```

## Monitoring and Logging

### Task Status
```python
from tasks import get_scraping_status

status = get_scraping_status.apply_async().get()
print(status)
```

### Celery Monitoring
```bash
# Monitor Celery workers
celery -A celery_config worker --loglevel=info

# Monitor Celery beat scheduler
celery -A celery_config beat --loglevel=info

# Monitor tasks with Flower (if installed)
celery -A celery_config flower
```

## Error Handling

The system includes comprehensive error handling:

- **Automatic retries**: Configurable retry attempts with exponential backoff
- **Graceful failures**: Individual subreddit failures don't stop the entire batch
- **Detailed logging**: Comprehensive logging for debugging and monitoring
- **Status reporting**: Detailed status information for each operation

## Migration from Legacy System

The enhanced system maintains backward compatibility:

1. **Legacy tasks**: `scrape_and_upload_to_r2` still works
2. **Configuration**: Old `SUBREDDIT_CONFIGS` still supported
3. **Gradual migration**: Can run both systems simultaneously

To migrate:

1. Update your configurations in `subreddit_config.py`
2. Test with `python scrape_manager.py test`
3. Run manual tests with `python scrape_manager.py manual-config`
4. Enable new scheduled tasks in `celery_config.py`

## Troubleshooting

### Common Issues

1. **Connection errors**: Check Redis/Celery configuration
2. **URS path issues**: Ensure `urs` directory is accessible
3. **Permission errors**: Check file system permissions for scrapes directory
4. **Timeout errors**: Adjust timeout values in `TASK_CONFIG`

### Debug Mode

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Manual Testing

Test individual components:
```bash
# Test URS directly
cd urs && poetry run python Urs.py -r CreditCardsIndia t 5 -y

# Test Celery connection
python scrape_manager.py test

# Test specific schedule
python scrape_manager.py schedule daily_scrapes
```

## Performance Considerations

- **Rate limiting**: Built-in delays between requests
- **Resource management**: Configurable concurrent task limits
- **Archive compression**: Adjustable compression levels
- **Comment limits**: Configurable per-post comment limits
- **Timeout handling**: Configurable operation timeouts

## Security

- **Environment variables**: Sensitive data stored in `.env`
- **Auto-confirmation**: Prevents interactive prompts in automation
- **Error sanitization**: Sensitive information filtered from logs
- **Access control**: Redis authentication and SSL support 