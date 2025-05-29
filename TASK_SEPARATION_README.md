# Task Separation Architecture

This document explains the new modular task architecture that separates database operations from upload operations, making them independent and allowing for more flexible task orchestration.

## Overview

The Reddit scraping system now supports independent task execution with the following separation:

1. **Scraping Tasks** - Handle Reddit data collection only
2. **Database Tasks** - Handle database operations independently  
3. **Upload Tasks** - Handle file upload and archiving independently

## Independent Task Definitions

### 1. Database-Only Task

```python
@app.task(bind=True, max_retries=2)
def database_only_task(task_id, task_type, config, result, scrape_file_path=None)
```

**Purpose**: Handles only database operations for scraped data.

**Parameters**:
- `task_id`: Unique identifier for the scraping session
- `task_type`: Type of task (e.g., "scheduled", "manual")  
- `config`: Configuration dictionary used for scraping
- `result`: Result dictionary from scraping operation
- `scrape_file_path`: Path to the scraped data file (optional)

**Usage Example**:
```python
# Launch database task independently
db_task = database_only_task.apply_async(
    args=[
        "manual_reddit_123", 
        "manual",
        {"name": "CreditCardsIndia", "category": "t", "n_results": 25},
        {"status": "success", "subreddit": "CreditCardsIndia", "submissions_found": 25},
        "/path/to/scrape_file.json"
    ]
)

# Check result
result = db_task.get()
print(f"Database save status: {result['status']}")
```

### 2. Upload-Only Task

```python
@app.task(bind=True, max_retries=2)
def upload_only_task(archive_path, object_key, upload_metadata=None, cleanup_after_upload=True)
```

**Purpose**: Handles only file upload to R2/S3 storage.

**Parameters**:
- `archive_path`: Path to the archive file to upload
- `object_key`: Unique key/path for the file in R2 storage
- `upload_metadata`: Dictionary of metadata to attach to the upload
- `cleanup_after_upload`: Whether to delete local file after successful upload

**Usage Example**:
```python
# Launch upload task independently
upload_task = upload_only_task.apply_async(
    args=[
        "/path/to/reddit_scrapes_2024-01-15.zip",
        "daily_scrapes/CreditCardsIndia/2024-01-15/daily_scrape_123.zip",
        {"scrape_type": "manual", "subreddit": "CreditCardsIndia"},
        True
    ]
)

# Check result
result = upload_task.get()
print(f"Upload status: {result['status']}")
```

### 3. Archive and Upload Task

```python
@app.task(bind=True, max_retries=2)
def archive_and_upload_task(scrapes_dir_path, archive_type="daily", custom_name=None, 
                          configs_processed=None, results=None, upload_metadata=None,
                          cleanup_after_upload=True)
```

**Purpose**: Combines archive creation and upload into a single independent task.

**Parameters**:
- `scrapes_dir_path`: Path to directory containing scraped files
- `archive_type`: Type of archive (e.g., "daily", "manual", "scheduled")
- `custom_name`: Custom name for the archive (optional)
- `configs_processed`: List of configurations that were processed
- `results`: List of scraping results
- `upload_metadata`: Metadata for the upload
- `cleanup_after_upload`: Whether to clean up local archive after upload

## Modified Task Functions

### 1. Scraping-Only Function

```python
def process_subreddit_config(config: Dict, scrapes_dir: Path) -> Dict
```

**Changes**: 
- Removed inline database operations
- Returns comprehensive result including `scrape_file_path` and `config_used`
- Focuses only on Reddit scraping and comment collection

### 2. Legacy Function with Database

```python
def process_subreddit_config_with_database(config: Dict, scrapes_dir: Path, task_type: str = "scheduled") -> Dict
```

**Purpose**: Backward compatibility function that combines scraping + async database task launch.

## New Modular Scheduled Task

### scheduled_scrape_task_modular

```python
@app.task(bind=True, max_retries=None)
def scheduled_scrape_task_modular(config_id: int = None)
```

**Purpose**: A new version of the scheduled task that uses the independent task architecture.

**Execution Flow**:
1. **Scraping Phase**: Collects Reddit data using `process_subreddit_config`
2. **Database Phase**: Launches independent `database_only_task` for each successful scrape
3. **Archive/Upload Phase**: Uses `archive_and_upload_task` for file management
4. **Reporting Phase**: Collects status from all independent tasks

**Benefits**:
- Database operations don't block archive/upload operations
- Upload operations don't block database operations  
- Better error isolation - one failure doesn't affect others
- Improved monitoring and debugging capabilities
- Scalable task distribution

## Usage Patterns

### Pattern 1: Full Independence

```python
# 1. Scrape only
scrape_result = process_subreddit_config(config, scrapes_dir)

# 2. Launch database task (don't wait)
if scrape_result["status"] == "success":
    db_task = database_only_task.delay(
        task_id, "manual", config, scrape_result, scrape_result["scrape_file_path"]
    )

# 3. Launch upload task (don't wait) 
if scrapes_dir.exists():
    upload_task = archive_and_upload_task.delay(
        str(scrapes_dir), "manual", None, [config], [scrape_result], metadata
    )

# 4. Continue with other work while tasks run in background
```

### Pattern 2: Sequential with Error Handling

```python
# 1. Scrape first
scrape_result = process_subreddit_config(config, scrapes_dir)

if scrape_result["status"] == "success":
    # 2. Database operations (wait for completion)
    try:
        db_result = database_only_task.apply_async(...).get(timeout=300)
        print(f"Database saved: {db_result['database_saved']}")
    except Exception as e:
        print(f"Database failed: {e}")
    
    # 3. Upload operations (independent of database results)
    try:
        upload_result = upload_only_task.apply_async(...).get(timeout=600) 
        print(f"Upload completed: {upload_result['uploaded']}")
    except Exception as e:
        print(f"Upload failed: {e}")
```

### Pattern 3: Database-Only Operations

```python
# Re-process existing scrape files to database
scrape_files = Path("scrapes/2024-01-15").glob("*.json")

for scrape_file in scrape_files:
    # Extract info from filename or metadata
    config = extract_config_from_filename(scrape_file.name)
    result = {"status": "success", "subreddit": config["name"], "scrape_file_path": str(scrape_file)}
    
    # Process to database only
    db_task = database_only_task.delay(
        f"reprocess_{scrape_file.stem}", "reprocess", config, result, str(scrape_file)
    )
    print(f"Launched database task: {db_task.id}")
```

## Configuration Changes

### Celery Beat Schedule

The legacy `scrape_and_upload_to_r2` task is now available for backward compatibility and will work with existing schedules.

To use the new modular tasks, you can update your beat schedule:

```python
# In celery_config.py
beat_schedule = {
    'modular-daily-scrape': {
        'task': 'tasks.scheduled_scrape_task_modular',
        'schedule': crontab(hour=23, minute=30),
        'args': [0]  # Config ID
    }
}
```

### Global Configuration

All existing configuration flags are respected:

```python
GLOBAL_SCRAPING_CONFIG = {
    "master_enabled": True,
    "upload_to_r2_enabled": True,  # Affects upload tasks
    "create_archives_enabled": True,  # Affects archive tasks
    # ... other settings
}
```

## Benefits of the New Architecture

1. **Independence**: Database and upload operations can run independently
2. **Scalability**: Tasks can be distributed across different workers
3. **Reliability**: Failure in one operation doesn't affect others
4. **Monitoring**: Better visibility into each operation's status
5. **Flexibility**: Can run database-only or upload-only operations as needed
6. **Debugging**: Easier to isolate and fix issues in specific operations
7. **Performance**: Operations can run in parallel instead of sequentially

## Migration Guide

### For Existing Schedules
- No changes needed - legacy tasks continue to work
- `scrape_and_upload_to_r2` task has been restored for backward compatibility

### For Custom Scripts
- Replace `process_subreddit_config_with_database()` calls with independent task launches
- Use `process_subreddit_config()` for scraping-only operations
- Launch `database_only_task` and `upload_only_task` as needed

### For New Implementations
- Use `scheduled_scrape_task_modular` for new scheduled tasks
- Use independent tasks for custom workflows
- Take advantage of parallel execution capabilities

## Monitoring and Debugging

### Task Status Checking

```python
from celery.result import AsyncResult

# Check database task status
db_result = AsyncResult(task_id)
print(f"Database task status: {db_result.state}")
if db_result.ready():
    print(f"Database result: {db_result.result}")

# Check upload task status  
upload_result = AsyncResult(upload_task_id)
print(f"Upload task status: {upload_result.state}")
```

### Logging

Each independent task logs its operations separately:
- `database_only_task`: Logs database operations
- `upload_only_task`: Logs upload operations
- `archive_and_upload_task`: Logs both archive creation and upload

### Flower Monitoring

Independent tasks appear as separate entries in Flower, allowing better monitoring of:
- Task distribution across workers
- Individual task performance 
- Error rates for each operation type
- Queue depths and processing times 