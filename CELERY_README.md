# Reddit Scraping Automation with Celery

This setup provides automated daily scraping of Reddit data using Celery and uploads the results to a Cloudflare R2 bucket.

## Features

- **Automated Daily Scraping**: Runs every day at 11:30 PM UTC
- **Multiple Categories**: Scrapes new, hot, and top posts from CreditCardsIndia subreddit
- **Comment Scraping**: Automatically scrapes all comments from each post
- **Cloud Storage**: Uploads compressed archives to Cloudflare R2 bucket
- **Retry Logic**: Automatically retries failed tasks up to 3 times
- **Monitoring**: Includes logging and optional Flower monitoring interface

## Prerequisites

1. **Redis Server**: Required as the message broker for Celery
2. **Cloudflare R2 Account**: For storing the scraped data
3. **Python Dependencies**: Installed via Poetry

## Installation

1. **Install Dependencies**:
   ```bash
   poetry install
   ```

2. **Install and Start Redis**:
   ```bash
   # On macOS with Homebrew
   brew install redis
   redis-server
   
   # On Ubuntu/Debian
   sudo apt-get install redis-server
   sudo systemctl start redis-server
   ```

3. **Configure Environment Variables**:
   ```bash
   cp env.example .env
   ```
   
   Edit `.env` with your actual credentials:
   ```env
   # Redis Configuration
   REDIS_URL=redis://localhost:6379/0
   
   # Cloudflare R2 Configuration
   R2_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
   R2_ACCESS_KEY_ID=your_r2_access_key_id
   R2_SECRET_ACCESS_KEY=your_r2_secret_access_key
   R2_BUCKET_NAME=creditcardsindia
   ```

## Usage

### Starting the Automation System

1. **Start Celery Services**:
   ```bash
   ./start_celery.sh
   ```
   
   This starts both the Celery worker and beat scheduler in the background.

2. **Check Status**:
   ```bash
   # Check if services are running
   ps aux | grep celery
   
   # View logs
   tail -f logs/celery_worker.log
   tail -f logs/celery_beat.log
   ```

### Manual Testing

1. **Test the Task**:
   ```bash
   python test_task.py
   ```
   
   Choose option 1 for a full scraping test or option 2 for a simple test.

2. **Monitor Tasks** (Optional):
   ```bash
   # Install Flower for web monitoring
   pip install flower
   
   # Start Flower
   celery -A celery_config flower
   ```
   
   Then visit `http://localhost:5555` in your browser.

### Stopping the System

```bash
./stop_celery.sh
```

## Configuration

### Subreddit Configuration

Edit the `SUBREDDIT_CONFIGS` list in `tasks.py` to modify what gets scraped:

```python
SUBREDDIT_CONFIGS = [
    {
        "name": "CreditCardsIndia",
        "category": "n",  # new posts
        "n_results": 100
    },
    {
        "name": "CreditCardsIndia", 
        "category": "h",  # hot posts
        "n_results": 50
    },
    # Add more configurations as needed
]
```

### Schedule Configuration

The task runs daily at 11:30 PM UTC. To change this, edit the `beat_schedule` in `celery_config.py`:

```python
beat_schedule={
    'daily-reddit-scrape': {
        'task': 'tasks.scrape_and_upload_to_r2',
        'schedule': crontab(hour=23, minute=30),  # 11:30 PM UTC
    },
},
```

## File Structure

```
├── celery_config.py      # Celery configuration and scheduling
├── tasks.py              # Main scraping and upload tasks
├── start_celery.sh       # Script to start Celery services
├── stop_celery.sh        # Script to stop Celery services
├── test_task.py          # Manual task testing script
├── env.example           # Environment variables template
├── logs/                 # Celery log files
│   ├── celery_worker.log
│   └── celery_beat.log
└── scrapes/              # Local scraping output (temporary)
```

## Data Flow

1. **Daily Trigger**: Celery beat scheduler triggers the task at 11:30 PM UTC
2. **Subreddit Scraping**: For each configured subreddit:
   - Scrapes posts using URS
   - Extracts submission URLs
   - Scrapes all comments from each submission
3. **Archive Creation**: Creates a compressed ZIP file of all scraped data
4. **Upload to R2**: Uploads the archive to the configured R2 bucket
5. **Cleanup**: Removes local archive file after successful upload

## Monitoring and Troubleshooting

### Log Files

- **Worker Log**: `logs/celery_worker.log` - Shows task execution details
- **Beat Log**: `logs/celery_beat.log` - Shows scheduling information

### Common Issues

1. **Redis Connection Error**:
   - Ensure Redis is running: `redis-cli ping`
   - Check REDIS_URL in `.env`

2. **R2 Upload Failures**:
   - Verify R2 credentials in `.env`
   - Check bucket permissions
   - Ensure bucket exists

3. **URS Scraping Issues**:
   - Verify Reddit API credentials are configured in URS
   - Check if the `urs/` directory exists and is properly set up

### Task Status

Check task status programmatically:

```python
from celery_config import app

# Get task result
result = app.AsyncResult('task-id-here')
print(result.status)
print(result.result)
```

## Security Notes

- Keep your `.env` file secure and never commit it to version control
- Use strong, unique credentials for R2 access
- Consider using IAM roles or more restrictive permissions for production
- Regularly rotate access keys

## Customization

### Adding More Subreddits

Add new configurations to `SUBREDDIT_CONFIGS` in `tasks.py`:

```python
{
    "name": "YourSubreddit",
    "category": "h",  # hot, new, top, rising, controversial
    "n_results": 50
}
```

### Changing Upload Structure

Modify the `object_key` in the `scrape_and_upload_to_r2` task:

```python
object_key = f"custom_path/{today}/reddit_data_{today}.zip"
```

### Adding Notifications

You can extend the task to send notifications on completion or failure by adding email/webhook functionality to the task.

## Support

For issues related to:
- **URS**: Check the URS documentation
- **Celery**: Refer to Celery documentation
- **R2**: Check Cloudflare R2 documentation 