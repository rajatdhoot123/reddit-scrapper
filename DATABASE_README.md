# Reddit Scraping Database Integration

This document explains the database schema and integration for the Reddit scraping system. The database stores all scraped content, metadata, and provides a queue system for downstream processing applications.

## 🏗️ Database Schema Overview

### Core Tables

#### 1. **Subreddits** (`subreddits`)
Master table for tracking subreddits being scraped.

```sql
- id (Primary Key)
- name (Unique, e.g., "CreditCardsIndia")
- display_name
- description
- subscribers
- created_utc
- is_active (Boolean flag)
- first_scraped_at
- last_scraped_at
- total_scrapes
```

#### 2. **Scrape Sessions** (`scrape_sessions`)
Records each scraping task/session with complete metadata.

```sql
- id (UUID, Primary Key)
- session_name
- task_id (Celery task ID)
- task_type (ENUM: scheduled, manual, legacy)
- config_id
- subreddit_id (Foreign Key)
- category (ENUM: h, n, t, r, c, s)
- n_results
- keywords (for search category)
- time_filter (ENUM: hour, day, week, month, year, all)
- status (ENUM: pending, running, success, failed, skipped)
- started_at, completed_at, duration_seconds
- submissions_found, submissions_scraped, comments_scraped
- scrape_file_path, archive_path, r2_object_key
- scrape_options (JSONB)
- error_message, retry_count
```

#### 3. **Submissions** (`submissions`)
Individual Reddit posts/submissions.

```sql
- id (UUID, Primary Key)
- reddit_id (Unique, e.g., "t3_abc123")
- title, url, permalink
- selftext, selftext_html
- author, author_flair
- created_utc, score, upvote_ratio, num_comments
- Boolean flags: is_self, is_original_content, is_nsfw, etc.
- link_flair_text, link_flair_css_class
- thumbnail, media_metadata (JSONB), gallery_data (JSONB)
- subreddit_id, scrape_session_id (Foreign Keys)
- processing_status (for downstream apps)
- processed_at, processing_metadata (JSONB)
- first_seen_at, last_updated_at
```

#### 4. **Comments** (`comments`)
Reddit comments on submissions.

```sql
- id (UUID, Primary Key)
- reddit_id (Unique, e.g., "t1_def456")
- body, body_html
- author, author_flair
- created_utc, score
- parent_id, link_id, depth (thread structure)
- Boolean flags: is_submitter, is_stickied
- distinguished, edited
- submission_id, subreddit_id (Foreign Keys)
- processing_status (for downstream apps)
- processed_at, processing_metadata (JSONB)
- first_seen_at, last_updated_at
```

#### 5. **Archives** (`archives`)
Information about created archive files.

```sql
- id (UUID, Primary Key)
- filename, archive_type, file_path, r2_object_key
- original_size_bytes, compressed_size_bytes, compression_ratio
- compression_level, file_count
- created_at, uploaded_at
- subreddits_included, date_range_start, date_range_end
- total_submissions, total_comments
- upload_metadata (JSONB)
- is_uploaded, is_deleted_locally
```

#### 6. **Processing Queue** (`processing_queue`)
Queue system for downstream processing applications.

```sql
- id (UUID, Primary Key)
- content_type (submission/comment)
- content_id (UUID reference)
- reddit_id (for easy lookup)
- priority (higher number = higher priority)
- processing_status (ENUM: pending, processing, completed, failed)
- processor_name (which downstream app)
- queued_at, started_processing_at, completed_at
- retry_count, max_retries
- processing_result (JSONB), error_message
```

#### 7. **Task Metrics** (`task_metrics`)
Performance metrics and monitoring.

```sql
- id (UUID, Primary Key)
- session_id (Foreign Key)
- metric_name, metric_value, metric_unit
- recorded_at
- task_metadata (JSONB)
```

### Indexes

The schema includes comprehensive indexes for optimal query performance:

- Primary and foreign key indexes
- `reddit_id` unique indexes for submissions and comments
- Date-based indexes for time-range queries
- Status indexes for filtering
- Author indexes for user-based queries
- Composite indexes for common query patterns

## 🚀 Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

Set the `DATABASE_URL` in your `.env` file:

```bash
# For PostgreSQL (recommended)
DATABASE_URL=postgresql://username:password@host:port/database_name

# Examples:
# Local: postgresql://user:pass@localhost:5432/reddit_scraper
# Supabase: postgresql://user:pass@db.xxx.supabase.co:5432/postgres
# Neon: postgresql://user:pass@ep-xxx.us-east-1.aws.neon.tech/dbname
```

### 3. Initialize Database

```bash
python setup_database.py --setup
```

### 4. Test Integration

```bash
python setup_database.py --info
```

## 📊 Usage Examples

### For Your Scraping System

The database integration is automatically enabled when `DATABASE_URL` is set. The scraping tasks will:

1. Create scrape session records
2. Save all submissions and comments to the database
3. Add items to the processing queue
4. Track archive information

### For Downstream Processing Apps

Use the `api_client.py` module to access the data:

```python
from api_client import get_reddit_api

# Initialize API
api = get_reddit_api()

# Get recent submissions
recent_posts = api.get_recent_submissions('CreditCardsIndia', limit=50)

# Search for specific content
card_posts = api.get_submissions_by_keywords(['credit card', 'cashback'])

# Get a submission with all comments
post_with_comments = api.get_submission_with_comments('abc123')

# Get pending processing items
pending_items = api.get_pending_processing_items(
    processor_name='my_app',
    content_type='submission',
    limit=100
)

# Process items
for item in pending_items:
    # Mark as processing
    api.mark_item_as_processing(item['id'], 'my_app')
    
    # Do your processing...
    result = process_content(item)
    
    # Mark as completed
    api.mark_item_as_completed(item['id'], result)
```

### Common Queries

#### Get Top Posts by Score
```python
top_posts = api.get_top_submissions_by_score(
    subreddit='CreditCardsIndia',
    days_back=7,
    limit=25
)
```

#### Get Content by Author
```python
author_content = api.get_content_by_author(
    author='u/username',
    content_type='both',  # submissions, comments, or both
    limit=100
)
```

#### Get Subreddit Statistics
```python
stats = api.get_subreddit_stats('CreditCardsIndia')
print(f"Total submissions: {stats['total_submissions']}")
print(f"Average score: {stats['avg_score']}")
print(f"Unique authors: {stats['unique_authors']}")
```

## 🔄 Processing Queue System

The processing queue enables downstream applications to efficiently process scraped content:

### Features
- **Priority-based processing**: Higher priority items processed first
- **Retry mechanism**: Failed items can be retried with backoff
- **Status tracking**: Complete lifecycle tracking
- **Multiple processors**: Different apps can process different content types
- **Result storage**: Processing results stored as JSON

### Workflow
1. Scraping system adds items to queue with appropriate priority
2. Downstream app queries for pending items
3. App marks items as "processing" to prevent duplicate work
4. App processes content and stores results
5. App marks items as "completed" or "failed"

## 📈 Performance Considerations

### Indexing Strategy
- All foreign keys are indexed
- Frequently queried columns have dedicated indexes
- Composite indexes for common query patterns

### Partitioning (Future Enhancement)
For high-volume installations, consider partitioning by:
- Date ranges (monthly/yearly partitions)
- Subreddit (if tracking many subreddits)

### Connection Pooling
The `DatabaseManager` class includes connection pooling:
- Pool size: 10 connections
- Max overflow: 20 connections
- Pre-ping enabled for connection health

## 🛠️ Maintenance

### Regular Tasks

#### Database Statistics
```bash
python setup_database.py --info
```

#### Cleanup Old Records
Consider implementing cleanup jobs for:
- Old processing queue items (completed > 30 days)
- Old task metrics (> 90 days)
- Archived session data (> 1 year)

#### Monitor Performance
- Track query performance
- Monitor connection pool usage
- Set up alerts for failed processing items

### Backup Strategy
- Regular database backups
- Archive old data to cold storage
- Test restore procedures

## 🔧 Customization

### Adding Custom Fields
To add custom fields to any model:

1. Update the model in `models.py`
2. Create a migration script
3. Update the API client if needed

### Custom Processing Apps
Create your own processing app:

```python
from api_client import get_reddit_api

class MyCustomProcessor:
    def __init__(self):
        self.api = get_reddit_api()
        self.processor_name = "my_custom_app"
    
    def process_submissions(self):
        items = self.api.get_pending_processing_items(
            processor_name=self.processor_name,
            content_type='submission'
        )
        
        for item in items:
            self.api.mark_item_as_processing(item['id'], self.processor_name)
            
            # Your custom processing logic here
            result = self.custom_process(item)
            
            self.api.mark_item_as_completed(item['id'], result)
```

## 🚨 Troubleshooting

### Common Issues

#### Connection Errors
- Verify `DATABASE_URL` is correct
- Check network connectivity
- Ensure database server is running

#### Performance Issues
- Check index usage with `EXPLAIN` queries
- Monitor connection pool exhaustion
- Consider query optimization

#### Data Integrity
- Unique constraint violations indicate duplicate processing
- Foreign key errors suggest data synchronization issues

### Logging
Enable SQL debugging:
```python
# In database_integration.py, set echo=True
engine = create_engine(database_url, echo=True)
```

## 📚 Additional Resources

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/current/performance-tips.html)
- [Database Design Best Practices](https://www.postgresql.org/docs/current/ddl.html) 