#!/usr/bin/env python3

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from models import (
    Base, Subreddit, ScrapeSession, Submission, Comment, Archive, 
    ProcessingQueue, TaskMetrics, ScrapeStatus, CategoryType, TaskType, 
    TimeFilter, ProcessingStatus, 
    create_comment_from_reddit_data, add_to_processing_queue
)

# Set up logging
logger = logging.getLogger(__name__)

def convert_urs_category_to_db_category(category: str) -> str:
    """Convert URS category (uppercase letter) to database CategoryType enum value (lowercase letter)"""
    # URS uses uppercase letters, database expects lowercase
    category_mapping = {
        'H': 'h',  # Hot
        'N': 'n',  # New
        'T': 't',  # Top
        'R': 'r',  # Rising
        'C': 'c',  # Controversial
        'S': 's',  # Search
        # Also handle lowercase input
        'h': 'h',
        'n': 'n',
        't': 't',
        'r': 'r',
        'c': 'c',
        's': 's'
    }
    
    return category_mapping.get(category, category.lower())

class DatabaseManager:
    """Manages database connections and operations for Reddit scraping"""
    
    def __init__(self, database_url: str = None):
        if database_url is None:
            database_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/reddit_scraper')
        
        self.engine = create_engine(
            database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False
        )
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database initialized successfully")
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()
    
    def get_or_create_subreddit(self, session, name: str, **kwargs) -> Subreddit:
        """Get or create a subreddit record with enhanced metadata"""
        subreddit = session.query(Subreddit).filter_by(name=name).first()
        if not subreddit:
            # Create subreddit with provided metadata
            subreddit_data = {
                'name': name,
                'display_name': kwargs.get('display_name', name),
                'description': kwargs.get('description'),
                'subscribers': kwargs.get('subscribers'),
                'created_utc': kwargs.get('created_utc'),
                'is_active': kwargs.get('is_active', True)
            }
            
            # Remove None values
            subreddit_data = {k: v for k, v in subreddit_data.items() if v is not None}
            
            subreddit = Subreddit(**subreddit_data)
            session.add(subreddit)
            session.flush()
            logger.info(f"Created new subreddit record: r/{name} with metadata")
        else:
            # Update existing subreddit with new metadata if provided
            updated = False
            for key, value in kwargs.items():
                if value is not None and hasattr(subreddit, key):
                    if getattr(subreddit, key) != value:
                        setattr(subreddit, key, value)
                        updated = True
            
            if updated:
                subreddit.last_scraped_at = datetime.now()
                logger.debug(f"Updated subreddit metadata for r/{name}")
        
        return subreddit


class ScrapingDataProcessor:
    """Processes scraped data and saves it to the database"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        
    def create_scrape_session(self, task_id: str, task_type: str, subreddit_name: str, 
                            category: str, config: Dict) -> str:
        """Create a new scrape session record"""
        
        with self.db.get_session() as session:
            # Get or create subreddit
            subreddit = self.db.get_or_create_subreddit(session, subreddit_name)
            
            # Create scrape session
            scrape_session = ScrapeSession(
                task_id=task_id,
                task_type=TaskType(task_type),
                subreddit_id=subreddit.id,
                category=CategoryType(convert_urs_category_to_db_category(category)),
                n_results=config.get('n_results'),
                keywords=config.get('keywords'),
                time_filter=TimeFilter(config['time_filter']) if config.get('time_filter') else None,
                scrape_options=config.get('options', {}),
                status=ScrapeStatus.PENDING
            )
            
            session.add(scrape_session)
            session.flush()
            
            session_id = str(scrape_session.id)
            logger.info(f"Created scrape session: {session_id}")
            return session_id
    
    def update_scrape_session_status(self, session_id: str, status: str, **kwargs):
        """Update scrape session status and metadata"""
        
        with self.db.get_session() as session:
            scrape_session = session.query(ScrapeSession).filter_by(id=session_id).first()
            if scrape_session:
                scrape_session.status = ScrapeStatus(status)
                
                # Update timing
                if status == "running":
                    scrape_session.started_at = datetime.now()
                elif status in ["success", "failed"]:
                    scrape_session.completed_at = datetime.now()
                    if scrape_session.started_at:
                        duration = (scrape_session.completed_at - scrape_session.started_at).total_seconds()
                        scrape_session.duration_seconds = int(duration)
                
                # Update other fields
                for key, value in kwargs.items():
                    if hasattr(scrape_session, key):
                        setattr(scrape_session, key, value)
                
                logger.info(f"Updated scrape session {session_id}: status={status}")
    
    def process_scraped_submissions(self, session_id: str, json_file_path: Path) -> int:
        """Process scraped submissions from JSON file and save to database"""
        
        if not json_file_path.exists():
            logger.error(f"Scrape file not found: {json_file_path}")
            return 0
        
        processed_count = 0
        
        with self.db.get_session() as session:
            try:
                with open(json_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract subreddit name from scrape_settings
                scrape_settings = data.get('scrape_settings', {})
                subreddit_name = scrape_settings.get('subreddit')
                
                if not subreddit_name:
                    logger.error("No subreddit name found in scrape_settings")
                    return 0
                
                # Get or create subreddit record with basic info
                subreddit = self.db.get_or_create_subreddit(session, subreddit_name)
                
                # Handle both direct data array and wrapped data structure
                submissions_data = data.get('data', data) if isinstance(data, dict) else data
                
                for submission_data in submissions_data:
                    try:
                        # Check if submission already exists
                        existing = session.query(Submission).filter_by(
                            reddit_id=submission_data['id']
                        ).first()
                        
                        if existing:
                            logger.debug(f"Submission {submission_data['id']} already exists, skipping")
                            continue
                        
                        # Create submission record using the subreddit we already have
                        submission = self._create_submission_from_data(
                            session, submission_data, session_id, subreddit.id
                        )
                        session.add(submission)
                        session.flush()  # Flush to get the submission ID
                        
                        # Add to processing queue for downstream apps
                        add_to_processing_queue(
                            session, "submission", str(submission.id), 
                            submission_data['id'], priority=1
                        )
                        
                        processed_count += 1
                        
                        if processed_count % 10 == 0:
                            session.flush()  # Periodic flush for large datasets
                            
                    except Exception as e:
                        logger.error(f"Error processing submission {submission_data.get('id', 'unknown')}: {e}")
                        continue
                
                # Update scrape session with results
                self.update_scrape_session_status(
                    session_id, "success",
                    submissions_found=len(submissions_data),
                    submissions_scraped=processed_count,
                    scrape_file_path=str(json_file_path)
                )
                
                logger.info(f"Processed {processed_count} submissions from {json_file_path}")
                
                # Delete the processed file after successful database save
                if processed_count > 0:
                    self._safe_delete_file(json_file_path, "submissions")
                
            except Exception as e:
                logger.error(f"Error processing submissions file {json_file_path}: {e}")
                self.update_scrape_session_status(
                    session_id, "failed",
                    error_message=str(e)
                )
                
        return processed_count
    
    def _create_submission_from_data(self, session, submission_data: dict, 
                                   scrape_session_id: str, subreddit_id: int) -> Submission:
        """Create a Submission record from scraped data"""
        
        submission = Submission(
            reddit_id=submission_data['id'],
            title=submission_data['title'],
            url=submission_data['url'],
            permalink=submission_data['permalink'],
            selftext=submission_data.get('selftext', ''),
            author=submission_data.get('author', '[deleted]'),
            created_utc=datetime.fromisoformat(submission_data['created_utc']),
            score=submission_data.get('score', 0),
            upvote_ratio=submission_data.get('upvote_ratio'),
            num_comments=submission_data.get('num_comments', 0),
            is_self=submission_data.get('is_self', False),
            is_original_content=submission_data.get('is_original_content', False),
            is_nsfw=submission_data.get('nsfw', False),
            is_spoiler=submission_data.get('spoiler', False),
            is_stickied=submission_data.get('stickied', False),
            is_locked=submission_data.get('locked', False),
            distinguished=submission_data.get('distinguished'),
            link_flair_text=submission_data.get('link_flair_text'),
            subreddit_id=subreddit_id,
            scrape_session_id=scrape_session_id
        )
        
        return submission
    
    def process_scraped_comments(self, submission_reddit_id: str, comments_file_path: Path) -> int:
        """Process scraped comments from JSON file and save to database"""
        
        if not comments_file_path.exists():
            logger.error(f"Comments file not found: {comments_file_path}")
            return 0
        
        processed_count = 0
        
        with self.db.get_session() as session:
            try:
                # Find the submission in database
                submission = session.query(Submission).filter_by(
                    reddit_id=submission_reddit_id
                ).first()
                
                if not submission:
                    logger.error(f"Submission {submission_reddit_id} not found in database")
                    return 0
                
                with open(comments_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract comments from the structured data
                comments_data = data.get('data', {}).get('comments', [])
                
                if isinstance(comments_data, list):
                    for comment_data in comments_data:
                        try:
                            # Check if comment already exists
                            existing = session.query(Comment).filter_by(
                                reddit_id=comment_data['id']
                            ).first()
                            
                            if existing:
                                continue
                            
                            # Create comment record
                            comment = create_comment_from_reddit_data(
                                session, comment_data, str(submission.id)
                            )
                            comment.subreddit_id = submission.subreddit_id
                            session.add(comment)
                            session.flush()  # Flush to get the comment ID
                            
                            # Add to processing queue
                            add_to_processing_queue(
                                session, "comment", str(comment.id),
                                comment_data['id'], priority=0
                            )
                            
                            processed_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing comment {comment_data.get('id', 'unknown')}: {e}")
                            continue
                
                logger.info(f"Processed {processed_count} comments for submission {submission_reddit_id}")
                
                # Delete the processed comment file after successful database save
                if processed_count > 0:
                    self._safe_delete_file(comments_file_path, "comments")
                
            except Exception as e:
                logger.error(f"Error processing comments file {comments_file_path}: {e}")
                
        return processed_count
    
    def create_archive_record(self, archive_path: Path, archive_type: str, 
                            r2_object_key: str, metadata: Dict) -> str:
        """Create an archive record in the database"""
        
        with self.db.get_session() as session:
            archive = Archive(
                filename=archive_path.name,
                archive_type=archive_type,
                file_path=str(archive_path),
                r2_object_key=r2_object_key,
                compressed_size_bytes=archive_path.stat().st_size,
                upload_metadata=metadata,
                is_uploaded=True,
                uploaded_at=datetime.now(),
                subreddits_included=metadata.get('subreddits', ''),
                total_submissions=metadata.get('total_submissions', 0),
                total_comments=metadata.get('total_comments_scraped', 0)
            )
            
            session.add(archive)
            session.flush()
            
            archive_id = str(archive.id)
            logger.info(f"Created archive record: {archive_id}")
            return archive_id
    
    def get_pending_content_for_processing(self, processor_name: str = None, 
                                         content_type: str = None, limit: int = 100) -> List[Dict]:
        """Get pending content items for downstream processing"""
        
        with self.db.get_session() as session:
            query = session.query(ProcessingQueue).filter_by(
                processing_status=ProcessingStatus.PENDING
            )
            
            if processor_name:
                query = query.filter_by(processor_name=processor_name)
            
            if content_type:
                query = query.filter_by(content_type=content_type)
            
            items = query.order_by(ProcessingQueue.priority.desc(), 
                                 ProcessingQueue.queued_at).limit(limit).all()
            
            results = []
            for item in items:
                results.append({
                    'id': str(item.id),
                    'content_type': item.content_type,
                    'content_id': str(item.content_id),
                    'reddit_id': item.reddit_id,
                    'priority': item.priority,
                    'queued_at': item.queued_at.isoformat()
                })
            
            return results
    
    def mark_content_as_processing(self, queue_id: str, processor_name: str):
        """Mark content as currently being processed"""
        
        with self.db.get_session() as session:
            item = session.query(ProcessingQueue).filter_by(id=queue_id).first()
            if item:
                item.processing_status = ProcessingStatus.PROCESSING
                item.processor_name = processor_name
                item.started_processing_at = datetime.now()
    
    def mark_content_as_completed(self, queue_id: str, result: Dict = None):
        """Mark content processing as completed"""
        
        with self.db.get_session() as session:
            item = session.query(ProcessingQueue).filter_by(id=queue_id).first()
            if item:
                item.processing_status = ProcessingStatus.COMPLETED
                item.completed_at = datetime.now()
                if result:
                    item.processing_result = result
    
    def _safe_delete_file(self, file_path: Path, content_type: str) -> bool:
        """Safely delete a processed file with proper error handling"""
        try:
            if file_path.exists():
                file_size = file_path.stat().st_size
                file_path.unlink()
                logger.info(f"Deleted processed {content_type} file: {file_path.name} ({file_size:,} bytes)")
                return True
            else:
                logger.warning(f"File not found for deletion: {file_path}")
                return False
        except Exception as e:
            logger.error(f"Failed to delete {content_type} file {file_path}: {e}")
            return False


# Integration functions for use in tasks.py
def initialize_database_integration():
    """Initialize database integration for the scraping system"""
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        logger.warning("DATABASE_URL not set, database integration disabled")
        return None
    
    try:
        db_manager = DatabaseManager(database_url)
        processor = ScrapingDataProcessor(db_manager)
        
        # Test connection
        with db_manager.get_session() as session:
            session.execute(text("SELECT 1"))
        
        logger.info("Database integration initialized successfully")
        return processor
        
    except Exception as e:
        logger.error(f"Failed to initialize database integration: {e}")
        return None


def save_scraping_results_to_db(processor: ScrapingDataProcessor, task_id: str, 
                               task_type: str, config: Dict, result: Dict, 
                               scrape_file: Path = None):
    """Save scraping results to database"""
    
    if not processor:
        logger.debug("Database processor not available, skipping database save")
        return
    
    try:
        # Create scrape session
        session_id = processor.create_scrape_session(
            task_id=task_id,
            task_type=task_type,
            subreddit_name=config['name'],
            category=config['category'],
            config=config
        )
        
        files_cleaned_up = []
        total_submissions_processed = 0
        total_comments_processed = 0
        
        # Process submissions if scrape file exists
        if scrape_file and scrape_file.exists():
            # Extract submission data BEFORE processing (since processing deletes the file)
            submissions_data = extract_submissions_data_from_file(scrape_file)
            
            # Process submissions to database
            submissions_count = processor.process_scraped_submissions(session_id, scrape_file)
            total_submissions_processed = submissions_count
            
            # Track if the main scrape file was cleaned up
            if submissions_count > 0 and not scrape_file.exists():
                files_cleaned_up.append(str(scrape_file))
            
            # Process comments using the pre-extracted submission data
            comments_processed = 0
            
            for submission in submissions_data:
                reddit_id = submission.get('id')
                
                if reddit_id:
                    # Find comment file by Reddit ID (much more reliable than title matching)
                    comments_file = find_comments_file_by_reddit_id(reddit_id)
                    
                    if comments_file:
                        processed = processor.process_scraped_comments(reddit_id, comments_file)
                        comments_processed += processed
                        
                        # Track if comment file was cleaned up
                        if processed > 0 and not comments_file.exists():
                            files_cleaned_up.append(str(comments_file))
                        
                        logger.info(f"Processed {processed} comments for submission {reddit_id} from {comments_file}")
                    else:
                        logger.debug(f"No comments file found for submission {reddit_id}")
            
            total_comments_processed = comments_processed
            logger.info(f"Total comments processed and saved to database: {comments_processed}")
            
            # Log cleanup summary
            if files_cleaned_up:
                logger.info(f"Successfully cleaned up {len(files_cleaned_up)} processed files after database save")
                for file_path in files_cleaned_up:
                    logger.debug(f"Cleaned up: {file_path}")
        
        # Update session with final results
        processor.update_scrape_session_status(
            session_id, result['status'],
            submissions_found=result.get('submissions_found', 0),
            comments_scraped=total_comments_processed,
            error_message=result.get('error') if result['status'] == 'failed' else None
        )
        
        logger.info(f"Saved scraping results to database for session {session_id} "
                   f"(submissions: {total_submissions_processed}, comments: {total_comments_processed}, "
                   f"files cleaned: {len(files_cleaned_up)})")
        
    except Exception as e:
        logger.error(f"Failed to save scraping results to database: {e}")


def extract_submissions_data_from_file(json_file: Path) -> List[Dict[str, Any]]:
    """Extract submissions data (list of dicts) from a JSON scrape file."""
    if not json_file.exists():
        logger.error(f"Scrape file not found for data extraction: {json_file}")
        return []
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data_content = json.load(f)
        
        # Handle common URS structures:
        # 1. {'scrape_settings': ..., 'data': [submissions]}
        # 2. [submissions] (direct list)
        extracted_data = data_content.get('data', data_content) if isinstance(data_content, dict) else data_content
        
        if not isinstance(extracted_data, list):
            logger.warning(f"Expected a list of submissions from {json_file}, but found type {type(extracted_data)}. File content might be malformed or not a list of submissions.")
            return []
            
        # Ensure all items in the list are dictionaries, as expected for submission data
        if not all(isinstance(item, dict) for item in extracted_data):
            logger.warning(f"Data extracted from {json_file} is a list, but not all items are dictionaries as expected for submissions.")
            # Depending on strictness, you might want to filter non-dict items or return empty
            return [item for item in extracted_data if isinstance(item, dict)]

        return extracted_data
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {json_file}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error extracting submissions data from {json_file}: {e}")
        return []


def extract_submission_urls_from_file(json_file: Path) -> List[str]:
    """Extract submission URLs from scrape file"""
    try:
        with open(json_file) as f:
            data = json.load(f)
        
        urls = []
        submissions_data = data.get('data', data) if isinstance(data, dict) else data
        
        for post in submissions_data:
            url = f"https://www.reddit.com{post['permalink']}"
            urls.append(url)
        
        return urls
    except Exception as e:
        logger.error(f"Error extracting URLs from {json_file}: {e}")
        return []


def extract_reddit_id_from_url(url: str) -> str:
    """Extract Reddit submission ID from URL"""
    # Reddit URLs are like: https://www.reddit.com/r/subreddit/comments/abc123/title/
    parts = url.split('/')
    for i, part in enumerate(parts):
        if part == 'comments' and i + 1 < len(parts):
            return parts[i + 1]
    return ""


def find_comments_file_by_reddit_id(reddit_id: str) -> Optional[Path]:
    """Find comments file for a specific submission by Reddit ID (checking file content)"""
    # Search across the last 3 days to handle timezone/timing issues
    for days_back in range(3):
        search_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        comments_dir = Path(f"scrapes/{search_date}/comments")
        
        if not comments_dir.exists():
            logger.debug(f"Comments directory does not exist: {comments_dir}")
            continue
        
        logger.debug(f"Searching for Reddit ID {reddit_id} in {comments_dir}")
        
        # Look through all comment files and check their content
        for file_path in comments_dir.glob("*.json"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check if the URL in scrape_settings contains our Reddit ID
                url = data.get('scrape_settings', {}).get('url', '')
                if reddit_id in url:
                    logger.debug(f"Found comment file for Reddit ID {reddit_id}: {file_path}")
                    return file_path
                    
                # Also check permalink in submission_metadata as backup
                permalink = data.get('data', {}).get('submission_metadata', {}).get('permalink', '')
                if reddit_id in permalink:
                    logger.debug(f"Found comment file for Reddit ID {reddit_id} via permalink: {file_path}")
                    return file_path
                    
            except (json.JSONDecodeError, IOError) as e:
                logger.debug(f"Could not read comment file {file_path}: {e}")
                continue
    
    logger.debug(f"No comment file found for Reddit ID: {reddit_id}")
    return None


# Global processor instance
_db_processor = None

def get_database_processor():
    """Get the global database processor instance"""
    global _db_processor
    if _db_processor is None:
        _db_processor = initialize_database_integration()
    return _db_processor 