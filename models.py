#!/usr/bin/env python3

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Boolean, JSON, 
    ForeignKey, Index, UniqueConstraint, CheckConstraint,
    Float, BigInteger, Enum as SQLEnum
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from datetime import datetime
import enum
import uuid

Base = declarative_base()

# Enums for better data integrity
class ScrapeStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"
    RUNNING = "running"

class CategoryType(enum.Enum):
    HOT = "h"
    NEW = "n" 
    TOP = "t"
    RISING = "r"
    CONTROVERSIAL = "c"
    SEARCH = "s"

class TaskType(enum.Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"

class TimeFilter(enum.Enum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"

class ProcessingStatus(enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


# Core models
class Subreddit(Base):
    """Master table for subreddits being tracked"""
    __tablename__ = "subreddits"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False, index=True)
    display_name = Column(String(50))
    description = Column(Text)
    subscribers = Column(BigInteger)
    created_utc = Column(DateTime)
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Tracking metadata
    first_scraped_at = Column(DateTime, default=func.now())
    last_scraped_at = Column(DateTime)
    total_scrapes = Column(Integer, default=0)
    
    # Relationships
    scrape_sessions = relationship("ScrapeSession", back_populates="subreddit")
    submissions = relationship("Submission", back_populates="subreddit")
    
    __table_args__ = (
        Index('idx_subreddit_name', 'name'),
        Index('idx_subreddit_active', 'is_active'),
    )


class ScrapeSession(Base):
    """Individual scraping sessions/tasks"""
    __tablename__ = "scrape_sessions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_name = Column(String(200))  # Descriptive name for the session
    
    # Task metadata
    task_id = Column(String(100))  # Celery task ID
    task_type = Column(SQLEnum(TaskType), nullable=False)
    config_id = Column(Integer)  # Reference to config used
    
    # Scraping parameters
    subreddit_id = Column(Integer, ForeignKey("subreddits.id"), nullable=False)
    category = Column(SQLEnum(CategoryType), nullable=False)
    n_results = Column(Integer)
    keywords = Column(String(500))  # For search category
    time_filter = Column(SQLEnum(TimeFilter))
    
    # Session status and timing
    status = Column(SQLEnum(ScrapeStatus), default=ScrapeStatus.PENDING, nullable=False)
    started_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Results summary
    submissions_found = Column(Integer, default=0)
    submissions_scraped = Column(Integer, default=0)
    comments_scraped = Column(Integer, default=0)
    total_content_items = Column(Integer, default=0)
    
    # File and archive info
    scrape_file_path = Column(String(500))
    archive_path = Column(String(500))
    r2_object_key = Column(String(500))
    file_format = Column(String(10), default="json")  # json, csv
    
    # Configuration used
    scrape_options = Column(JSONB)  # Store the options dict as JSON
    
    # Error handling
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    
    # Relationships
    subreddit = relationship("Subreddit", back_populates="scrape_sessions")
    submissions = relationship("Submission", back_populates="scrape_session")
    
    __table_args__ = (
        Index('idx_scrape_session_status', 'status'),
        Index('idx_scrape_session_started', 'started_at'),
        Index('idx_scrape_session_subreddit', 'subreddit_id'),
        Index('idx_scrape_session_task_type', 'task_type'),
        Index('idx_scrape_session_category', 'category'),
    )


class Submission(Base):
    """Reddit submissions/posts"""
    __tablename__ = "submissions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Reddit metadata
    reddit_id = Column(String(20), unique=True, nullable=False, index=True)  # t3_xxxxx
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=False)
    permalink = Column(String(500), nullable=False)
    selftext = Column(Text)
    selftext_html = Column(Text)
    
    # Author info
    author = Column(String(50))  # Can be [deleted]
    author_flair = Column(String(200))
    
    # Submission metadata
    created_utc = Column(DateTime, nullable=False, index=True)
    score = Column(Integer, default=0)
    upvote_ratio = Column(Float)
    num_comments = Column(Integer, default=0)
    
    # Flags
    is_self = Column(Boolean, default=False)
    is_original_content = Column(Boolean, default=False)
    is_nsfw = Column(Boolean, default=False)
    is_spoiler = Column(Boolean, default=False)
    is_stickied = Column(Boolean, default=False)
    is_locked = Column(Boolean, default=False)
    distinguished = Column(String(20))  # moderator, admin, etc.
    
    # Content classification
    link_flair_text = Column(String(200))
    link_flair_css_class = Column(String(100))
    
    # Media info
    thumbnail = Column(String(500))
    media_metadata = Column(JSONB)
    gallery_data = Column(JSONB)
    
    # Relationships
    subreddit_id = Column(Integer, ForeignKey("subreddits.id"), nullable=False)
    scrape_session_id = Column(UUID(as_uuid=True), ForeignKey("scrape_sessions.id"), nullable=False)
    
    # Processing status for downstream apps
    processing_status = Column(SQLEnum(ProcessingStatus), default=ProcessingStatus.PENDING)
    processed_at = Column(DateTime)
    processing_metadata = Column(JSONB)
    
    # Tracking
    first_seen_at = Column(DateTime, default=func.now())
    last_updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    subreddit = relationship("Subreddit", back_populates="submissions")
    scrape_session = relationship("ScrapeSession", back_populates="submissions")
    comments = relationship("Comment", back_populates="submission")
    
    __table_args__ = (
        Index('idx_submission_reddit_id', 'reddit_id'),
        Index('idx_submission_created_utc', 'created_utc'),
        Index('idx_submission_score', 'score'),
        Index('idx_submission_subreddit', 'subreddit_id'),
        Index('idx_submission_processing_status', 'processing_status'),
        Index('idx_submission_author', 'author'),
        UniqueConstraint('reddit_id', name='uq_submission_reddit_id'),
    )


class Comment(Base):
    """Reddit comments"""
    __tablename__ = "comments"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Reddit metadata
    reddit_id = Column(String(20), unique=True, nullable=False, index=True)  # t1_xxxxx
    body = Column(Text, nullable=False)
    body_html = Column(Text)
    
    # Author info
    author = Column(String(50))  # Can be [deleted]
    author_flair = Column(String(200))
    
    # Comment metadata
    created_utc = Column(DateTime, nullable=False, index=True)
    score = Column(Integer, default=0)
    
    # Thread structure
    parent_id = Column(String(20), nullable=False)  # Parent comment or submission ID
    link_id = Column(String(20), nullable=False)    # Always the submission ID
    depth = Column(Integer, default=0)              # Comment nesting level
    
    # Flags
    is_submitter = Column(Boolean, default=False)  # Is the submission author
    is_stickied = Column(Boolean, default=False)
    distinguished = Column(String(20))
    edited = Column(DateTime)  # When comment was edited, if ever
    
    # Relationships
    submission_id = Column(UUID(as_uuid=True), ForeignKey("submissions.id"), nullable=False)
    subreddit_id = Column(Integer, ForeignKey("subreddits.id"), nullable=False)
    
    # Processing status for downstream apps
    processing_status = Column(SQLEnum(ProcessingStatus), default=ProcessingStatus.PENDING)
    processed_at = Column(DateTime)
    processing_metadata = Column(JSONB)
    
    # Tracking
    first_seen_at = Column(DateTime, default=func.now())
    last_updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    submission = relationship("Submission", back_populates="comments")
    subreddit = relationship("Subreddit")
    
    __table_args__ = (
        Index('idx_comment_reddit_id', 'reddit_id'),
        Index('idx_comment_submission', 'submission_id'),
        Index('idx_comment_created_utc', 'created_utc'),
        Index('idx_comment_score', 'score'),
        Index('idx_comment_processing_status', 'processing_status'),
        Index('idx_comment_parent_id', 'parent_id'),
        Index('idx_comment_author', 'author'),
        UniqueConstraint('reddit_id', name='uq_comment_reddit_id'),
    )


class Archive(Base):
    """Archive files created from scraping sessions"""
    __tablename__ = "archives"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Archive metadata
    filename = Column(String(500), nullable=False)
    archive_type = Column(String(50), nullable=False)  # daily, weekly, manual, etc.
    file_path = Column(String(500))  # Local path before upload
    r2_object_key = Column(String(500), unique=True)  # R2 storage key
    
    # Size and compression info
    original_size_bytes = Column(BigInteger)
    compressed_size_bytes = Column(BigInteger)
    compression_ratio = Column(Float)
    compression_level = Column(Integer)
    file_count = Column(Integer)
    
    # Timing
    created_at = Column(DateTime, default=func.now())
    uploaded_at = Column(DateTime)
    
    # Content summary
    subreddits_included = Column(String(500))  # Comma-separated list
    date_range_start = Column(DateTime)
    date_range_end = Column(DateTime)
    total_submissions = Column(Integer, default=0)
    total_comments = Column(Integer, default=0)
    
    # Upload metadata
    upload_metadata = Column(JSONB)
    
    # Status
    is_uploaded = Column(Boolean, default=False)
    is_deleted_locally = Column(Boolean, default=False)
    
    __table_args__ = (
        Index('idx_archive_created_at', 'created_at'),
        Index('idx_archive_r2_key', 'r2_object_key'),
        Index('idx_archive_type', 'archive_type'),
        Index('idx_archive_uploaded', 'is_uploaded'),
    )


class ProcessingQueue(Base):
    """Queue for downstream processing of scraped content"""
    __tablename__ = "processing_queue"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # What to process
    content_type = Column(String(20), nullable=False)  # submission, comment
    content_id = Column(UUID(as_uuid=True), nullable=False)  # FK to submission or comment
    reddit_id = Column(String(20), nullable=False, index=True)  # For easy lookup
    
    # Processing metadata
    priority = Column(Integer, default=0)  # Higher number = higher priority
    processing_status = Column(SQLEnum(ProcessingStatus), default=ProcessingStatus.PENDING)
    processor_name = Column(String(100))  # Which downstream app/service
    
    # Timing
    queued_at = Column(DateTime, default=func.now())
    started_processing_at = Column(DateTime)
    completed_at = Column(DateTime)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Results
    processing_result = Column(JSONB)
    error_message = Column(Text)
    
    __table_args__ = (
        Index('idx_processing_queue_status', 'processing_status'),
        Index('idx_processing_queue_priority', 'priority'),
        Index('idx_processing_queue_queued_at', 'queued_at'),
        Index('idx_processing_queue_content', 'content_type', 'content_id'),
        Index('idx_processing_queue_reddit_id', 'reddit_id'),
    )


class TaskMetrics(Base):
    """Metrics and performance tracking for scraping tasks"""
    __tablename__ = "task_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Task identification
    session_id = Column(UUID(as_uuid=True), ForeignKey("scrape_sessions.id"), nullable=False)
    metric_name = Column(String(100), nullable=False)
    metric_value = Column(Float, nullable=False)
    metric_unit = Column(String(20))  # seconds, bytes, count, etc.
    
    # Timing
    recorded_at = Column(DateTime, default=func.now())
    
    # Context
    task_metadata = Column(JSONB)
    
    __table_args__ = (
        Index('idx_task_metrics_session', 'session_id'),
        Index('idx_task_metrics_name', 'metric_name'),
        Index('idx_task_metrics_recorded', 'recorded_at'),
    )


# Views for common queries (these would be database views)
class ActiveScrapeStats(Base):
    """Materialized view for active scraping statistics"""
    __tablename__ = "active_scrape_stats"
    
    subreddit_name = Column(String(50), primary_key=True)
    total_sessions = Column(Integer)
    successful_sessions = Column(Integer)
    last_successful_scrape = Column(DateTime)
    total_submissions = Column(Integer)
    total_comments = Column(Integer)
    avg_session_duration = Column(Float)
    
    # This would be a materialized view in PostgreSQL
    __table_args__ = {'info': {'is_view': True}}


# Configuration for database connection
class DatabaseConfig:
    """Database configuration and session management"""
    
    def __init__(self, database_url: str):
        from sqlalchemy import create_engine
        
        self.engine = create_engine(
            database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False  # Set to True for SQL debugging
        )
        
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(bind=self.engine)
    
    def get_session(self):
        """Get a database session"""
        return self.SessionLocal()


# Utility functions for integration
def create_submission_from_reddit_data(session, submission_data: dict, scrape_session_id: str) -> Submission:
    """Create a Submission record from Reddit API data"""
    
    # Get or create subreddit
    subreddit = session.query(Subreddit).filter_by(name=submission_data['subreddit']).first()
    if not subreddit:
        subreddit = Subreddit(
            name=submission_data['subreddit'],
            display_name=submission_data['subreddit']
        )
        session.add(subreddit)
        session.flush()
    
    # Create submission
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
        subreddit_id=subreddit.id,
        scrape_session_id=scrape_session_id
    )
    
    return submission


def create_comment_from_reddit_data(session, comment_data: dict, submission_id: str) -> Comment:
    """Create a Comment record from Reddit API data"""
    
    comment = Comment(
        reddit_id=comment_data['id'],
        body=comment_data['body'],
        body_html=comment_data.get('body_html'),
        author=comment_data.get('author', '[deleted]'),
        created_utc=datetime.fromisoformat(comment_data['created_utc']),
        score=comment_data.get('score', 0),
        parent_id=comment_data['parent_id'],
        link_id=comment_data['link_id'],
        is_submitter=comment_data.get('is_submitter', False),
        is_stickied=comment_data.get('stickied', False),
        distinguished=comment_data.get('distinguished'),
        submission_id=submission_id
    )
    
    return comment


def add_to_processing_queue(session, content_type: str, content_id: str, reddit_id: str, 
                           priority: int = 0, processor_name: str = None):
    """Add content to processing queue for downstream apps"""
    
    queue_item = ProcessingQueue(
        content_type=content_type,
        content_id=content_id,
        reddit_id=reddit_id,
        priority=priority,
        processor_name=processor_name
    )
    
    session.add(queue_item)
    return queue_item 