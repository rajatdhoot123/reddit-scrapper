#!/usr/bin/env python3
"""
API Client for Reddit Scraping Database
This module provides a simple interface for downstream applications to access
scraped Reddit data from the database.
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from sqlalchemy.orm import joinedload
from sqlalchemy import desc, and_, or_, func

from database_integration import DatabaseManager
from models import (
    Subreddit, ScrapeSession, Submission, Comment, Archive, 
    ProcessingQueue, ScrapeStatus, ProcessingStatus, CategoryType
)


class RedditDataAPI:
    """API for accessing Reddit scraping data"""
    
    def __init__(self, database_url: str = None):
        self.db = DatabaseManager(database_url)
    
    def get_recent_submissions(self, subreddit: str = None, limit: int = 100, 
                             days_back: int = 7) -> List[Dict]:
        """Get recent submissions, optionally filtered by subreddit"""
        
        with self.db.get_session() as session:
            query = session.query(Submission).options(
                joinedload(Submission.subreddit),
                joinedload(Submission.scrape_session)
            )
            
            # Filter by date
            cutoff_date = datetime.now() - timedelta(days=days_back)
            query = query.filter(Submission.created_utc >= cutoff_date)
            
            # Filter by subreddit if specified
            if subreddit:
                query = query.join(Subreddit).filter(Subreddit.name == subreddit)
            
            submissions = query.order_by(desc(Submission.created_utc)).limit(limit).all()
            
            return [self._submission_to_dict(sub) for sub in submissions]
    
    def get_submissions_by_keywords(self, keywords: List[str], subreddit: str = None,
                                  limit: int = 100) -> List[Dict]:
        """Search for submissions containing specific keywords"""
        
        with self.db.get_session() as session:
            query = session.query(Submission).options(
                joinedload(Submission.subreddit)
            )
            
            # Search in title and selftext
            keyword_filters = []
            for keyword in keywords:
                keyword_filter = or_(
                    Submission.title.ilike(f'%{keyword}%'),
                    Submission.selftext.ilike(f'%{keyword}%')
                )
                keyword_filters.append(keyword_filter)
            
            # Combine all keyword filters with OR
            if keyword_filters:
                query = query.filter(or_(*keyword_filters))
            
            # Filter by subreddit if specified
            if subreddit:
                query = query.join(Subreddit).filter(Subreddit.name == subreddit)
            
            submissions = query.order_by(desc(Submission.score)).limit(limit).all()
            
            return [self._submission_to_dict(sub) for sub in submissions]
    
    def get_submission_with_comments(self, reddit_id: str) -> Optional[Dict]:
        """Get a submission with all its comments"""
        
        with self.db.get_session() as session:
            submission = session.query(Submission).options(
                joinedload(Submission.comments),
                joinedload(Submission.subreddit)
            ).filter_by(reddit_id=reddit_id).first()
            
            if not submission:
                return None
            
            result = self._submission_to_dict(submission)
            result['comments'] = [self._comment_to_dict(comment) for comment in submission.comments]
            
            return result
    
    def get_top_submissions_by_score(self, subreddit: str = None, 
                                   days_back: int = 7, limit: int = 50) -> List[Dict]:
        """Get top submissions by score in the specified time period"""
        
        with self.db.get_session() as session:
            query = session.query(Submission).options(
                joinedload(Submission.subreddit)
            )
            
            # Filter by date
            cutoff_date = datetime.now() - timedelta(days=days_back)
            query = query.filter(Submission.created_utc >= cutoff_date)
            
            # Filter by subreddit if specified
            if subreddit:
                query = query.join(Subreddit).filter(Subreddit.name == subreddit)
            
            submissions = query.order_by(desc(Submission.score)).limit(limit).all()
            
            return [self._submission_to_dict(sub) for sub in submissions]
    
    def get_pending_processing_items(self, processor_name: str = None,
                                   content_type: str = None, limit: int = 100) -> List[Dict]:
        """Get items pending processing for downstream apps"""
        
        with self.db.get_session() as session:
            query = session.query(ProcessingQueue).filter_by(
                processing_status=ProcessingStatus.PENDING
            )
            
            if processor_name:
                query = query.filter_by(processor_name=processor_name)
            
            if content_type:
                query = query.filter_by(content_type=content_type)
            
            items = query.order_by(
                desc(ProcessingQueue.priority),
                ProcessingQueue.queued_at
            ).limit(limit).all()
            
            return [self._processing_item_to_dict(item) for item in items]
    
    def mark_item_as_processing(self, queue_id: str, processor_name: str) -> bool:
        """Mark a processing queue item as currently being processed"""
        
        with self.db.get_session() as session:
            item = session.query(ProcessingQueue).filter_by(id=queue_id).first()
            if item and item.processing_status == ProcessingStatus.PENDING:
                item.processing_status = ProcessingStatus.PROCESSING
                item.processor_name = processor_name
                item.started_processing_at = datetime.now()
                return True
            return False
    
    def mark_item_as_completed(self, queue_id: str, result: Dict = None) -> bool:
        """Mark a processing queue item as completed"""
        
        with self.db.get_session() as session:
            item = session.query(ProcessingQueue).filter_by(id=queue_id).first()
            if item:
                item.processing_status = ProcessingStatus.COMPLETED
                item.completed_at = datetime.now()
                if result:
                    item.processing_result = result
                return True
            return False
    
    def get_subreddit_stats(self, subreddit: str) -> Dict:
        """Get statistics for a specific subreddit"""
        
        with self.db.get_session() as session:
            subreddit_obj = session.query(Subreddit).filter_by(name=subreddit).first()
            if not subreddit_obj:
                return {}
            
            # Get submission stats
            submission_stats = session.query(
                func.count(Submission.id).label('total_submissions'),
                func.avg(Submission.score).label('avg_score'),
                func.max(Submission.score).label('max_score'),
                func.count(func.distinct(Submission.author)).label('unique_authors')
            ).filter_by(subreddit_id=subreddit_obj.id).first()
            
            # Get comment stats
            comment_stats = session.query(
                func.count(Comment.id).label('total_comments'),
                func.avg(Comment.score).label('avg_comment_score')
            ).filter_by(subreddit_id=subreddit_obj.id).first()
            
            # Get recent activity
            recent_cutoff = datetime.now() - timedelta(days=7)
            recent_stats = session.query(
                func.count(Submission.id).label('recent_submissions')
            ).filter(
                and_(
                    Submission.subreddit_id == subreddit_obj.id,
                    Submission.created_utc >= recent_cutoff
                )
            ).first()
            
            return {
                'subreddit': subreddit,
                'total_submissions': submission_stats.total_submissions or 0,
                'avg_score': float(submission_stats.avg_score or 0),
                'max_score': submission_stats.max_score or 0,
                'unique_authors': submission_stats.unique_authors or 0,
                'total_comments': comment_stats.total_comments or 0,
                'avg_comment_score': float(comment_stats.avg_comment_score or 0),
                'recent_submissions_7d': recent_stats.recent_submissions or 0,
                'last_scraped_at': subreddit_obj.last_scraped_at.isoformat() if subreddit_obj.last_scraped_at else None
            }
    
    def get_scraping_session_history(self, days_back: int = 30, limit: int = 100) -> List[Dict]:
        """Get history of scraping sessions"""
        
        with self.db.get_session() as session:
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            sessions = session.query(ScrapeSession).options(
                joinedload(ScrapeSession.subreddit)
            ).filter(
                ScrapeSession.started_at >= cutoff_date
            ).order_by(
                desc(ScrapeSession.started_at)
            ).limit(limit).all()
            
            return [self._session_to_dict(session_obj) for session_obj in sessions]
    
    def get_content_by_author(self, author: str, content_type: str = "both", 
                            limit: int = 100) -> Dict:
        """Get content (submissions and/or comments) by a specific author"""
        
        with self.db.get_session() as session:
            result = {'author': author, 'submissions': [], 'comments': []}
            
            if content_type in ['submissions', 'both']:
                submissions = session.query(Submission).options(
                    joinedload(Submission.subreddit)
                ).filter_by(author=author).order_by(
                    desc(Submission.created_utc)
                ).limit(limit).all()
                
                result['submissions'] = [self._submission_to_dict(sub) for sub in submissions]
            
            if content_type in ['comments', 'both']:
                comments = session.query(Comment).filter_by(
                    author=author
                ).order_by(
                    desc(Comment.created_utc)
                ).limit(limit).all()
                
                result['comments'] = [self._comment_to_dict(comment) for comment in comments]
            
            return result
    
    def _submission_to_dict(self, submission: Submission) -> Dict:
        """Convert Submission object to dictionary"""
        return {
            'id': str(submission.id),
            'reddit_id': submission.reddit_id,
            'title': submission.title,
            'url': submission.url,
            'permalink': submission.permalink,
            'selftext': submission.selftext,
            'author': submission.author,
            'created_utc': submission.created_utc.isoformat(),
            'score': submission.score,
            'upvote_ratio': submission.upvote_ratio,
            'num_comments': submission.num_comments,
            'is_self': submission.is_self,
            'is_nsfw': submission.is_nsfw,
            'is_spoiler': submission.is_spoiler,
            'is_stickied': submission.is_stickied,
            'link_flair_text': submission.link_flair_text,
            'subreddit': submission.subreddit.name if submission.subreddit else None,
            'processing_status': submission.processing_status.value,
            'first_seen_at': submission.first_seen_at.isoformat()
        }
    
    def _comment_to_dict(self, comment: Comment) -> Dict:
        """Convert Comment object to dictionary"""
        return {
            'id': str(comment.id),
            'reddit_id': comment.reddit_id,
            'body': comment.body,
            'author': comment.author,
            'created_utc': comment.created_utc.isoformat(),
            'score': comment.score,
            'parent_id': comment.parent_id,
            'link_id': comment.link_id,
            'depth': comment.depth,
            'is_submitter': comment.is_submitter,
            'is_stickied': comment.is_stickied,
            'processing_status': comment.processing_status.value,
            'submission_id': str(comment.submission_id)
        }
    
    def _session_to_dict(self, session: ScrapeSession) -> Dict:
        """Convert ScrapeSession object to dictionary"""
        return {
            'id': str(session.id),
            'task_id': session.task_id,
            'task_type': session.task_type.value,
            'subreddit': session.subreddit.name if session.subreddit else None,
            'category': session.category.value,
            'n_results': session.n_results,
            'keywords': session.keywords,
            'time_filter': session.time_filter.value if session.time_filter else None,
            'status': session.status.value,
            'started_at': session.started_at.isoformat() if session.started_at else None,
            'completed_at': session.completed_at.isoformat() if session.completed_at else None,
            'duration_seconds': session.duration_seconds,
            'submissions_found': session.submissions_found,
            'submissions_scraped': session.submissions_scraped,
            'comments_scraped': session.comments_scraped,
            'error_message': session.error_message
        }
    
    def _processing_item_to_dict(self, item: ProcessingQueue) -> Dict:
        """Convert ProcessingQueue object to dictionary"""
        return {
            'id': str(item.id),
            'content_type': item.content_type,
            'content_id': str(item.content_id),
            'reddit_id': item.reddit_id,
            'priority': item.priority,
            'processing_status': item.processing_status.value,
            'processor_name': item.processor_name,
            'queued_at': item.queued_at.isoformat(),
            'started_processing_at': item.started_processing_at.isoformat() if item.started_processing_at else None,
            'completed_at': item.completed_at.isoformat() if item.completed_at else None,
            'retry_count': item.retry_count,
            'processing_result': item.processing_result
        }


# Convenience function for quick API access
def get_reddit_api(database_url: str = None) -> RedditDataAPI:
    """Get a Reddit Data API instance"""
    return RedditDataAPI(database_url or os.getenv('DATABASE_URL'))


# Example usage functions
def example_usage():
    """Example of how to use the API"""
    
    api = get_reddit_api()
    
    # Get recent submissions from CreditCardsIndia
    recent_posts = api.get_recent_submissions('CreditCardsIndia', limit=10)
    print(f"Found {len(recent_posts)} recent posts")
    
    # Search for submissions about credit cards
    card_posts = api.get_submissions_by_keywords(['credit card', 'cashback'])
    print(f"Found {len(card_posts)} posts about credit cards")
    
    # Get subreddit statistics
    stats = api.get_subreddit_stats('CreditCardsIndia')
    print(f"Subreddit stats: {stats}")
    
    # Get pending processing items
    pending = api.get_pending_processing_items(limit=5)
    print(f"Found {len(pending)} items pending processing")


if __name__ == "__main__":
    example_usage() 