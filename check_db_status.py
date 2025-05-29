#!/usr/bin/env python3

from database_integration import get_database_processor
import logging

logging.basicConfig(level=logging.INFO)

def check_database_status():
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    print("✅ Database processor available!")
    
    try:
        with processor.db.get_session() as session:
            from models import Subreddit, Submission, Comment, ScrapeSession
            
            # Count records
            subreddits = session.query(Subreddit).count()
            submissions = session.query(Submission).count() 
            comments = session.query(Comment).count()
            sessions = session.query(ScrapeSession).count()
            
            print("\n📊 Current database counts:")
            print(f"   Subreddits: {subreddits}")
            print(f"   Submissions: {submissions}")
            print(f"   Comments: {comments}")
            print(f"   Scrape Sessions: {sessions}")
            
            # Show recent sessions
            if sessions > 0:
                print("\n🔍 Recent scrape sessions:")
                recent_sessions = session.query(ScrapeSession).order_by(ScrapeSession.started_at.desc()).limit(5).all()
                for i, s in enumerate(recent_sessions, 1):
                    print(f"   {i}. {s.id} - Status: {s.status} - Submissions: {s.submissions_scraped} - Comments: {s.comments_scraped}")
            
            # Show recent submissions
            if submissions > 0:
                print("\n📝 Recent submissions:")
                recent_submissions = session.query(Submission).order_by(Submission.first_seen_at.desc()).limit(3).all()
                for i, sub in enumerate(recent_submissions, 1):
                    print(f"   {i}. {sub.reddit_id} - {sub.title[:50]}...")
            
            # Show recent comments
            if comments > 0:
                print("\n💬 Recent comments:")
                recent_comments = session.query(Comment).order_by(Comment.first_seen_at.desc()).limit(3).all()
                for i, comment in enumerate(recent_comments, 1):
                    print(f"   {i}. {comment.reddit_id} - {comment.body[:50]}...")
                    
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_database_status() 