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


def show_subreddit_details():
    """Show detailed information about each subreddit and its content"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    print("\n" + "="*60)
    print("📊 DETAILED SUBREDDIT ANALYSIS")
    print("="*60)
    
    try:
        with processor.db.get_session() as session:
            from models import Subreddit, Submission, Comment, ScrapeSession
            from sqlalchemy import func
            
            # Get all subreddits with their statistics
            subreddits = session.query(Subreddit).all()
            
            for subreddit in subreddits:
                print(f"\n🏷️  r/{subreddit.name}")
                print(f"   📈 ID: {subreddit.id}")
                print(f"   📅 First scraped: {subreddit.first_scraped_at}")
                print(f"   🔄 Last scraped: {subreddit.last_scraped_at}")
                print(f"   📊 Total scrapes: {subreddit.total_scrapes}")
                
                # Count submissions for this subreddit
                submission_count = session.query(Submission).filter_by(subreddit_id=subreddit.id).count()
                print(f"   📝 Submissions: {submission_count}")
                
                # Count comments for this subreddit
                comment_count = session.query(Comment).filter_by(subreddit_id=subreddit.id).count()
                print(f"   💬 Comments: {comment_count}")
                
                # Show recent submissions for this subreddit
                if submission_count > 0:
                    print(f"\n   📝 Recent submissions in r/{subreddit.name}:")
                    recent_subs = session.query(Submission).filter_by(
                        subreddit_id=subreddit.id
                    ).order_by(Submission.first_seen_at.desc()).limit(5).all()
                    
                    for j, sub in enumerate(recent_subs, 1):
                        # Count comments for this submission
                        sub_comment_count = session.query(Comment).filter_by(submission_id=sub.id).count()
                        print(f"      {j}. {sub.reddit_id} - {sub.title[:60]}...")
                        print(f"         👤 Author: {sub.author} | 👍 Score: {sub.score} | 💬 Comments: {sub_comment_count}")
                        print(f"         📅 Created: {sub.created_utc}")
                        
                        # Show some comments for this submission
                        if sub_comment_count > 0:
                            sample_comments = session.query(Comment).filter_by(
                                submission_id=sub.id
                            ).order_by(Comment.score.desc()).limit(2).all()
                            
                            print(f"         💬 Top comments:")
                            for k, comment in enumerate(sample_comments, 1):
                                comment_preview = comment.body.replace('\n', ' ')[:80]
                                print(f"            {k}. {comment.author}: {comment_preview}... (Score: {comment.score})")
                
                print("-" * 50)
                
    except Exception as e:
        print(f"❌ Error showing subreddit details: {e}")
        import traceback
        traceback.print_exc()


def show_comments_by_submission():
    """Show comments organized by submission"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    print("\n" + "="*60)
    print("💬 COMMENTS BY SUBMISSION")
    print("="*60)
    
    try:
        with processor.db.get_session() as session:
            from models import Submission, Comment
            
            # Get submissions with comments
            submissions_with_comments = session.query(Submission).join(Comment).distinct().all()
            
            for submission in submissions_with_comments:
                comments = session.query(Comment).filter_by(submission_id=submission.id).order_by(Comment.score.desc()).all()
                
                print(f"\n📝 {submission.title[:80]}...")
                print(f"   🆔 Reddit ID: {submission.reddit_id}")
                print(f"   👤 Author: {submission.author}")
                print(f"   👍 Score: {submission.score}")
                print(f"   💬 Total Comments: {len(comments)}")
                
                # Show top 5 comments
                print(f"   🏆 Top comments:")
                for i, comment in enumerate(comments[:5], 1):
                    comment_text = comment.body.replace('\n', ' ')[:100]
                    print(f"      {i}. {comment.author} (Score: {comment.score})")
                    print(f"         {comment_text}...")
                
                print("-" * 50)
                
    except Exception as e:
        print(f"❌ Error showing comments by submission: {e}")
        import traceback
        traceback.print_exc()


def show_database_stats():
    """Show comprehensive database statistics"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    print("\n" + "="*60)
    print("📈 DATABASE STATISTICS")
    print("="*60)
    
    try:
        with processor.db.get_session() as session:
            from models import Subreddit, Submission, Comment, ScrapeSession
            from sqlalchemy import func
            
            # Top subreddits by submissions
            print("\n🏆 Top subreddits by submissions:")
            sub_stats = session.query(
                Subreddit.name,
                func.count(Submission.id).label('submission_count')
            ).outerjoin(Submission).group_by(Subreddit.name).order_by(func.count(Submission.id).desc()).all()
            
            for i, (name, count) in enumerate(sub_stats, 1):
                print(f"   {i}. r/{name}: {count} submissions")
            
            # Top subreddits by comments
            print("\n💬 Top subreddits by comments:")
            comment_stats = session.query(
                Subreddit.name,
                func.count(Comment.id).label('comment_count')
            ).outerjoin(Comment).group_by(Subreddit.name).order_by(func.count(Comment.id).desc()).all()
            
            for i, (name, count) in enumerate(comment_stats, 1):
                print(f"   {i}. r/{name}: {count} comments")
            
            # Top authors by comments
            print("\n✍️  Top comment authors:")
            author_stats = session.query(
                Comment.author,
                func.count(Comment.id).label('comment_count')
            ).filter(Comment.author != '[deleted]').group_by(Comment.author).order_by(func.count(Comment.id).desc()).limit(5).all()
            
            for i, (author, count) in enumerate(author_stats, 1):
                print(f"   {i}. {author}: {count} comments")
            
            # Average scores
            avg_submission_score = session.query(func.avg(Submission.score)).scalar()
            avg_comment_score = session.query(func.avg(Comment.score)).scalar()
            
            print(f"\n📊 Average scores:")
            print(f"   📝 Submissions: {avg_submission_score:.1f}" if avg_submission_score else "   📝 Submissions: N/A")
            print(f"   💬 Comments: {avg_comment_score:.1f}" if avg_comment_score else "   💬 Comments: N/A")
            
    except Exception as e:
        print(f"❌ Error showing database stats: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run all analysis functions
    check_database_status()
    show_subreddit_details()
    show_comments_by_submission()
    show_database_stats() 