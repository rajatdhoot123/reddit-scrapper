#!/usr/bin/env python3

from database_integration import get_database_processor
import logging

logging.basicConfig(level=logging.WARNING)  # Reduce noise

def query_subreddit_data(subreddit_name=None):
    """Query specific subreddit data"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    try:
        with processor.db.get_session() as session:
            from models import Subreddit, Submission, Comment
            
            if subreddit_name:
                # Query specific subreddit
                subreddit = session.query(Subreddit).filter_by(name=subreddit_name).first()
                if not subreddit:
                    print(f"❌ Subreddit r/{subreddit_name} not found!")
                    return
                
                subreddits = [subreddit]
            else:
                # Query all subreddits
                subreddits = session.query(Subreddit).all()
            
            for subreddit in subreddits:
                submissions = session.query(Submission).filter_by(subreddit_id=subreddit.id).all()
                total_comments = session.query(Comment).filter_by(subreddit_id=subreddit.id).count()
                
                print(f"\n🏷️  r/{subreddit.name}")
                print(f"   📝 {len(submissions)} submissions")
                print(f"   💬 {total_comments} comments")
                
                for sub in submissions:
                    sub_comments = session.query(Comment).filter_by(submission_id=sub.id).count()
                    print(f"      📄 {sub.reddit_id}: {sub.title[:60]}... ({sub_comments} comments)")
                    
    except Exception as e:
        print(f"❌ Error: {e}")


def query_comments_for_submission(reddit_id):
    """Query comments for a specific submission"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    try:
        with processor.db.get_session() as session:
            from models import Submission, Comment
            
            submission = session.query(Submission).filter_by(reddit_id=reddit_id).first()
            if not submission:
                print(f"❌ Submission {reddit_id} not found!")
                return
            
            comments = session.query(Comment).filter_by(submission_id=submission.id).order_by(Comment.score.desc()).all()
            
            print(f"\n📝 {submission.title}")
            print(f"   🆔 Reddit ID: {submission.reddit_id}")
            print(f"   👤 Author: {submission.author}")
            print(f"   👍 Score: {submission.score}")
            print(f"   💬 {len(comments)} comments")
            
            print(f"\n💬 Comments:")
            for i, comment in enumerate(comments, 1):
                print(f"   {i}. {comment.author} (Score: {comment.score})")
                print(f"      {comment.body[:100]}...")
                print()
                
    except Exception as e:
        print(f"❌ Error: {e}")


def get_post_with_all_comments(reddit_id):
    """Get a comprehensive view of a post with all its comments and metadata"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return None
    
    try:
        with processor.db.get_session() as session:
            from models import Submission, Comment, Subreddit
            
            # Get the submission with subreddit info
            submission = session.query(Submission).filter_by(reddit_id=reddit_id).first()
            if not submission:
                print(f"❌ Submission {reddit_id} not found!")
                return None
            
            # Get subreddit info
            subreddit = session.query(Subreddit).filter_by(id=submission.subreddit_id).first()
            
            # Get all comments for this submission
            comments = session.query(Comment).filter_by(submission_id=submission.id).order_by(Comment.created_utc).all()
            
            print("=" * 80)
            print("📋 COMPLETE POST ANALYSIS")
            print("=" * 80)
            
            # Submission details
            print(f"\n🏷️  SUBREDDIT: r/{subreddit.name if subreddit else 'Unknown'}")
            print(f"📝 TITLE: {submission.title}")
            print(f"🆔 REDDIT ID: {submission.reddit_id}")
            print(f"🔗 URL: {submission.url}")
            print(f"📄 PERMALINK: https://reddit.com{submission.permalink}")
            print(f"👤 AUTHOR: {submission.author}")
            print(f"📅 CREATED: {submission.created_utc}")
            print(f"👍 SCORE: {submission.score}")
            print(f"📊 UPVOTE RATIO: {submission.upvote_ratio}")
            print(f"💬 COMMENT COUNT: {submission.num_comments}")
            print(f"🏷️  FLAIR: {submission.link_flair_text or 'None'}")
            
            # Content
            if submission.selftext:
                print(f"\n📄 CONTENT:")
                print("-" * 60)
                print(submission.selftext[:500] + ("..." if len(submission.selftext) > 500 else ""))
                print("-" * 60)
            
            # Flags
            flags = []
            if submission.is_nsfw: flags.append("NSFW")
            if submission.is_spoiler: flags.append("SPOILER")
            if submission.is_stickied: flags.append("STICKIED")
            if submission.is_locked: flags.append("LOCKED")
            if submission.is_original_content: flags.append("OC")
            if flags:
                print(f"🏷️  FLAGS: {', '.join(flags)}")
            
            print(f"\n💬 COMMENTS ({len(comments)} total):")
            print("=" * 80)
            
            if not comments:
                print("   No comments found.")
                return
            
            # Group comments by score ranges for better analysis
            high_score = [c for c in comments if c.score >= 10]
            med_score = [c for c in comments if 1 <= c.score < 10]
            low_score = [c for c in comments if c.score < 1]
            
            if high_score:
                print(f"\n🏆 HIGH SCORING COMMENTS (Score ≥ 10) - {len(high_score)} comments:")
                for i, comment in enumerate(high_score[:10], 1):  # Show top 10
                    print(f"\n   {i}. 👤 {comment.author} | 👍 Score: {comment.score} | 📅 {comment.created_utc}")
                    print(f"      🆔 {comment.reddit_id}")
                    if comment.parent_id:
                        print(f"      ↳ Reply to: {comment.parent_id}")
                    comment_text = comment.body.replace('\n', '\n      ')
                    print(f"      💬 {comment_text}")
                    if comment.edited:
                        print(f"      ✏️  (Edited: {comment.edited})")
                    print("      " + "-" * 70)
            
            if med_score:
                print(f"\n📈 MEDIUM SCORING COMMENTS (Score 1-9) - {len(med_score)} comments:")
                for i, comment in enumerate(med_score[:5], 1):  # Show top 5
                    print(f"\n   {i}. 👤 {comment.author} | 👍 Score: {comment.score}")
                    comment_text = comment.body[:200] + ("..." if len(comment.body) > 200 else "")
                    print(f"      💬 {comment_text}")
            
            if low_score:
                print(f"\n📉 LOW/NEGATIVE SCORING COMMENTS (Score < 1) - {len(low_score)} comments:")
                for i, comment in enumerate(low_score[:3], 1):  # Show top 3
                    print(f"   {i}. 👤 {comment.author} | 👍 Score: {comment.score}")
                    comment_text = comment.body[:100] + ("..." if len(comment.body) > 100 else "")
                    print(f"      💬 {comment_text}")
            
            # Summary stats
            print(f"\n📊 COMMENT STATISTICS:")
            print(f"   🏆 Highest scored comment: {max(comments, key=lambda x: x.score).score} points")
            print(f"   📉 Lowest scored comment: {min(comments, key=lambda x: x.score).score} points")
            print(f"   📈 Average comment score: {sum(c.score for c in comments) / len(comments):.1f}")
            
            # Top commenters
            from collections import Counter
            author_counts = Counter(c.author for c in comments if c.author != '[deleted]')
            if author_counts:
                print(f"   ✍️  Most active commenters:")
                for author, count in author_counts.most_common(3):
                    print(f"      • {author}: {count} comments")
            
            print("=" * 80)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def search_posts_by_title(search_term):
    """Search for posts by title keyword"""
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return
    
    try:
        with processor.db.get_session() as session:
            from models import Submission, Subreddit, Comment
            from sqlalchemy import func
            
            # Search submissions by title
            submissions = session.query(Submission).filter(
                Submission.title.ilike(f'%{search_term}%')
            ).order_by(Submission.score.desc()).all()
            
            print(f"🔍 Search results for '{search_term}': {len(submissions)} posts found")
            print("=" * 60)
            
            for i, sub in enumerate(submissions, 1):
                subreddit = session.query(Subreddit).filter_by(id=sub.subreddit_id).first()
                comment_count = session.query(Comment).filter_by(submission_id=sub.id).count()
                
                print(f"\n{i}. 📝 {sub.title[:70]}...")
                print(f"   🏷️  r/{subreddit.name if subreddit else 'Unknown'}")
                print(f"   🆔 {sub.reddit_id} | 👤 {sub.author} | 👍 {sub.score} | 💬 {comment_count}")
                print(f"   📅 {sub.created_utc}")
                
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) == 1:
        # No arguments - show all subreddits
        print("📊 All Subreddits and Submissions:")
        query_subreddit_data()
    elif len(sys.argv) == 2:
        # One argument - could be subreddit name or submission ID
        arg = sys.argv[1]
        if arg.startswith('r/'):
            # Subreddit name
            subreddit_name = arg[2:]  # Remove 'r/' prefix
            print(f"📊 Data for r/{subreddit_name}:")
            query_subreddit_data(subreddit_name)
        elif arg.startswith('--full'):
            # Show help for full analysis
            print("Usage for full post analysis:")
            print("  python3 query_db.py --full REDDIT_ID")
            print("  Example: python3 query_db.py --full 1j9bd4e")
        elif arg.startswith('--search'):
            # Show help for search
            print("Usage for searching posts:")
            print("  python3 query_db.py --search 'search term'")
            print("  Example: python3 query_db.py --search 'credit card'")
        else:
            # Submission ID - show brief comments
            print(f"💬 Comments for submission {arg}:")
            query_comments_for_submission(arg)
    elif len(sys.argv) == 3:
        # Two arguments
        if sys.argv[1] == '--full':
            # Full post analysis
            reddit_id = sys.argv[2]
            get_post_with_all_comments(reddit_id)
        elif sys.argv[1] == '--search':
            # Search posts
            search_term = sys.argv[2]
            search_posts_by_title(search_term)
        else:
            print("Invalid arguments. See usage below.")
            print_usage()
    else:
        print_usage()

def print_usage():
    """Print usage instructions"""
    print("Usage:")
    print("  python3 query_db.py                          # Show all subreddits")
    print("  python3 query_db.py r/CreditCardsIndia       # Show specific subreddit")
    print("  python3 query_db.py 1j9bd4e                  # Show brief comments for submission")
    print("  python3 query_db.py --full 1j9bd4e           # Show COMPLETE post with all comments")
    print("  python3 query_db.py --search 'credit card'   # Search posts by title")
    print("\nExamples:")
    print("  python3 query_db.py --full 1j9bd4e           # Complete analysis of HDFC guide post")
    print("  python3 query_db.py --search 'HDFC'          # Find all posts mentioning HDFC") 