#!/usr/bin/env python3
"""
Dedicated script to get a Reddit post with all its comments from the database.
Usage: python3 get_post_and_comments.py <reddit_id>
"""

import sys
import logging
from database_integration import get_database_processor

logging.basicConfig(level=logging.WARNING)

def get_post_and_all_comments(reddit_id):
    """
    Retrieve a Reddit post and all its comments from the database.
    
    Args:
        reddit_id (str): The Reddit submission ID (e.g., '1j9bd4e')
    
    Returns:
        dict: Complete post data with comments, or None if not found
    """
    processor = get_database_processor()
    if not processor:
        print("❌ Database processor not available!")
        return None
    
    try:
        with processor.db.get_session() as session:
            from models import Submission, Comment, Subreddit
            
            # Get the submission
            submission = session.query(Submission).filter_by(reddit_id=reddit_id).first()
            if not submission:
                print(f"❌ Post with ID '{reddit_id}' not found in database!")
                print("\n💡 Available posts:")
                # Show available posts
                recent_posts = session.query(Submission).order_by(Submission.first_seen_at.desc()).limit(5).all()
                for i, post in enumerate(recent_posts, 1):
                    print(f"   {i}. {post.reddit_id} - {post.title[:60]}...")
                return None
            
            # Get subreddit info
            subreddit = session.query(Subreddit).filter_by(id=submission.subreddit_id).first()
            
            # Get all comments
            comments = session.query(Comment).filter_by(submission_id=submission.id).order_by(Comment.created_utc).all()
            
            # Build comprehensive data structure
            post_data = {
                'submission': {
                    'reddit_id': submission.reddit_id,
                    'title': submission.title,
                    'url': submission.url,
                    'permalink': submission.permalink,
                    'author': submission.author,
                    'created_utc': submission.created_utc,
                    'score': submission.score,
                    'upvote_ratio': submission.upvote_ratio,
                    'num_comments': submission.num_comments,
                    'selftext': submission.selftext,
                    'link_flair_text': submission.link_flair_text,
                    'is_nsfw': submission.is_nsfw,
                    'is_spoiler': submission.is_spoiler,
                    'is_stickied': submission.is_stickied,
                    'is_locked': submission.is_locked,
                    'is_original_content': submission.is_original_content,
                },
                'subreddit': {
                    'name': subreddit.name if subreddit else 'Unknown',
                    'id': subreddit.id if subreddit else None
                },
                'comments': [],
                'stats': {
                    'total_comments': len(comments),
                    'high_score_comments': len([c for c in comments if c.score >= 10]),
                    'medium_score_comments': len([c for c in comments if 1 <= c.score < 10]),
                    'low_score_comments': len([c for c in comments if c.score < 1]),
                    'highest_score': max(c.score for c in comments) if comments else 0,
                    'lowest_score': min(c.score for c in comments) if comments else 0,
                    'average_score': sum(c.score for c in comments) / len(comments) if comments else 0,
                }
            }
            
            # Add all comments
            for comment in comments:
                comment_data = {
                    'reddit_id': comment.reddit_id,
                    'author': comment.author,
                    'body': comment.body,
                    'created_utc': comment.created_utc,
                    'score': comment.score,
                    'parent_id': comment.parent_id,
                    'depth': comment.depth,
                    'is_submitter': comment.is_submitter,
                    'is_stickied': comment.is_stickied,
                    'distinguished': comment.distinguished,
                    'edited': comment.edited,
                }
                post_data['comments'].append(comment_data)
            
            return post_data
            
    except Exception as e:
        print(f"❌ Error retrieving post: {e}")
        return None


def display_post_and_comments(reddit_id, format_type='detailed'):
    """
    Display a post and its comments in a formatted way.
    
    Args:
        reddit_id (str): Reddit submission ID
        format_type (str): 'detailed', 'simple', or 'json'
    """
    
    data = get_post_and_all_comments(reddit_id)
    if not data:
        return
    
    if format_type == 'json':
        import json
        print(json.dumps(data, default=str, indent=2))
        return
    
    sub = data['submission']
    subreddit = data['subreddit']
    comments = data['comments']
    stats = data['stats']
    
    print("=" * 80)
    print(f"📋 POST: {sub['title']}")
    print("=" * 80)
    
    print(f"🏷️  Subreddit: r/{subreddit['name']}")
    print(f"🆔 Reddit ID: {sub['reddit_id']}")
    print(f"👤 Author: {sub['author']}")
    print(f"📅 Posted: {sub['created_utc']}")
    print(f"👍 Score: {sub['score']} (↑{sub['upvote_ratio']:.1%})")
    print(f"💬 Comments: {stats['total_comments']}")
    print(f"🔗 URL: {sub['url']}")
    
    if sub['link_flair_text']:
        print(f"🏷️  Flair: {sub['link_flair_text']}")
    
    # Show flags
    flags = []
    if sub['is_nsfw']: flags.append("NSFW")
    if sub['is_spoiler']: flags.append("SPOILER") 
    if sub['is_stickied']: flags.append("STICKIED")
    if sub['is_locked']: flags.append("LOCKED")
    if sub['is_original_content']: flags.append("OC")
    if flags:
        print(f"🚩 Flags: {', '.join(flags)}")
    
    # Show content if it's a text post
    if sub['selftext']:
        print(f"\n📄 Content:")
        print("-" * 60)
        print(sub['selftext'][:500] + ("..." if len(sub['selftext']) > 500 else ""))
        print("-" * 60)
    
    if format_type == 'simple':
        print(f"\n💬 Comments Summary:")
        print(f"   🏆 High scoring (≥10): {stats['high_score_comments']}")
        print(f"   📈 Medium scoring (1-9): {stats['medium_score_comments']}")
        print(f"   📉 Low scoring (<1): {stats['low_score_comments']}")
        print(f"   📊 Score range: {stats['lowest_score']} to {stats['highest_score']}")
        print(f"   📈 Average score: {stats['average_score']:.1f}")
        return
    
    # Detailed view - show all comments
    print(f"\n💬 ALL COMMENTS ({len(comments)}):")
    print("=" * 80)
    
    for i, comment in enumerate(comments, 1):
        print(f"\n{i:3d}. 👤 {comment['author']} | 👍 {comment['score']} | 📅 {comment['created_utc']}")
        print(f"     🆔 {comment['reddit_id']}")
        if comment['parent_id'] != f"t3_{reddit_id}":
            print(f"     ↳ Reply to: {comment['parent_id']}")
        if comment['distinguished']:
            print(f"     🌟 Distinguished: {comment['distinguished']}")
        if comment['is_stickied']:
            print(f"     📌 Stickied")
        if comment['edited']:
            print(f"     ✏️  Edited: {comment['edited']}")
        
        # Format comment body
        body = comment['body'].replace('\n', '\n     ')
        print(f"     💬 {body}")
        print("     " + "-" * 70)


def main():
    """Main function for command line usage"""
    if len(sys.argv) < 2:
        print("Usage: python3 get_post_and_comments.py <reddit_id> [format]")
        print("\nFormat options:")
        print("  detailed (default) - Show post and all comments with full details")
        print("  simple            - Show post and comment summary only")
        print("  json              - Output raw JSON data")
        print("\nExamples:")
        print("  python3 get_post_and_comments.py 1j9bd4e")
        print("  python3 get_post_and_comments.py 1j9bd4e simple")
        print("  python3 get_post_and_comments.py 1j9bd4e json")
        return
    
    reddit_id = sys.argv[1]
    format_type = sys.argv[2] if len(sys.argv) > 2 else 'detailed'
    
    if format_type not in ['detailed', 'simple', 'json']:
        print("❌ Invalid format. Use 'detailed', 'simple', or 'json'")
        return
    
    display_post_and_comments(reddit_id, format_type)


if __name__ == "__main__":
    main() 