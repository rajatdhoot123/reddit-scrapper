#!/usr/bin/env python3

from database_integration import get_database_processor, save_scraping_results_to_db
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)

def test_database_save():
    """Test database saving with existing scrape files"""
    
    print("🧪 Testing database save functionality...")
    
    # Get database processor
    db_processor = get_database_processor()
    if not db_processor:
        print("❌ Database processor not available!")
        return
    
    print("✅ Database processor available!")
    
    # Look for existing scrape files
    today = "2025-05-29"  # Use yesterday's date where we know files exist
    scrapes_dir = Path(f"scrapes/{today}/subreddits")
    
    if not scrapes_dir.exists():
        print(f"❌ Scrapes directory not found: {scrapes_dir}")
        return
    
    json_files = list(scrapes_dir.glob("*.json"))
    if not json_files:
        print("❌ No JSON scrape files found!")
        return
    
    print(f"📁 Found {len(json_files)} scrape files")
    
    # Test with the remaining file
    test_file = json_files[0]
    print(f"🔬 Testing with file: {test_file.name}")
    
    # Create a mock config and result
    config = {
        "name": "CreditCardsIndia",
        "category": "h",  # Use single letter format
        "n_results": 10
    }
    
    result = {
        "status": "success",
        "subreddit": "CreditCardsIndia",
        "category": "h", 
        "submissions_found": 10,
        "comments_scraped": 0,
        "scrape_file": str(test_file)
    }
    
    task_id = "test_task_comments_20250530"
    
    try:
        print("💾 Attempting to save to database...")
        save_scraping_results_to_db(
            processor=db_processor,
            task_id=task_id,
            task_type="manual",
            config=config,
            result=result,
            scrape_file=test_file
        )
        print("✅ Database save completed!")
        
    except Exception as e:
        print(f"❌ Database save failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Check database status after test
    print("\n📊 Checking database status after test...")
    try:
        with db_processor.db.get_session() as session:
            from models import Subreddit, Submission, Comment, ScrapeSession
            
            subreddits = session.query(Subreddit).count()
            submissions = session.query(Submission).count() 
            comments = session.query(Comment).count()
            sessions = session.query(ScrapeSession).count()
            
            print(f"   Subreddits: {subreddits}")
            print(f"   Submissions: {submissions}")
            print(f"   Comments: {comments}")
            print(f"   Scrape Sessions: {sessions}")
            
    except Exception as e:
        print(f"❌ Error checking database: {e}")

if __name__ == "__main__":
    test_database_save() 