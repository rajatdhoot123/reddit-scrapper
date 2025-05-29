#!/usr/bin/env python3

import sys
import time
from celery_config import app
from tasks import manual_scrape_subreddit

def test_database_integration():
    """Test database integration with a small scrape"""
    print("Testing database integration with a small Reddit scrape...")
    print("=" * 60)
    
    # Test configuration - scrape only 2 items from CreditCardsIndia
    subreddit = "CreditCardsIndia"
    category = "H"  # hot posts (uppercase as required by URS)
    n_results = 2  # Only 2 items for testing
    options = {
        "csv": False,  # Use JSON for better database integration testing
        "auto_confirm": True,
        "timeout": 120  # Short timeout for testing
    }
    
    print(f"Scraping r/{subreddit}")
    print(f"Category: {category} (hot posts)")
    print(f"Number of results: {n_results}")
    print(f"Options: {options}")
    print("-" * 60)
    
    try:
        # Submit the task
        print("Submitting manual scrape task...")
        result = manual_scrape_subreddit.apply_async(
            args=[subreddit, category, n_results],
            kwargs={"options": options, "scrape_comments": True}
        )
        
        print(f"Task ID: {result.id}")
        print("Waiting for task completion...")
        
        # Wait for the result with timeout
        task_result = result.get(timeout=300)  # 5 minute timeout
        
        print("\nTask completed!")
        print("=" * 60)
        print("RESULT:")
        print("-" * 60)
        
        # Print the result nicely
        if isinstance(task_result, dict):
            for key, value in task_result.items():
                print(f"{key}: {value}")
        else:
            print(task_result)
        
        print("=" * 60)
        
        # Check if database integration worked
        if task_result.get("status") == "success":
            print("\n✅ Scraping task completed successfully!")
            submissions_found = task_result.get("submissions_found", 0)
            comments_scraped = task_result.get("comments_scraped", 0)
            
            print(f"📊 Submissions found: {submissions_found}")
            print(f"💬 Comments scraped: {comments_scraped}")
            
            if submissions_found > 0:
                print("\n🎯 Database integration test: The scraping data should now be saved to the database.")
                print("   Check your database to verify the data was stored correctly.")
            else:
                print("\n⚠️  No submissions found, so no data to test database integration with.")
        else:
            print(f"\n❌ Scraping task failed: {task_result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"\n❌ Error running test: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Database Integration Test")
    print("=" * 60)
    print("This script will test the database integration by running a small Reddit scrape.")
    print("It will scrape only 2 hot posts from r/CreditCardsIndia to test the database.")
    print()
    
    success = test_database_integration()
    
    if success:
        print("\n✅ Test completed! Check the output above and your database for results.")
    else:
        print("\n❌ Test failed. Check the error messages above.")
    
    sys.exit(0 if success else 1) 