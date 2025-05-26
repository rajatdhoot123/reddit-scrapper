#!/usr/bin/env python3

"""
Test script to manually trigger the Reddit scraping task
"""

import sys
from tasks import scrape_and_upload_to_r2, test_scrape_task

def main():
    print("Reddit Scraping Task Test")
    print("=" * 40)
    
    choice = input("Choose an option:\n1. Run full scraping task\n2. Run test task\nEnter choice (1 or 2): ")
    
    if choice == "1":
        print("Triggering full scraping task...")
        result = scrape_and_upload_to_r2.delay()
        print(f"Task submitted with ID: {result.id}")
        print("Check the Celery logs for progress.")
        
    elif choice == "2":
        print("Triggering test task...")
        result = test_scrape_task.delay()
        print(f"Test task submitted with ID: {result.id}")
        print("Check the Celery logs for progress.")
        
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)
    
    print("\nTo monitor task status, you can use:")
    print("celery -A celery_config flower")
    print("Then visit http://localhost:5555 in your browser")

if __name__ == "__main__":
    main() 