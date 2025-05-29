#!/usr/bin/env python3
"""
Database-Only Operations Example

This script demonstrates how to use only database operations
without any upload functionality. Useful for scenarios where:
- You want to re-process existing scrape files
- You only need database storage without cloud uploads
- You're debugging database integration
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def database_only_workflow():
    """Example workflow using only database operations"""
    print("🗃️  Database-Only Workflow Example")
    print("="*50)
    
    try:
        from tasks import (
            process_subreddit_config, 
            database_only_task
        )
        
        # Configuration for a simple scrape
        config = {
            "name": "CreditCardsIndia",
            "category": "h",  # hot posts
            "n_results": 5,   # Small number for testing
            "options": {"csv": False, "auto_confirm": True},
            "enabled": True
        }
        
        print(f"Configuration: {config}")
        
        # Step 1: Scrape data (without database operations)
        print("\n📥 Step 1: Scraping Reddit data...")
        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")
        
        scrape_result = process_subreddit_config(config, scrapes_dir)
        print(f"Scraping result: {scrape_result}")
        
        if scrape_result["status"] != "success":
            print("❌ Scraping failed, cannot proceed with database operations")
            return False
        
        # Step 2: Launch database-only task
        print("\n💾 Step 2: Saving to database...")
        task_id = f"db_only_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        db_task = database_only_task.apply_async(
            args=[
                task_id, 
                "manual", 
                config, 
                scrape_result, 
                scrape_result.get("scrape_file_path")
            ]
        )
        
        print(f"Database task launched with ID: {db_task.id}")
        print("Waiting for database operations to complete...")
        
        # Wait for database task to complete
        try:
            db_result = db_task.get(timeout=120)
            print(f"Database operation completed: {db_result}")
            
            if db_result.get("database_saved"):
                print("✅ Data successfully saved to database!")
                print(f"   Subreddit: {db_result.get('subreddit')}")
                print(f"   Task ID: {db_result.get('task_id')}")
                return True
            else:
                print("❌ Database save failed")
                return False
                
        except Exception as e:
            print(f"❌ Database task failed: {e}")
            return False
    
    except ImportError as e:
        print(f"❌ Could not import required modules: {e}")
        print("Make sure database integration is properly set up")
        return False
    except Exception as e:
        print(f"❌ Error in database workflow: {e}")
        return False


def reprocess_existing_files():
    """Example of reprocessing existing scrape files to database"""
    print("\n🔄 Reprocessing Existing Files Example")
    print("="*50)
    
    try:
        from tasks import database_only_task
        
        # Look for existing scrape files
        scrapes_base = Path("scrapes")
        if not scrapes_base.exists():
            print("No scrapes directory found")
            return False
        
        # Find recent JSON files
        json_files = []
        for date_dir in scrapes_base.iterdir():
            if date_dir.is_dir():
                for json_file in date_dir.rglob("*.json"):
                    if json_file.stat().st_size > 100:  # Skip empty files
                        json_files.append(json_file)
        
        if not json_files:
            print("No scrape files found to reprocess")
            return False
        
        print(f"Found {len(json_files)} scrape files to reprocess")
        
        # Process each file
        processed = 0
        for json_file in json_files[:3]:  # Limit to first 3 for demo
            print(f"\nProcessing: {json_file}")
            
            # Create a basic config based on filename
            # This is a simplified example - you might extract more info from filename
            config = {
                "name": "CreditCardsIndia",  # Default subreddit
                "category": "h",
                "n_results": 10,
                "options": {"csv": False}
            }
            
            result = {
                "status": "success",
                "subreddit": config["name"],
                "category": config["category"],
                "scrape_file_path": str(json_file),
                "submissions_found": 0,  # Would be counted from file
                "comments_scraped": 0
            }
            
            # Launch database task for this file
            task_id = f"reprocess_{json_file.stem}_{datetime.now().strftime('%H%M%S')}"
            
            db_task = database_only_task.apply_async(
                args=[task_id, "reprocess", config, result, str(json_file)]
            )
            
            print(f"Launched database task: {db_task.id}")
            
            # For demo, wait for each task (in production you might not wait)
            try:
                db_result = db_task.get(timeout=60)
                if db_result.get("database_saved"):
                    print(f"✅ {json_file.name} processed successfully")
                    processed += 1
                else:
                    print(f"❌ {json_file.name} processing failed")
            except Exception as e:
                print(f"❌ Error processing {json_file.name}: {e}")
        
        print(f"\n📊 Reprocessing complete: {processed} files successfully processed")
        return processed > 0
        
    except ImportError as e:
        print(f"❌ Could not import database task: {e}")
        return False
    except Exception as e:
        print(f"❌ Error in reprocessing: {e}")
        return False


def check_database_status():
    """Check if database integration is available and working"""
    print("\n🔍 Database Status Check")
    print("="*30)
    
    try:
        from database_integration import DATABASE_INTEGRATION_AVAILABLE, get_database_processor
        
        if not DATABASE_INTEGRATION_AVAILABLE:
            print("❌ Database integration is not available")
            print("   Check your DATABASE_URL environment variable")
            print("   Ensure PostgreSQL is running and accessible")
            return False
        
        print("✅ Database integration is available")
        
        # Try to get database processor
        try:
            db_processor = get_database_processor()
            if db_processor:
                print("✅ Database processor initialized successfully")
                print("✅ Ready for database operations")
                return True
            else:
                print("❌ Could not initialize database processor")
                return False
        except Exception as e:
            print(f"❌ Database processor error: {e}")
            return False
            
    except ImportError as e:
        print(f"❌ Could not import database integration: {e}")
        return False


def main():
    """Run database-only examples"""
    print("🗃️  Database-Only Operations Examples")
    print("="*60)
    
    # Check if celery is running
    try:
        from celery_config import app
        inspect = app.control.inspect()
        active_nodes = inspect.active()
        
        if not active_nodes:
            print("❌ No active Celery workers found!")
            print("Please start Celery workers with: ./start_celery.sh")
            return False
        else:
            print(f"✅ Found {len(active_nodes)} active Celery worker(s)")
    except Exception as e:
        print(f"❌ Could not connect to Celery: {e}")
        return False
    
    # Check database status
    if not check_database_status():
        print("\n❌ Database is not properly configured")
        print("Please check your database setup before running database operations")
        return False
    
    print("\n" + "="*60)
    print("Choose an example to run:")
    print("1. Database-only workflow (scrape + save to database)")
    print("2. Reprocess existing scrape files to database")
    print("3. Run both examples")
    print("="*60)
    
    try:
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == "1":
            success = database_only_workflow()
        elif choice == "2":
            success = reprocess_existing_files()
        elif choice == "3":
            print("\n🚀 Running both examples...")
            success1 = database_only_workflow()
            success2 = reprocess_existing_files()
            success = success1 or success2
        else:
            print("Invalid choice")
            return False
        
        if success:
            print("\n🎉 Database operations completed successfully!")
            print("Check your database to see the stored data")
        else:
            print("\n⚠️  Some database operations failed")
            print("Check the output above for details")
        
        return success
        
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 