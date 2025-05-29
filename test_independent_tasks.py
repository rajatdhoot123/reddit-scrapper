#!/usr/bin/env python3
"""
Test script for independent database and upload tasks.

This script demonstrates how to use the new modular task architecture
where database operations and upload operations are independent.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_database_only_task():
    """Test the database-only task independently"""
    print("="*50)
    print("Testing Database-Only Task")
    print("="*50)
    
    try:
        from tasks import database_only_task
        
        # Sample configuration and result
        config = {
            "name": "CreditCardsIndia",
            "category": "t",
            "n_results": 5,
            "time_filter": "day",
            "options": {"csv": False, "auto_confirm": True}
        }
        
        result = {
            "status": "success",
            "subreddit": "CreditCardsIndia",
            "category": "t",
            "submissions_found": 5,
            "comments_scraped": 0,
            "scrape_file_path": "/fake/path/for/testing.json"
        }
        
        task_id = f"test_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"Launching database task with ID: {task_id}")
        print(f"Config: {config}")
        print(f"Result: {result}")
        
        # Launch database task
        db_task = database_only_task.apply_async(
            args=[task_id, "test", config, result, None]
        )
        
        print(f"Database task launched with Celery ID: {db_task.id}")
        
        # Wait for result (with timeout)
        try:
            db_result = db_task.get(timeout=60)
            print(f"Database task completed: {db_result}")
            return True
        except Exception as e:
            print(f"Database task failed or timed out: {e}")
            return False
            
    except ImportError as e:
        print(f"Could not import database task: {e}")
        return False
    except Exception as e:
        print(f"Error testing database task: {e}")
        return False


def test_upload_only_task():
    """Test the upload-only task independently"""
    print("\n" + "="*50)
    print("Testing Upload-Only Task")
    print("="*50)
    
    try:
        from tasks import upload_only_task, create_archive
        
        # Create a test archive for upload
        today = datetime.now().strftime("%Y-%m-%d")
        test_dir = Path(f"test_scrapes_{today}")
        test_dir.mkdir(exist_ok=True)
        
        # Create some test files
        test_file = test_dir / "test_data.json"
        test_file.write_text('{"test": "data", "posts": []}')
        
        # Create archive
        print("Creating test archive...")
        archive_path = create_archive(test_dir, archive_type="test", custom_name="test_upload")
        print(f"Test archive created: {archive_path}")
        
        # Prepare upload metadata
        upload_metadata = {
            "test_upload": True,
            "created_at": datetime.now().isoformat(),
            "scrape_type": "test"
        }
        
        # Generate object key
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        object_key = f"test_uploads/{today}/test_archive_{timestamp}.zip"
        
        print(f"Uploading to object key: {object_key}")
        
        # Launch upload task
        upload_task = upload_only_task.apply_async(
            args=[str(archive_path), object_key, upload_metadata, True]
        )
        
        print(f"Upload task launched with Celery ID: {upload_task.id}")
        
        # Wait for result (with timeout)
        try:
            upload_result = upload_task.get(timeout=120)
            print(f"Upload task completed: {upload_result}")
            
            # Clean up test directory
            if test_dir.exists():
                import shutil
                shutil.rmtree(test_dir)
                print("Cleaned up test directory")
            
            return upload_result.get("uploaded", False)
        except Exception as e:
            print(f"Upload task failed or timed out: {e}")
            # Clean up on failure
            if test_dir.exists():
                import shutil
                shutil.rmtree(test_dir)
            if archive_path.exists():
                archive_path.unlink()
            return False
            
    except ImportError as e:
        print(f"Could not import upload task: {e}")
        return False
    except Exception as e:
        print(f"Error testing upload task: {e}")
        return False


def test_modular_scrape_task():
    """Test the new modular scheduled scrape task"""
    print("\n" + "="*50)
    print("Testing Modular Scheduled Task")
    print("="*50)
    
    try:
        from tasks import scheduled_scrape_task_modular
        
        print("Launching modular scheduled scrape task...")
        print("This will use the first enabled configuration")
        
        # Launch modular task
        modular_task = scheduled_scrape_task_modular.apply_async(
            args=[0]  # Use first config
        )
        
        print(f"Modular task launched with Celery ID: {modular_task.id}")
        
        # Wait for result (with longer timeout since it includes scraping)
        try:
            modular_result = modular_task.get(timeout=600)  # 10 minutes
            print(f"Modular task completed: {modular_result}")
            
            # Check if it's using modular execution
            if modular_result.get("modular_execution"):
                print("✅ Task used modular execution architecture")
                print(f"Database tasks: {len(modular_result.get('database_tasks', []))}")
                print(f"Upload task status: {modular_result.get('upload_task', {}).get('status')}")
            
            return modular_result.get("status") == "success"
            
        except Exception as e:
            print(f"Modular task failed or timed out: {e}")
            return False
            
    except ImportError as e:
        print(f"Could not import modular task: {e}")
        return False
    except Exception as e:
        print(f"Error testing modular task: {e}")
        return False


def test_independent_workflow():
    """Test a complete independent workflow"""
    print("\n" + "="*50)
    print("Testing Complete Independent Workflow")
    print("="*50)
    
    try:
        from tasks import (
            process_subreddit_config, 
            database_only_task, 
            archive_and_upload_task
        )
        
        # Step 1: Scraping only
        print("Step 1: Scraping data...")
        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")
        
        config = {
            "name": "CreditCardsIndia",
            "category": "h",  # hot posts
            "n_results": 3,   # Just a few for testing
            "options": {"csv": False, "auto_confirm": True},
            "enabled": True
        }
        
        scrape_result = process_subreddit_config(config, scrapes_dir)
        print(f"Scraping result: {scrape_result}")
        
        if scrape_result["status"] != "success":
            print("❌ Scraping failed, cannot continue workflow test")
            return False
        
        # Step 2: Launch database task independently
        print("\nStep 2: Launching database task...")
        task_id = f"independent_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        db_task = database_only_task.apply_async(
            args=[task_id, "test", config, scrape_result, scrape_result.get("scrape_file_path")]
        )
        print(f"Database task launched: {db_task.id}")
        
        # Step 3: Launch archive/upload task independently (don't wait for database)
        print("\nStep 3: Launching archive and upload task...")
        
        upload_metadata = {
            "workflow_test": True,
            "subreddit": scrape_result["subreddit"],
            "submissions_found": scrape_result.get("submissions_found", 0)
        }
        
        upload_task = archive_and_upload_task.apply_async(
            args=[str(scrapes_dir), "test", "independent_workflow", [config], [scrape_result], upload_metadata, True]
        )
        print(f"Upload task launched: {upload_task.id}")
        
        # Step 4: Wait for both tasks and report results
        print("\nStep 4: Waiting for tasks to complete...")
        
        db_success = False
        upload_success = False
        
        try:
            db_result = db_task.get(timeout=120)
            print(f"Database task result: {db_result}")
            db_success = db_result.get("database_saved", False)
        except Exception as e:
            print(f"Database task error: {e}")
        
        try:
            upload_result = upload_task.get(timeout=180)
            print(f"Upload task result: {upload_result}")
            upload_success = upload_result.get("status") == "success"
        except Exception as e:
            print(f"Upload task error: {e}")
        
        print(f"\n🎯 Workflow Results:")
        print(f"   Scraping: ✅ Success")
        print(f"   Database: {'✅ Success' if db_success else '❌ Failed'}")
        print(f"   Upload: {'✅ Success' if upload_success else '❌ Failed'}")
        
        return db_success and upload_success
        
    except ImportError as e:
        print(f"Could not import required tasks: {e}")
        return False
    except Exception as e:
        print(f"Error in independent workflow test: {e}")
        return False


def main():
    """Run all tests"""
    print("🚀 Independent Tasks Test Suite")
    print("=" * 60)
    
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
    
    tests = [
        ("Database Only Task", test_database_only_task),
        ("Upload Only Task", test_upload_only_task),
        ("Modular Scrape Task", test_modular_scrape_task),
        ("Independent Workflow", test_independent_workflow)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        try:
            success = test_func()
            results[test_name] = success
            print(f"Result: {'✅ PASSED' if success else '❌ FAILED'}")
        except Exception as e:
            print(f"Result: ❌ ERROR - {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name:.<40} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Independent tasks are working correctly.")
        return True
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 