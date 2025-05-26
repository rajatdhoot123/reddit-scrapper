#!/usr/bin/env python3

"""
Test script to verify Flower visibility of Celery tasks
"""

import time
from celery_config import app
from tasks import test_scrape_task, scrape_and_upload_to_r2

def test_task_visibility():
    """Test if tasks are visible in Flower"""
    print("Testing Celery task visibility in Flower...")
    
    # Test 1: Simple task execution
    print("\n1. Testing test_scrape_task...")
    result = test_scrape_task.delay()
    print(f"Task ID: {result.id}")
    print(f"Task State: {result.state}")
    
    # Wait a bit and check status
    time.sleep(2)
    print(f"Task State after 2s: {result.state}")
    
    # Test 2: Check if task is in active tasks
    print("\n2. Checking active tasks...")
    active_tasks = app.control.inspect().active()
    print(f"Active tasks: {active_tasks}")
    
    # Test 3: Check registered tasks
    print("\n3. Checking registered tasks...")
    registered_tasks = app.control.inspect().registered()
    print(f"Registered tasks: {registered_tasks}")
    
    # Test 4: Check task stats
    print("\n4. Checking task stats...")
    stats = app.control.inspect().stats()
    print(f"Worker stats: {stats}")
    
    print(f"\nTask result: {result.get(timeout=30)}")
    print("Test completed!")

if __name__ == "__main__":
    test_task_visibility() 