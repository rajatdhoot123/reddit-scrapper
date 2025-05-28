#!/usr/bin/env python3

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Any


def read_config_file():
    """Read the current configuration file"""
    config_file = Path("subreddit_config.py")
    if not config_file.exists():
        print("Error: subreddit_config.py not found")
        return None
    
    with open(config_file, 'r') as f:
        return f.read()


def write_config_file(content: str):
    """Write the updated configuration file"""
    config_file = Path("subreddit_config.py")
    
    # Create backup
    backup_file = Path("subreddit_config.py.backup")
    if config_file.exists():
        with open(config_file, 'r') as f:
            backup_content = f.read()
        with open(backup_file, 'w') as f:
            f.write(backup_content)
        print(f"Backup created: {backup_file}")
    
    with open(config_file, 'w') as f:
        f.write(content)
    print(f"Configuration updated: {config_file}")


def toggle_global_setting(setting_name: str, enabled: bool):
    """Toggle a global setting"""
    content = read_config_file()
    if not content:
        return
    
    # Pattern to match the setting in GLOBAL_SCRAPING_CONFIG
    pattern = rf'("{setting_name}"\s*:\s*)(True|False)'
    replacement = rf'\g<1>{str(enabled)}'
    
    new_content = re.sub(pattern, replacement, content)
    
    if new_content == content:
        print(f"Warning: Setting '{setting_name}' not found or already set to {enabled}")
        return
    
    write_config_file(new_content)
    print(f"✓ Set {setting_name} = {enabled}")


def toggle_schedule(schedule_name: str, enabled: bool):
    """Toggle a schedule on/off"""
    content = read_config_file()
    if not content:
        return
    
    # Pattern to match the schedule's enabled setting
    pattern = rf"('{schedule_name}'\s*:\s*\{{[^}}]*?'enabled'\s*:\s*)(True|False)"
    replacement = rf'\g<1>{str(enabled)}'
    
    new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    
    if new_content == content:
        print(f"Warning: Schedule '{schedule_name}' not found or already set to {enabled}")
        return
    
    write_config_file(new_content)
    print(f"✓ Set schedule '{schedule_name}' enabled = {enabled}")


def toggle_subreddit_in_schedule(schedule_name: str, subreddit_name: str, enabled: bool):
    """Toggle a specific subreddit within a schedule"""
    content = read_config_file()
    if not content:
        return
    
    # This is more complex - we need to find the specific subreddit config within the schedule
    # For now, provide instructions
    print(f"To toggle r/{subreddit_name} in {schedule_name}:")
    print(f"1. Open subreddit_config.py")
    print(f"2. Find the '{schedule_name}' section")
    print(f"3. Find the subreddit config with 'name': '{subreddit_name}'")
    print(f"4. Set 'enabled': {enabled}")
    print("5. Save the file and restart Celery")


def list_available_settings():
    """List all available settings that can be toggled"""
    print("=== Available Global Settings ===")
    global_settings = [
        "master_enabled",
        "scheduled_scraping_enabled", 
        "manual_scraping_enabled",
        "comment_scraping_globally_enabled",
        "upload_to_r2_enabled",
        "create_archives_enabled"
    ]
    
    for setting in global_settings:
        print(f"  {setting}")
    
    print("\n=== Individual Configuration IDs ===")
    print("Use 'python scrape_manager.py list-configs' to see all configurations with their IDs")
    print("Configuration IDs range from 0 to N-1 where N is the total number of configs")


def toggle_config_by_id(config_id: int, enabled: bool):
    """Toggle a specific configuration by ID"""
    content = read_config_file()
    if not content:
        return
    
    print(f"To toggle configuration {config_id}:")
    print(f"1. Open subreddit_config.py")
    print(f"2. Find SUBREDDIT_CONFIGS[{config_id}]")
    print(f"3. Set 'enabled': {enabled}")
    print("4. Save the file and restart Celery")


def restart_celery_services():
    """Restart Celery worker and beat services"""
    import subprocess
    import time
    
    print("🔄 Restarting Celery services...")
    
    try:
        # Kill existing Celery processes
        print("Stopping existing Celery processes...")
        subprocess.run(["pkill", "-f", "celery"], check=False)
        time.sleep(2)  # Give processes time to stop
        
        # Start worker in background
        print("Starting Celery worker...")
        worker_process = subprocess.Popen(
            ["celery", "-A", "celery_config", "worker", "--loglevel=info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Start beat in background
        print("Starting Celery beat...")
        beat_process = subprocess.Popen(
            ["celery", "-A", "celery_config", "beat", "--loglevel=info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        time.sleep(3)  # Give services time to start
        
        # Check if processes are running
        if worker_process.poll() is None and beat_process.poll() is None:
            print("✅ Celery services restarted successfully!")
            print(f"Worker PID: {worker_process.pid}")
            print(f"Beat PID: {beat_process.pid}")
        else:
            print("❌ Failed to start one or more Celery services")
            
    except Exception as e:
        print(f"❌ Error restarting Celery services: {e}")
        print("Please restart manually:")
        print("pkill -f celery")
        print("celery -A celery_config worker --loglevel=info &")
        print("celery -A celery_config beat --loglevel=info &")


def main():
    parser = argparse.ArgumentParser(
        description="Configuration Editor for Reddit Scraping System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Disable all scraping globally
  python config_editor.py global master_enabled false
  
  # Disable comment scraping globally
  python config_editor.py global comment_scraping_globally_enabled false
  
  # Disable a specific configuration (by ID)
  python config_editor.py config 0 false
  
  # Enable a specific configuration (by ID)
  python config_editor.py config 1 true
  
  # List available settings
  python config_editor.py list
  
  # Restart Celery services after changes
  python config_editor.py restart
  
  # Monitor R2 uploads (separate script)
  python r2_monitor.py --analyze
  python r2_monitor.py --recent 12
  python r2_monitor.py --conflicts
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Global settings command
    global_parser = subparsers.add_parser('global', help='Toggle global settings')
    global_parser.add_argument('setting', help='Global setting name')
    global_parser.add_argument('enabled', choices=['true', 'false'], help='Enable or disable')
    
    # Config command (replaces schedule command)
    config_parser = subparsers.add_parser('config', help='Toggle configuration by ID')
    config_parser.add_argument('config_id', type=int, help='Configuration ID (0-based index)')
    config_parser.add_argument('enabled', choices=['true', 'false'], help='Enable or disable')
    
    # List command
    subparsers.add_parser('list', help='List available settings')
    
    # Restart command
    subparsers.add_parser('restart', help='Restart Celery services')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'global':
        toggle_global_setting(args.setting, args.enabled == 'true')
    elif args.command == 'config':
        toggle_config_by_id(args.config_id, args.enabled == 'true')
    elif args.command == 'list':
        list_available_settings()
    elif args.command == 'restart':
        restart_celery_services()
    else:
        parser.print_help()


if __name__ == '__main__':
    main() 