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
    
    print("\n=== Available Schedules ===")
    schedules = [
        "daily_scrapes",
        "weekly_scrapes", 
        "hourly_hot_scrapes",
        "custom_interval_scrapes"
    ]
    
    for schedule in schedules:
        print(f"  {schedule}")


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
  
  # Disable hourly scrapes
  python config_editor.py schedule hourly_hot_scrapes false
  
  # Enable weekly scrapes
  python config_editor.py schedule weekly_scrapes true
  
  # List available settings
  python config_editor.py list
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Global settings command
    global_parser = subparsers.add_parser('global', help='Toggle global settings')
    global_parser.add_argument('setting', help='Global setting name')
    global_parser.add_argument('enabled', choices=['true', 'false'], help='Enable or disable')
    
    # Schedule command
    schedule_parser = subparsers.add_parser('schedule', help='Toggle schedule')
    schedule_parser.add_argument('schedule_name', help='Schedule name')
    schedule_parser.add_argument('enabled', choices=['true', 'false'], help='Enable or disable')
    
    # List command
    subparsers.add_parser('list', help='List available settings')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'global':
        enabled = args.enabled.lower() == 'true'
        toggle_global_setting(args.setting, enabled)
        
    elif args.command == 'schedule':
        enabled = args.enabled.lower() == 'true'
        toggle_schedule(args.schedule_name, enabled)
        
    elif args.command == 'list':
        list_available_settings()
    
    if args.command in ['global', 'schedule']:
        print("\n⚠️  Remember to restart Celery for changes to take effect:")
        print("pkill -f celery")
        print("celery -A celery_config worker --loglevel=info &")
        print("celery -A celery_config beat --loglevel=info &")


if __name__ == '__main__':
    main() 