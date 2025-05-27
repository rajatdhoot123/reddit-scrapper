#!/usr/bin/env python3

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from celery_config import app
from subreddit_config import SUBREDDIT_SCHEDULES, MANUAL_SUBREDDIT_CONFIGS, GLOBAL_SCRAPING_CONFIG
import tasks


def print_status():
    """Print the current status of scraping tasks"""
    try:
        result = tasks.get_scraping_status.apply_async().get(timeout=10)
        print("=== Reddit Scraping Status ===")
        print(f"Timestamp: {result['timestamp']}")
        print(f"Available Schedules: {', '.join(result['available_schedules'])}")
        
        if result.get('active_tasks'):
            print("\nActive Tasks:")
            for worker, task_list in result['active_tasks'].items():
                if task_list:
                    print(f"  {worker}: {len(task_list)} tasks")
                    for task in task_list:
                        print(f"    - {task['name']} (ID: {task['id'][:8]}...)")
        else:
            print("\nNo active tasks")
        
        if result.get('scheduled_tasks'):
            print("\nScheduled Tasks:")
            for worker, task_list in result['scheduled_tasks'].items():
                if task_list:
                    print(f"  {worker}: {len(task_list)} tasks")
        else:
            print("\nNo scheduled tasks")
            
    except Exception as e:
        print(f"Error getting status: {e}")


def run_manual_scrape(subreddit: str, category: str, n_results_or_keywords: str,
                     time_filter: Optional[str] = None, csv: bool = True, 
                     rules: bool = False, no_comments: bool = False):
    """Run a manual scrape for a specific subreddit"""
    try:
        # Convert n_results_or_keywords to int if it's numeric
        try:
            n_results_or_keywords = int(n_results_or_keywords)
        except ValueError:
            # Keep as string for search keywords
            pass
        
        options = {
            "csv": csv,
            "rules": rules,
            "auto_confirm": True
        }
        
        print(f"Starting manual scrape for r/{subreddit}...")
        print(f"Category: {category}")
        print(f"Results/Keywords: {n_results_or_keywords}")
        print(f"Time Filter: {time_filter}")
        print(f"Options: {options}")
        
        result = tasks.manual_scrape_subreddit.apply_async(
            args=[subreddit, category, n_results_or_keywords],
            kwargs={
                "time_filter": time_filter,
                "options": options,
                "scrape_comments": not no_comments
            }
        ).get(timeout=600)  # 10 minute timeout
        
        print("\n=== Manual Scrape Result ===")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error running manual scrape: {e}")


def run_scheduled_task(schedule_name: str):
    """Run a specific scheduled task"""
    if schedule_name not in SUBREDDIT_SCHEDULES:
        print(f"Error: Unknown schedule '{schedule_name}'")
        print(f"Available schedules: {', '.join(SUBREDDIT_SCHEDULES.keys())}")
        return
    
    try:
        print(f"Starting scheduled task: {schedule_name}")
        result = tasks.scheduled_scrape_task.apply_async(
            args=[schedule_name]
        ).get(timeout=1800)  # 30 minute timeout
        
        print("\n=== Scheduled Task Result ===")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error running scheduled task: {e}")


def run_manual_config_scrape():
    """Run scraping using predefined manual configurations"""
    try:
        print("Starting manual scrape from predefined configurations...")
        print(f"Configurations to process: {len(MANUAL_SUBREDDIT_CONFIGS)}")
        
        result = tasks.manual_scrape_from_config.apply_async().get(timeout=1800)
        
        print("\n=== Manual Config Scrape Result ===")
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"Error running manual config scrape: {e}")


def list_configurations():
    """List all available configurations with their enable/disable status"""
    print("=== Global Configuration ===")
    for key, value in GLOBAL_SCRAPING_CONFIG.items():
        status = "✓ ENABLED" if value else "✗ DISABLED"
        print(f"  {key}: {status}")
    
    print("\n=== Scheduled Configurations ===")
    for schedule_name, config in SUBREDDIT_SCHEDULES.items():
        schedule_status = "✓ ENABLED" if config.get('enabled', True) else "✗ DISABLED"
        print(f"\n{schedule_name}: {schedule_status}")
        print(f"  Schedule: {config['schedule']}")
        print(f"  Subreddits: {len(config['subreddits'])}")
        for subreddit_config in config['subreddits']:
            name = subreddit_config['name']
            category = subreddit_config['category']
            n_results = subreddit_config.get('n_results', subreddit_config.get('keywords', 'N/A'))
            time_filter = subreddit_config.get('time_filter', 'None')
            sub_status = "✓" if subreddit_config.get('enabled', True) else "✗"
            print(f"    {sub_status} r/{name} ({category}, {n_results}, {time_filter})")
    
    print("\n=== Manual Configurations ===")
    for i, config in enumerate(MANUAL_SUBREDDIT_CONFIGS, 1):
        name = config['name']
        category = config['category']
        n_results = config.get('n_results', config.get('keywords', 'N/A'))
        time_filter = config.get('time_filter', 'None')
        status = "✓" if config.get('enabled', True) else "✗"
        print(f"{i}. {status} r/{name} ({category}, {n_results}, {time_filter})")


def toggle_schedule(schedule_name: str, enabled: bool):
    """Toggle a schedule on or off"""
    if schedule_name not in SUBREDDIT_SCHEDULES:
        print(f"Error: Unknown schedule '{schedule_name}'")
        print(f"Available schedules: {', '.join(SUBREDDIT_SCHEDULES.keys())}")
        return
    
    # This would require modifying the config file
    print(f"To {'enable' if enabled else 'disable'} schedule '{schedule_name}':")
    print(f"Edit subreddit_config.py and set:")
    print(f"SUBREDDIT_SCHEDULES['{schedule_name}']['enabled'] = {enabled}")
    print("Then restart Celery for changes to take effect.")


def show_config_help():
    """Show help for configuration management"""
    print("=== Configuration Management Help ===")
    print("\nTo enable/disable configurations, edit subreddit_config.py:")
    print("\n1. Global Controls (GLOBAL_SCRAPING_CONFIG):")
    print("   - master_enabled: Master switch for all scraping")
    print("   - scheduled_scraping_enabled: Enable/disable all scheduled tasks")
    print("   - manual_scraping_enabled: Enable/disable manual scraping")
    print("   - comment_scraping_globally_enabled: Global comment scraping toggle")
    print("   - upload_to_r2_enabled: Enable/disable R2 uploads")
    print("   - create_archives_enabled: Enable/disable archive creation")
    
    print("\n2. Schedule-level Controls:")
    print("   SUBREDDIT_SCHEDULES['schedule_name']['enabled'] = True/False")
    
    print("\n3. Individual Subreddit Controls:")
    print("   Each subreddit config has an 'enabled' field")
    
    print("\nExamples:")
    print("# Disable hourly scrapes")
    print("SUBREDDIT_SCHEDULES['hourly_hot_scrapes']['enabled'] = False")
    print("\n# Disable a specific subreddit in weekly scrapes")
    print("# Find the subreddit config and set 'enabled': False")
    
    print("\n# Disable all comment scraping globally")
    print("GLOBAL_SCRAPING_CONFIG['comment_scraping_globally_enabled'] = False")
    
    print("\nAfter making changes, restart Celery:")
    print("pkill -f celery && celery -A celery_config worker --loglevel=info &")
    print("celery -A celery_config beat --loglevel=info &")


def test_connection():
    """Test the connection to Celery and Redis"""
    try:
        print("Testing Celery connection...")
        result = tasks.test_scrape_task.apply_async().get(timeout=30)
        print("✓ Celery connection successful")
        print(f"Test result: {result}")
    except Exception as e:
        print(f"✗ Celery connection failed: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Reddit Scraping Manager - Enhanced URS automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check status
  python scrape_manager.py status
  
  # Run manual scrape
  python scrape_manager.py manual CreditCardsIndia t 25 --time-filter day --csv
  
  # Search subreddit
  python scrape_manager.py manual CreditCardsIndia s "cashback rewards" --time-filter month
  
  # Run scheduled task
  python scrape_manager.py schedule daily_scrapes
  
  # Run predefined manual configurations
  python scrape_manager.py manual-config
  
  # List all configurations with status
  python scrape_manager.py list-configs
  
  # Show configuration help
  python scrape_manager.py config-help
  
  # Test connection
  python scrape_manager.py test
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    subparsers.add_parser('status', help='Show current scraping status')
    
    # Manual scrape command
    manual_parser = subparsers.add_parser('manual', help='Run manual scrape')
    manual_parser.add_argument('subreddit', help='Subreddit name (without r/)')
    manual_parser.add_argument('category', choices=['h', 'n', 'c', 't', 'r', 's'],
                              help='Category: h=hot, n=new, c=controversial, t=top, r=rising, s=search')
    manual_parser.add_argument('n_results_or_keywords', 
                              help='Number of results (for h,n,c,t,r) or search keywords (for s)')
    manual_parser.add_argument('--time-filter', choices=['day', 'week', 'month', 'year', 'all'],
                              help='Time filter (for top and controversial)')
    manual_parser.add_argument('--csv', action='store_true', default=True,
                              help='Output in CSV format (default: True)')
    manual_parser.add_argument('--no-csv', action='store_false', dest='csv',
                              help='Output in JSON format')
    manual_parser.add_argument('--rules', action='store_true',
                              help='Include subreddit rules')
    manual_parser.add_argument('--no-comments', action='store_true',
                              help='Skip comment scraping')
    
    # Scheduled task command
    schedule_parser = subparsers.add_parser('schedule', help='Run scheduled task')
    schedule_parser.add_argument('schedule_name', choices=list(SUBREDDIT_SCHEDULES.keys()),
                                help='Schedule name to run')
    
    # Manual config command
    subparsers.add_parser('manual-config', help='Run predefined manual configurations')
    
    # List configurations command
    subparsers.add_parser('list-configs', help='List all available configurations with status')
    
    # Configuration help command
    subparsers.add_parser('config-help', help='Show configuration management help')
    
    # Test command
    subparsers.add_parser('test', help='Test Celery connection')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == 'status':
        print_status()
    elif args.command == 'manual':
        run_manual_scrape(
            args.subreddit, args.category, args.n_results_or_keywords,
            args.time_filter, args.csv, args.rules, args.no_comments
        )
    elif args.command == 'schedule':
        run_scheduled_task(args.schedule_name)
    elif args.command == 'manual-config':
        run_manual_config_scrape()
    elif args.command == 'list-configs':
        list_configurations()
    elif args.command == 'config-help':
        show_config_help()
    elif args.command == 'test':
        test_connection()


if __name__ == '__main__':
    main() 