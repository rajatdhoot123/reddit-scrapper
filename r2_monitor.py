#!/usr/bin/env python3

import os
import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import List, Dict, Any
import argparse

# Load environment variables
load_dotenv()

# R2/S3 Configuration
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', 'creditcardsindia')


def get_r2_client():
    """Initialize and return R2 client"""
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )


def list_r2_files(prefix: str = "", days_back: int = 7) -> List[Dict[str, Any]]:
    """List files in R2 bucket with optional prefix filter"""
    try:
        r2_client = get_r2_client()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        response = r2_client.list_objects_v2(
            Bucket=R2_BUCKET_NAME,
            Prefix=prefix
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                # Filter by date if within range
                if obj['LastModified'].replace(tzinfo=None) >= start_date:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'size_mb': round(obj['Size'] / (1024 * 1024), 2)
                    })
        
        return sorted(files, key=lambda x: x['last_modified'], reverse=True)
        
    except Exception as e:
        print(f"Error listing R2 files: {e}")
        return []


def get_file_metadata(object_key: str) -> Dict[str, Any]:
    """Get metadata for a specific file in R2"""
    try:
        r2_client = get_r2_client()
        response = r2_client.head_object(Bucket=R2_BUCKET_NAME, Key=object_key)
        
        return {
            'content_length': response.get('ContentLength', 0),
            'last_modified': response.get('LastModified'),
            'metadata': response.get('Metadata', {}),
            'content_type': response.get('ContentType', 'unknown')
        }
    except Exception as e:
        print(f"Error getting metadata for {object_key}: {e}")
        return {}


def analyze_scrape_patterns():
    """Analyze scraping patterns and identify potential conflicts"""
    print("=== R2 Scrape Analysis ===\n")
    
    # Check different scrape types
    scrape_types = [
        ("daily_scrapes", "Daily Scrapes"),
        ("hourly_hot_scrapes", "Hourly Hot Scrapes"),
        ("weekly_scrapes", "Weekly Scrapes"),
        ("manual_scrapes", "Manual Scrapes")
    ]
    
    for prefix, name in scrape_types:
        print(f"--- {name} ---")
        files = list_r2_files(prefix=prefix, days_back=7)
        
        if not files:
            print("No files found in the last 7 days")
        else:
            print(f"Found {len(files)} files:")
            for file in files[:10]:  # Show last 10 files
                print(f"  {file['key']} ({file['size_mb']} MB) - {file['last_modified']}")
                
                # Get metadata for recent files
                if file['last_modified'].replace(tzinfo=None) > datetime.now() - timedelta(days=1):
                    metadata = get_file_metadata(file['key'])
                    if metadata.get('metadata'):
                        print(f"    Metadata: {metadata['metadata']}")
            
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more files")
        
        print()


def check_recent_uploads(hours: int = 24):
    """Check for recent uploads in the last N hours"""
    print(f"=== Recent Uploads (Last {hours} hours) ===\n")
    
    cutoff_time = datetime.now() - timedelta(hours=hours)
    all_files = list_r2_files(days_back=2)  # Get last 2 days to be safe
    
    recent_files = [f for f in all_files if f['last_modified'].replace(tzinfo=None) > cutoff_time]
    
    if not recent_files:
        print("No recent uploads found")
        return
    
    print(f"Found {len(recent_files)} recent uploads:")
    for file in recent_files:
        print(f"  {file['key']}")
        print(f"    Size: {file['size_mb']} MB")
        print(f"    Uploaded: {file['last_modified']}")
        
        # Get metadata
        metadata = get_file_metadata(file['key'])
        if metadata.get('metadata'):
            print(f"    Metadata: {metadata['metadata']}")
        print()


def check_naming_conflicts():
    """Check for potential naming conflicts"""
    print("=== Checking for Naming Conflicts ===\n")
    
    # Get all files from last 7 days
    all_files = list_r2_files(days_back=7)
    
    # Group by date and schedule type
    date_schedule_groups = {}
    
    for file in all_files:
        key = file['key']
        parts = key.split('/')
        
        if len(parts) >= 2:
            schedule_type = parts[0].replace('_scrapes', '')
            date_part = parts[1]
            
            group_key = f"{schedule_type}_{date_part}"
            if group_key not in date_schedule_groups:
                date_schedule_groups[group_key] = []
            date_schedule_groups[group_key].append(file)
    
    # Check for multiple files in same group
    conflicts_found = False
    for group_key, files in date_schedule_groups.items():
        if len(files) > 1:
            conflicts_found = True
            print(f"Multiple files found for {group_key}:")
            for file in files:
                print(f"  {file['key']} - {file['last_modified']}")
            print()
    
    if not conflicts_found:
        print("No naming conflicts detected")


def main():
    parser = argparse.ArgumentParser(description="Monitor R2 uploads and check for issues")
    parser.add_argument('--analyze', action='store_true', help='Analyze scrape patterns')
    parser.add_argument('--recent', type=int, default=24, help='Check recent uploads (hours)')
    parser.add_argument('--conflicts', action='store_true', help='Check for naming conflicts')
    parser.add_argument('--list', type=str, help='List files with prefix')
    
    args = parser.parse_args()
    
    if args.analyze:
        analyze_scrape_patterns()
    
    if args.recent:
        check_recent_uploads(args.recent)
    
    if args.conflicts:
        check_naming_conflicts()
    
    if args.list:
        files = list_r2_files(prefix=args.list)
        print(f"Files with prefix '{args.list}':")
        for file in files:
            print(f"  {file['key']} ({file['size_mb']} MB) - {file['last_modified']}")
    
    if not any([args.analyze, args.recent, args.conflicts, args.list]):
        # Default: show recent uploads and conflicts
        check_recent_uploads(24)
        check_naming_conflicts()


if __name__ == '__main__':
    main() 