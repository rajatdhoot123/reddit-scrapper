#!/usr/bin/env python3

import json
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def find_comments_file_by_reddit_id_debug(reddit_id: str, date_str: str = None):
    """Debug version of find_comments_file_by_reddit_id"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    
    comments_dir = Path(f"scrapes/{date_str}/comments")
    
    print(f"Looking for Reddit ID: {reddit_id}")
    print(f"Comments directory: {comments_dir}")
    print(f"Directory exists: {comments_dir.exists()}")
    
    if not comments_dir.exists():
        print("Comments directory does not exist!")
        return None
    
    json_files = list(comments_dir.glob('*.json'))
    print(f"Number of JSON files in directory: {len(json_files)}")
    if json_files:
        print(f"First few files: {[f.name for f in json_files[:3]]}")
    
    # Look through all comment files and check their content
    found_files = []
    for file_path in comments_dir.glob("*.json"):
        print(f"\nChecking file: {file_path.name}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if the URL in scrape_settings contains our Reddit ID
            url = data.get('scrape_settings', {}).get('url', '')
            print(f"  URL: {url}")
            print(f"  Reddit ID '{reddit_id}' in URL: {reddit_id in url}")
            
            if reddit_id in url:
                print(f"  FOUND! Comment file for Reddit ID {reddit_id}: {file_path}")
                found_files.append(file_path)
                
            # Also check permalink in submission_metadata as backup
            permalink = data.get('data', {}).get('submission_metadata', {}).get('permalink', '')
            print(f"  Permalink: {permalink}")
            print(f"  Reddit ID '{reddit_id}' in permalink: {reddit_id in permalink}")
            
            if reddit_id in permalink:
                print(f"  FOUND via permalink! Comment file for Reddit ID {reddit_id}: {file_path}")
                if file_path not in found_files:
                    found_files.append(file_path)
                
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Error reading file: {e}")
            continue
    
    if found_files:
        print(f"\nFound {len(found_files)} matching files: {found_files}")
        return found_files[0]  # Return first match
    else:
        print(f"\nNo comment file found for Reddit ID: {reddit_id}")
        return None

if __name__ == "__main__":
    # Test with known submission IDs from yesterday's scrape
    test_ids = ['1j9bd4e', '1kwf0a5', '1ky44lo']
    
    for reddit_id in test_ids:
        print("=" * 60)
        result = find_comments_file_by_reddit_id_debug(reddit_id, "2025-05-29")
        print(f"Final result for {reddit_id}: {result}")
        print() 