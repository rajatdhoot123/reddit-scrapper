#!/usr/bin/python

import json
import os
import subprocess
import time
import pexpect
import sys
from datetime import datetime
from pathlib import Path
import argparse

# Category mapping for consistent naming
CATEGORY_MAPPING = {
    "n": "new",
    "h": "hot",
    "t": "top",
    "r": "rising",
    "c": "controversial"
}

def get_latest_scrape_file(subreddit, category, n_results):
    """Get the most recent scrape file for a subreddit"""
    today = datetime.now().strftime("%Y-%m-%d")
    scrapes_dir = Path(f"scrapes/{today}/subreddits")
    
    print(f"Looking for scrape file in: {scrapes_dir}")
    if not scrapes_dir.exists():
        return None
        
    # Use the full category name from mapping, fallback to original if not found
    file_category = CATEGORY_MAPPING.get(category, category)
    expected_filename = f"{subreddit}-{file_category}-{n_results}-results.json"
    print(f"Expected file: {expected_filename}")
    file_path = scrapes_dir / expected_filename
    
    # Try up to 5 times with a 2 second delay between attempts
    for attempt in range(5):
        print(f"Checking file (attempt {attempt + 1}/5): {file_path}")
        if file_path.exists():
            return file_path
        time.sleep(2)
    
    return None

def extract_submission_urls(json_file):
    """Extract all submission URLs from a subreddit scrape JSON file"""
    with open(json_file) as f:
        json_data = json.load(f)
    
    urls = []
    for post in json_data["data"]:  # Posts are in the data array
        # Get full URL by combining reddit.com with permalink
        url = f"https://www.reddit.com{post['permalink']}"
        urls.append(url)
    return urls

def scrape_subreddit(subreddit, category, n_results):
    """Scrape a subreddit using URS"""
    print(f"Scraping r/{subreddit} with category {category} and {n_results} results")
    
    # Store the current working directory
    original_dir = os.getcwd()
    
    try:
        # Change to the urs directory
        os.chdir('urs')
        
        # Modify command to run from urs directory
        cmd = f"poetry run python Urs.py -r {subreddit} {category} {n_results}"
        
        try:
            # Spawn the process with longer timeout
            process = pexpect.spawn(cmd, timeout=120, encoding='utf-8')
            
            # Enable logging of the output
            process.logfile = sys.stdout
            
            # Wait for the confirmation prompt and respond
            index = process.expect(['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
            if index == 0:  # Found the prompt
                process.sendline("y")
                process.expect(pexpect.EOF, timeout=120)  # Wait longer for completion
            elif index == 1:  # EOF before prompt
                print("Process ended before prompt")
            else:  # Timeout
                print("Timed out waiting for prompt")
                
        except Exception as e:
            print(f"Error running command: {e}")
            if process.isalive():
                process.terminate()
            return None
        finally:
            # Change back to original directory
            os.chdir(original_dir)
            
        # Get the generated file
        return get_latest_scrape_file(subreddit, category, n_results)
        
    except Exception as e:
        print(f"Error changing directory: {e}")
        # Ensure we change back to original directory even if an error occurs
        os.chdir(original_dir)
        return None

def scrape_comments(url, n_comments=0):
    """Scrape comments from a submission using URS"""
    print(f"\nScraping comments from URL: {url}")
    print(f"Number of comments to scrape: {'all' if n_comments == 0 else n_comments}")
    
    # Store the current working directory
    original_dir = os.getcwd()
    print(f"Original directory: {original_dir}")
    
    try:
        # Change to the urs directory
        os.chdir('urs')
        print(f"Changed to directory: {os.getcwd()}")
        
        # Modify command to run from urs directory
        cmd = f"poetry run python Urs.py -c {url} {n_comments}"
        print(f"Running command: {cmd}")
        
        try:
            # Spawn the process with longer timeout
            process = pexpect.spawn(cmd, timeout=120, encoding='utf-8')
            
            # Enable logging of the output
            process.logfile = sys.stdout
            
            # Wait for the confirmation prompt and respond
            print("Waiting for confirmation prompt...")
            index = process.expect(['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
            if index == 0:  # Found the prompt
                print("Found prompt, sending 'y'")
                process.sendline("y")
                print("Waiting for process to complete...")
                process.expect(pexpect.EOF, timeout=120)  # Wait longer for completion
                print("Process completed")
            elif index == 1:  # EOF before prompt
                print("Process ended before prompt")
            else:  # Timeout
                print("Timed out waiting for prompt")
                
        except Exception as e:
            print(f"Error running command: {e}")
            if process.isalive():
                process.terminate()
        finally:
            # Change back to original directory
            os.chdir(original_dir)
            print(f"Changed back to original directory: {os.getcwd()}")
            
    except Exception as e:
        print(f"Error changing directory: {e}")
        # Ensure we change back to original directory even if an error occurs
        os.chdir(original_dir)
        print(f"Changed back to original directory after error: {os.getcwd()}")

def main():
    parser = argparse.ArgumentParser(
        description="Scrape subreddits and their comments using URS.",
        epilog="Example: python automate_scraping.py --scrape CreditCardsIndia n 100 --scrape LifeProTips h 50"
    )
    parser.add_argument(
        '--scrape', 
        nargs=3, 
        action='append', 
        metavar=('SUBREDDIT', 'CATEGORY', 'N_RESULTS'),
        help='Specify a subreddit scrape configuration: name, category (n/h/t/r/c), and number of results. Use this argument multiple times for multiple subreddits.',
        required=True
    )

    args = parser.parse_args()

    # Build scrape_configs from arguments
    scrape_configs = []
    for scrape_arg in args.scrape:
        try:
            name, category, n_results_str = scrape_arg
            n_results = int(n_results_str)
            if category not in CATEGORY_MAPPING:
                print(f"Warning: Invalid category '{category}' for subreddit '{name}'. Using '{category}' directly. Valid categories: {list(CATEGORY_MAPPING.keys())}")
            if n_results <= 0:
                raise ValueError("Number of results must be positive.")
                
            scrape_configs.append({
                "name": name,
                "category": category,
                "n_results": n_results
            })
        except ValueError as e:
            print(f"Error parsing arguments for '--scrape {scrape_arg}': Number of results must be an integer. {e} Skipping this configuration.")
            continue
        except Exception as e:
             print(f"Error processing scrape argument {scrape_arg}: {e}. Skipping this configuration.")
             continue

    if not scrape_configs:
        print("Error: No valid scrape configurations provided. Exiting.")
        sys.exit(1)
        
    # List of subreddits and their specific scraping parameters
    # scrape_configs = [
    #     {
    #         "name": "CreditCardsIndia",
    #         "category": "n",  # 'n' for new
    #         "n_results": 100 
    #     },
    #     {
    #         "name": "LifeProTips", 
    #         "category": "h",  # 'h' for hot
    #         "n_results": 50
    #     }
    #     # Add more subreddit configurations here
    # ]
    
    for config in scrape_configs:
        subreddit = config["name"]
        category = config["category"]
        n_results = config["n_results"]
        
        print(f"--- Starting scraping for r/{subreddit} (Category: {category}, Results: {n_results}) ---")
        
        # 1. First scrape the subreddit
        print(f"Scraping r/{subreddit}...")
        scrape_file = scrape_subreddit(subreddit, category, n_results)
        
        if not scrape_file:
            print(f"Error: Could not find the generated scrape file for r/{subreddit}. Skipping.")
            continue  # Move to the next subreddit
            
        # 2. Extract submission URLs
        print("Extracting submission URLs...")
        try:
            urls = extract_submission_urls(scrape_file)
        except Exception as e:
            print(f"Error extracting URLs from {scrape_file}: {e}. Skipping comment scraping for r/{subreddit}.")
            continue # Move to the next subreddit

        # 3. Scrape comments for each submission
        print(f"Scraping comments from {len(urls)} submissions in r/{subreddit}...")
        for i, url in enumerate(urls, 1):
            print(f"Scraping comments from submission {i}/{len(urls)} (r/{subreddit})")
            scrape_comments(url, 0)  # 0 means scrape all comments
            
        print(f"--- Finished scraping for r/{subreddit} ---")

if __name__ == "__main__":
    main() 
    
    
    
# python automate_scraping.py --scrape CreditCardsIndia n 100 --scrape LifeProTips h 50 --scrape anotherSub t 25



# python automate_scraping.py --scrape --scrape LifeProTips h 100