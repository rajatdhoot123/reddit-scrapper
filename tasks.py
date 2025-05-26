#!/usr/bin/env python3

from celery_config import app
import os
import json
import boto3
import zipfile
import shutil
from datetime import datetime
from pathlib import Path
from celery import Celery
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
import subprocess
import time
import pexpect
import sys
import random

# Load environment variables
load_dotenv()

# Import the Celery app from config

# Set up logging
logger = get_task_logger(__name__)

# R2/S3 Configuration
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', 'creditcardsindia')

# Reddit scraping configuration
SUBREDDIT_CONFIGS = [
    # {
    #     "name": "CreditCardsIndia",
    #     "category": "n",  # new posts
    #     "n_results": 100
    # },
    # {
    #     "name": "CreditCardsIndia",
    #     "category": "h",  # hot posts
    #     "n_results": 50
    # },
    {
        "name": "CreditCardsIndia",
        "category": "t",  # top posts
        "n_results": 25
    }
]


def get_r2_client():
    """Initialize and return R2 client"""
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )


def get_latest_scrape_file(subreddit, category, n_results):
    """Get the most recent scrape file for a subreddit"""
    today = datetime.now().strftime("%Y-%m-%d")
    scrapes_dir = Path(f"scrapes/{today}/subreddits")

    logger.info(f"Looking for scrape file in: {scrapes_dir}")
    if not scrapes_dir.exists():
        return None

    # Category mapping
    category_mapping = {
        "n": "new",
        "h": "hot",
        "t": "top",
        "r": "rising",
        "c": "controversial"
    }

    file_category = category_mapping.get(category, category)
    expected_filename = f"{subreddit}-{file_category}-{n_results}-results.json"
    logger.info(f"Expected file: {expected_filename}")
    file_path = scrapes_dir / expected_filename

    # Try up to 5 times with a 2 second delay between attempts
    for attempt in range(5):
        logger.info(f"Checking file (attempt {attempt + 1}/5): {file_path}")
        if file_path.exists():
            return file_path
        time.sleep(2)

    return None


def extract_submission_urls(json_file):
    """Extract all submission URLs from a subreddit scrape JSON file"""
    with open(json_file) as f:
        json_data = json.load(f)

    urls = []
    for post in json_data["data"]:
        url = f"https://www.reddit.com{post['permalink']}"
        urls.append(url)
    return urls


def scrape_subreddit(subreddit, category, n_results):
    """Scrape a subreddit using URS"""
    logger.info(
        f"Scraping r/{subreddit} with category {category} and {n_results} results")

    original_dir = os.getcwd()

    try:
        os.chdir('urs')
        cmd = f"poetry run python Urs.py -r {subreddit} {category} {n_results}"

        try:
            process = pexpect.spawn(cmd, timeout=120, encoding='utf-8')
            process.logfile = sys.stdout

            index = process.expect(
                ['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
            if index == 0:
                process.sendline("y")
                process.expect(pexpect.EOF, timeout=120)
            elif index == 1:
                logger.info("Process ended before prompt")
            else:
                logger.warning("Timed out waiting for prompt")

        except Exception as e:
            logger.error(f"Error running command: {e}")
            if process.isalive():
                process.terminate()
            return None
        finally:
            os.chdir(original_dir)

        return get_latest_scrape_file(subreddit, category, n_results)

    except Exception as e:
        logger.error(f"Error changing directory: {e}")
        os.chdir(original_dir)
        return None


def scrape_comments(url, n_comments=0):
    """Scrape comments from a submission using URS"""
    logger.info(f"Scraping comments from URL: {url}")

    original_dir = os.getcwd()

    try:
        os.chdir('urs')
        cmd = f"poetry run python Urs.py -c {url} {n_comments}"

        try:
            process = pexpect.spawn(cmd, timeout=120, encoding='utf-8')
            process.logfile = sys.stdout

            index = process.expect(
                ['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
            if index == 0:
                process.sendline("y")
                process.expect(pexpect.EOF, timeout=120)
            elif index == 1:
                logger.info("Process ended before prompt")
            else:
                logger.warning("Timed out waiting for prompt")

        except Exception as e:
            logger.error(f"Error running command: {e}")
            if process.isalive():
                process.terminate()
        finally:
            os.chdir(original_dir)

    except Exception as e:
        logger.error(f"Error changing directory: {e}")
        os.chdir(original_dir)


def create_archive(scrapes_dir):
    """Create a zip archive of all scraped data"""
    today = datetime.now().strftime("%Y-%m-%d")
    archive_name = f"reddit_scrapes_{today}.zip"
    archive_path = Path(archive_name)

    logger.info(f"Creating archive: {archive_path}")

    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in scrapes_dir.rglob('*'):
            if file_path.is_file():
                # Add file to zip with relative path
                arcname = file_path.relative_to(scrapes_dir.parent)
                zipf.write(file_path, arcname)
                logger.info(f"Added to archive: {arcname}")

    return archive_path


def upload_to_r2(file_path, object_key, config={}):
    """Upload file to R2 bucket with additional metadata"""
    try:
        r2_client = get_r2_client()

        logger.info(
            f"Uploading {file_path} to R2 bucket {R2_BUCKET_NAME} as {object_key}")

        # Build metadata dictionary
        metadata = {
            'upload_date': datetime.now().isoformat(),
            'source': 'automated_reddit_scraper',
            **config
        }

        with open(file_path, 'rb') as f:
            r2_client.upload_fileobj(
                f,
                R2_BUCKET_NAME,
                object_key,
                ExtraArgs={
                    'Metadata': metadata
                }
            )

        logger.info(
            f"Successfully uploaded {object_key} to R2 with metadata: {metadata}")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to R2: {e}")
        return False


@app.task(bind=True, max_retries=3)
def scrape_and_upload_to_r2(self):
    """Main Celery task to scrape Reddit data and upload to R2"""
    try:
        logger.info("Starting daily Reddit scraping task")

        # Validate R2 configuration
        if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
            raise ValueError(
                "R2 configuration is incomplete. Please check environment variables.")

        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")

        # Process each subreddit configuration
        for config in SUBREDDIT_CONFIGS:
            subreddit = config["name"]
            category = config["category"]
            n_results = config["n_results"]

            logger.info(
                f"Processing r/{subreddit} (Category: {category}, Results: {n_results})")

            # 1. Scrape the subreddit
            scrape_file = scrape_subreddit(subreddit, category, n_results)

            if not scrape_file:
                logger.error(
                    f"Could not find scrape file for r/{subreddit}. Skipping.")
                continue

            # 2. Extract submission URLs
            try:
                urls = extract_submission_urls(scrape_file)
                logger.info(f"Found {len(urls)} submissions in r/{subreddit}")
            except Exception as e:
                logger.error(f"Error extracting URLs from {scrape_file}: {e}")
                continue

            # 3. Scrape comments for each submission
            logger.info(f"Scraping comments from {len(urls)} submissions")
            for i, url in enumerate(urls, 1):
                logger.info(
                    f"Scraping comments from submission {i}/{len(urls)}")
                scrape_comments(url, 0)  # 0 means scrape all comments

                # Add random delay between comment scraping to avoid overwhelming the API
                if i < len(urls):  # Don't delay after the last submission
                    # Random delay between 3-8 seconds
                    delay = random.uniform(3, 8)
                    logger.info(
                        f"Waiting {delay:.1f} seconds before next submission...")
                    time.sleep(delay)

        # 4. Create archive of all scraped data
        if scrapes_dir.exists():
            archive_path = create_archive(scrapes_dir)

            # 5. Upload to R2
            object_key = f"daily_scrapes/{today}/reddit_scrapes_{today}.zip"
            upload_success = upload_to_r2(file_path=archive_path, object_key=object_key, config={
                'subreddit': subreddit,
                'category': category,
                'n_results': n_results
            })

            if upload_success:
                logger.info("Successfully completed daily scraping and upload")

                # Clean up local archive file
                archive_path.unlink()
                logger.info("Cleaned up local archive file")

                return {
                    "status": "success",
                    "date": today,
                    "archive_uploaded": object_key,
                    "subreddits_processed": len(SUBREDDIT_CONFIGS)
                }
            else:
                raise Exception("Failed to upload archive to R2")
        else:
            raise Exception(f"Scrapes directory {scrapes_dir} does not exist")

    except Exception as e:
        logger.error(f"Task failed: {e}")

        # Retry logic
        if self.request.retries < self.max_retries:
            logger.info(
                f"Retrying task (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(countdown=300, exc=e)  # Retry after 5 minutes
        else:
            logger.error("Max retries exceeded. Task failed permanently.")
            return {
                "status": "failed",
                "error": str(e),
                "date": datetime.now().strftime("%Y-%m-%d")
            }

# Manual trigger task for testing


@app.task
def test_scrape_task():
    """Test task to manually trigger scraping"""
    logger.info("Running test scrape task")
    # Call the main task directly instead of using delay()
    result = scrape_and_upload_to_r2()
    logger.info(f"Test scrape task completed with result: {result}")
    return result
