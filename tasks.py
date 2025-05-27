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
from typing import Dict, List, Optional, Union

# Load environment variables
load_dotenv()

# Import configurations
from subreddit_config import (
    SUBREDDIT_SCHEDULES, 
    MANUAL_SUBREDDIT_CONFIGS, 
    COMMENT_SCRAPING_CONFIG,
    ARCHIVE_CONFIG,
    TASK_CONFIG,
    GLOBAL_SCRAPING_CONFIG
)

# Set up logging
logger = get_task_logger(__name__)

# R2/S3 Configuration
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME', 'creditcardsindia')

# Legacy configuration for backward compatibility
SUBREDDIT_CONFIGS = [
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


def get_latest_scrape_file(subreddit: str, category: str, n_results_or_keywords: Union[int, str], 
                          time_filter: Optional[str] = None, use_csv: bool = False) -> Optional[Path]:
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
        "c": "controversial",
        "s": "search"
    }

    file_category = category_mapping.get(category, category)
    
    # Handle search category differently
    if category == "s":
        # For search, keywords are used instead of n_results
        expected_filename = f"{subreddit}-{file_category}-{n_results_or_keywords}"
        if time_filter:
            expected_filename += f"-{time_filter}"
    else:
        expected_filename = f"{subreddit}-{file_category}-{n_results_or_keywords}-results"
        if time_filter:
            expected_filename += f"-{time_filter}"
    
    # Add file extension
    file_extension = ".csv" if use_csv else ".json"
    expected_filename += file_extension
    
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


def scrape_subreddit(subreddit: str, category: str, n_results_or_keywords: Union[int, str], 
                    time_filter: Optional[str] = None, options: Optional[Dict] = None) -> Optional[Path]:
    """Scrape a subreddit using URS with enhanced options"""
    if options is None:
        options = {}
    
    # Build command description for logging
    if category == "s":
        logger.info(f"Searching r/{subreddit} for '{n_results_or_keywords}' with time filter: {time_filter}")
    else:
        logger.info(f"Scraping r/{subreddit} with category {category}, {n_results_or_keywords} results, time filter: {time_filter}")

    original_dir = os.getcwd()

    try:
        os.chdir('urs')
        
        # Build the base command
        cmd_parts = ["poetry", "run", "python", "Urs.py", "-r", subreddit, category]
        
        # Add n_results or keywords
        cmd_parts.append(str(n_results_or_keywords))
        
        # Add time filter if specified (for top, controversial categories)
        if time_filter and category in ["t", "c"]:
            cmd_parts.append(time_filter)
        
        # Add optional flags
        if options.get("csv", False):
            cmd_parts.append("--csv")
        
        if options.get("rules", False):
            cmd_parts.append("--rules")
        
        # Auto-confirm flag
        if options.get("auto_confirm", True):
            cmd_parts.append("-y")
        
        cmd = " ".join(cmd_parts)
        logger.info(f"Executing command: {cmd}")

        try:
            timeout = options.get("timeout", TASK_CONFIG.get("timeout", 300))
            process = pexpect.spawn(cmd, timeout=timeout, encoding='utf-8')
            process.logfile = sys.stdout

            # If auto_confirm is False, handle the Y/N prompt
            if not options.get("auto_confirm", True):
                index = process.expect(
                    ['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
                if index == 0:
                    process.sendline("y")
                    process.expect(pexpect.EOF, timeout=timeout)
                elif index == 1:
                    logger.info("Process ended before prompt")
                else:
                    logger.warning("Timed out waiting for prompt")
            else:
                # With -y flag, just wait for completion
                process.expect(pexpect.EOF, timeout=timeout)

        except Exception as e:
            logger.error(f"Error running command: {e}")
            if 'process' in locals() and process.isalive():
                process.terminate()
            return None
        finally:
            os.chdir(original_dir)

        return get_latest_scrape_file(
            subreddit, category, n_results_or_keywords, 
            time_filter, options.get("csv", False)
        )

    except Exception as e:
        logger.error(f"Error changing directory: {e}")
        os.chdir(original_dir)
        return None


def scrape_comments(url: str, n_comments: int = 0, options: Optional[Dict] = None) -> bool:
    """Scrape comments from a submission using URS"""
    if options is None:
        options = {}
    
    # Apply comment limits from configuration
    max_comments = COMMENT_SCRAPING_CONFIG.get("max_comments_per_post", 500)
    if n_comments == 0:
        n_comments = COMMENT_SCRAPING_CONFIG.get("default_n_comments", 0)
    elif n_comments > max_comments:
        logger.warning(f"Limiting comments to {max_comments} (requested: {n_comments})")
        n_comments = max_comments
    
    logger.info(f"Scraping comments from URL: {url} (limit: {n_comments})")

    original_dir = os.getcwd()

    try:
        os.chdir('urs')
        
        # Build command
        cmd_parts = ["poetry", "run", "python", "Urs.py", "-c", url, str(n_comments)]
        
        # Add optional flags
        if options.get("csv", False):
            cmd_parts.append("--csv")
        
        if options.get("auto_confirm", True):
            cmd_parts.append("-y")
        
        cmd = " ".join(cmd_parts)
        logger.info(f"Executing comment scrape command: {cmd}")

        try:
            timeout = options.get("timeout", TASK_CONFIG.get("timeout", 300))
            process = pexpect.spawn(cmd, timeout=timeout, encoding='utf-8')
            process.logfile = sys.stdout

            # If auto_confirm is False, handle the Y/N prompt
            if not options.get("auto_confirm", True):
                index = process.expect(
                    ['\[Y/N\]', pexpect.EOF, pexpect.TIMEOUT], timeout=60)
                if index == 0:
                    process.sendline("y")
                    process.expect(pexpect.EOF, timeout=timeout)
                elif index == 1:
                    logger.info("Process ended before prompt")
                else:
                    logger.warning("Timed out waiting for prompt")
            else:
                # With -y flag, just wait for completion
                process.expect(pexpect.EOF, timeout=timeout)
            
            return True

        except Exception as e:
            logger.error(f"Error running command: {e}")
            if 'process' in locals() and process.isalive():
                process.terminate()
            return False
        finally:
            os.chdir(original_dir)

    except Exception as e:
        logger.error(f"Error changing directory: {e}")
        os.chdir(original_dir)
        return False


def create_archive(scrapes_dir: Path, archive_type: str = "daily", 
                  custom_name: Optional[str] = None) -> Path:
    """Create a zip archive of all scraped data with enhanced options"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    if custom_name:
        archive_name = f"{custom_name}_{today}.zip"
    else:
        archive_name = f"reddit_scrapes_{archive_type}_{today}.zip"
    
    archive_path = Path(archive_name)
    
    # Get compression level from config
    compression_level = ARCHIVE_CONFIG.get("compress_level", 6)
    
    logger.info(f"Creating {archive_type} archive: {archive_path} (compression level: {compression_level})")

    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=compression_level) as zipf:
        file_count = 0
        total_size = 0
        
        for file_path in scrapes_dir.rglob('*'):
            if file_path.is_file():
                # Add file to zip with relative path
                arcname = file_path.relative_to(scrapes_dir.parent)
                zipf.write(file_path, arcname)
                
                file_size = file_path.stat().st_size
                total_size += file_size
                file_count += 1
                
                logger.info(f"Added to archive: {arcname} ({file_size} bytes)")
        
        # Add metadata file if enabled
        if ARCHIVE_CONFIG.get("include_metadata", True):
            metadata = {
                "created_at": datetime.now().isoformat(),
                "archive_type": archive_type,
                "file_count": file_count,
                "total_size_bytes": total_size,
                "compression_level": compression_level,
                "source_directory": str(scrapes_dir)
            }
            
            metadata_content = json.dumps(metadata, indent=2)
            zipf.writestr("archive_metadata.json", metadata_content)
            logger.info("Added metadata to archive")
    
    archive_size = archive_path.stat().st_size
    compression_ratio = (1 - archive_size / total_size) * 100 if total_size > 0 else 0
    
    logger.info(f"Archive created successfully: {file_count} files, "
               f"original size: {total_size} bytes, "
               f"compressed size: {archive_size} bytes "
               f"(compression: {compression_ratio:.1f}%)")

    return archive_path


def upload_to_r2(file_path, object_key, config={}):
    """Upload file to R2 bucket with additional metadata"""
    try:
        r2_client = get_r2_client()

        logger.info(
            f"Uploading {file_path} to R2 bucket {R2_BUCKET_NAME} as {object_key}")

        # Build metadata dictionary - convert all values to strings for R2 compatibility
        metadata = {
            'upload_date': datetime.now().isoformat(),
            'source': 'automated_reddit_scraper',
        }
        
        # Convert all config values to strings since R2 metadata must be strings
        for key, value in config.items():
            metadata[key] = str(value)

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


def process_subreddit_config(config: Dict, scrapes_dir: Path) -> Dict:
    """Process a single subreddit configuration"""
    # Check if this specific subreddit config is enabled
    if not config.get("enabled", True):
        logger.info(f"Skipping disabled subreddit config: r/{config['name']}")
        return {"status": "skipped", "subreddit": config["name"], "reason": "disabled"}
    
    subreddit = config["name"]
    category = config["category"]
    n_results_or_keywords = config.get("n_results") or config.get("keywords")
    time_filter = config.get("time_filter")
    options = config.get("options", {})

    logger.info(f"Processing r/{subreddit} (Category: {category}, "
               f"Results/Keywords: {n_results_or_keywords}, Time filter: {time_filter})")

    # 1. Scrape the subreddit
    scrape_file = scrape_subreddit(subreddit, category, n_results_or_keywords, time_filter, options)

    if not scrape_file:
        logger.error(f"Could not find scrape file for r/{subreddit}. Skipping.")
        return {"status": "failed", "subreddit": subreddit, "error": "Scrape file not found"}

    # 2. Extract submission URLs (only for JSON files)
    urls = []
    if scrape_file.suffix == '.json':
        try:
            urls = extract_submission_urls(scrape_file)
            logger.info(f"Found {len(urls)} submissions in r/{subreddit}")
        except Exception as e:
            logger.error(f"Error extracting URLs from {scrape_file}: {e}")
            return {"status": "failed", "subreddit": subreddit, "error": f"URL extraction failed: {e}"}

    # 3. Scrape comments for each submission (if enabled and URLs available)
    comments_scraped = 0
    comment_scraping_enabled = (
        GLOBAL_SCRAPING_CONFIG.get("comment_scraping_globally_enabled", True) and
        COMMENT_SCRAPING_CONFIG.get("enable_comment_scraping", True)
    )
    
    if comment_scraping_enabled and urls:
        logger.info(f"Scraping comments from {len(urls)} submissions")
        delay_range = COMMENT_SCRAPING_CONFIG.get("comment_delay_range", (3, 8))
        
        for i, url in enumerate(urls, 1):
            logger.info(f"Scraping comments from submission {i}/{len(urls)}")
            
            if scrape_comments(url, options=options):
                comments_scraped += 1

            # Add random delay between comment scraping
            if i < len(urls):
                delay = random.uniform(*delay_range)
                logger.info(f"Waiting {delay:.1f} seconds before next submission...")
                time.sleep(delay)

    return {
        "status": "success",
        "subreddit": subreddit,
        "category": category,
        "submissions_found": len(urls),
        "comments_scraped": comments_scraped,
        "scrape_file": str(scrape_file)
    }


@app.task(bind=True, max_retries=None)
def scheduled_scrape_task(self, schedule_name: str):
    """Enhanced scheduled scraping task for different subreddit schedules"""
    try:
        # Check global controls first
        if not GLOBAL_SCRAPING_CONFIG.get("master_enabled", True):
            logger.info("Scraping is globally disabled via master_enabled flag")
            return {"status": "skipped", "reason": "globally_disabled", "schedule_name": schedule_name}
        
        if not GLOBAL_SCRAPING_CONFIG.get("scheduled_scraping_enabled", True):
            logger.info("Scheduled scraping is disabled via scheduled_scraping_enabled flag")
            return {"status": "skipped", "reason": "scheduled_scraping_disabled", "schedule_name": schedule_name}
        
        max_retries = TASK_CONFIG.get("max_retries", 3)
        retry_delay = TASK_CONFIG.get("retry_delay", 300)
        
        logger.info(f"Starting scheduled Reddit scraping task: {schedule_name}")

        # Validate R2 configuration
        if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
            raise ValueError("R2 configuration is incomplete. Please check environment variables.")

        # Get schedule configuration
        if schedule_name not in SUBREDDIT_SCHEDULES:
            raise ValueError(f"Unknown schedule: {schedule_name}")
        
        schedule_config = SUBREDDIT_SCHEDULES[schedule_name]
        
        # Check if this specific schedule is enabled
        if not schedule_config.get("enabled", True):
            logger.info(f"Schedule '{schedule_name}' is disabled")
            return {"status": "skipped", "reason": "schedule_disabled", "schedule_name": schedule_name}
        
        subreddit_configs = schedule_config["subreddits"]

        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")

        # Process each subreddit configuration
        results = []
        for config in subreddit_configs:
            result = process_subreddit_config(config, scrapes_dir)
            results.append(result)

        # 4. Create archive of all scraped data
        successful_results = [r for r in results if r["status"] == "success"]
        
        if scrapes_dir.exists() and successful_results:
            # Check if archiving is enabled
            if GLOBAL_SCRAPING_CONFIG.get("create_archives_enabled", True):
                archive_path = create_archive(scrapes_dir, archive_type=schedule_name)

                # 5. Upload to R2 (if enabled)
                if GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True):
                    object_key = f"{schedule_name}_scrapes/{today}/reddit_scrapes_{schedule_name}_{today}.zip"
                    
                    # Build metadata for upload
                    upload_metadata = {
                        'schedule_name': schedule_name,
                        'subreddits_processed': len(subreddit_configs),
                        'successful_scrapes': len(successful_results),
                        'skipped_scrapes': len([r for r in results if r["status"] == "skipped"]),
                        'total_submissions': sum(r.get("submissions_found", 0) for r in results),
                        'total_comments_scraped': sum(r.get("comments_scraped", 0) for r in results)
                    }
                    
                    upload_success = upload_to_r2(file_path=archive_path, object_key=object_key, config=upload_metadata)
                else:
                    upload_success = False
                    object_key = None
                    logger.info("R2 upload is disabled via configuration")
            else:
                archive_path = None
                upload_success = False
                object_key = None
                logger.info("Archive creation is disabled via configuration")

            if upload_success or not GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True):
                logger.info(f"Successfully completed {schedule_name} scraping")

                # Clean up local archive file if it was created and uploaded
                if archive_path and upload_success:
                    archive_path.unlink()
                    logger.info("Cleaned up local archive file")

                return {
                    "status": "success",
                    "schedule_name": schedule_name,
                    "date": today,
                    "archive_uploaded": object_key,
                    "archive_created": archive_path is not None,
                    "upload_enabled": GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True),
                    "results": results,
                    "successful_scrapes": len(successful_results),
                    "skipped_scrapes": len([r for r in results if r["status"] == "skipped"]),
                    "total_submissions": sum(r.get("submissions_found", 0) for r in results),
                    "total_comments_scraped": sum(r.get("comments_scraped", 0) for r in results)
                }
            else:
                raise Exception("Failed to upload archive to R2")
        else:
            logger.info(f"No successful scrapes or scrapes directory doesn't exist for {schedule_name}")
            return {
                "status": "completed_no_data",
                "schedule_name": schedule_name,
                "date": today,
                "results": results,
                "message": "No successful scrapes to process"
            }

    except Exception as e:
        logger.error(f"Scheduled task {schedule_name} failed: {e}")

        # Retry logic
        if self.request.retries < max_retries:
            logger.info(f"Retrying task {schedule_name} (attempt {self.request.retries + 1}/{max_retries})")
            raise self.retry(countdown=retry_delay, exc=e)
        else:
            logger.error(f"Max retries exceeded for {schedule_name}. Task failed permanently.")
            return {
                "status": "failed",
                "schedule_name": schedule_name,
                "error": str(e),
                "date": datetime.now().strftime("%Y-%m-%d")
            }


@app.task(bind=True, max_retries=3)
def scrape_and_upload_to_r2(self):
    """Legacy task for backward compatibility - now calls daily_scrapes"""
    return scheduled_scrape_task.apply_async(args=["daily_scrapes"]).get()

# Individual scheduled tasks
@app.task(bind=True)
def daily_scrape_task(self):
    """Daily scraping task"""
    return scheduled_scrape_task.apply_async(args=["daily_scrapes"]).get()


@app.task(bind=True)
def weekly_scrape_task(self):
    """Weekly comprehensive scraping task"""
    return scheduled_scrape_task.apply_async(args=["weekly_scrapes"]).get()


@app.task(bind=True)
def hourly_hot_scrape_task(self):
    """Hourly hot posts scraping task"""
    return scheduled_scrape_task.apply_async(args=["hourly_hot_scrapes"]).get()


@app.task(bind=True)
def custom_interval_scrape_task(self):
    """Custom interval scraping task"""
    return scheduled_scrape_task.apply_async(args=["custom_interval_scrapes"]).get()


# Manual scraping tasks
@app.task(bind=True, max_retries=2)
def manual_scrape_subreddit(self, subreddit: str, category: str, n_results_or_keywords: Union[int, str],
                           time_filter: Optional[str] = None, options: Optional[Dict] = None,
                           scrape_comments: bool = True):
    """Manual task to scrape a specific subreddit with custom parameters"""
    try:
        # Check global controls first
        if not GLOBAL_SCRAPING_CONFIG.get("master_enabled", True):
            logger.info("Manual scraping is globally disabled via master_enabled flag")
            return {"status": "skipped", "reason": "globally_disabled", "subreddit": subreddit}
        
        if not GLOBAL_SCRAPING_CONFIG.get("manual_scraping_enabled", True):
            logger.info("Manual scraping is disabled via manual_scraping_enabled flag")
            return {"status": "skipped", "reason": "manual_scraping_disabled", "subreddit": subreddit}
        
        if options is None:
            options = {"csv": True, "auto_confirm": True}
        
        logger.info(f"Manual scraping: r/{subreddit}, category: {category}, "
                   f"results/keywords: {n_results_or_keywords}, time_filter: {time_filter}")

        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")

        # Create a temporary config for processing
        config = {
            "name": subreddit,
            "category": category,
            "options": options
        }
        
        if category == "s":
            config["keywords"] = n_results_or_keywords
        else:
            config["n_results"] = n_results_or_keywords
            
        if time_filter:
            config["time_filter"] = time_filter

        # Process the subreddit
        result = process_subreddit_config(config, scrapes_dir)
        
        if result["status"] == "success":
            logger.info(f"Manual scraping completed successfully for r/{subreddit}")
            return result
        else:
            raise Exception(f"Manual scraping failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        logger.error(f"Manual scraping task failed: {e}")
        
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying manual scrape (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(countdown=60, exc=e)
        else:
            return {
                "status": "failed",
                "subreddit": subreddit,
                "error": str(e),
                "date": datetime.now().strftime("%Y-%m-%d")
            }


@app.task
def manual_scrape_from_config():
    """Manual task to scrape using predefined manual configurations"""
    # Check global controls first
    if not GLOBAL_SCRAPING_CONFIG.get("master_enabled", True):
        logger.info("Manual scraping is globally disabled via master_enabled flag")
        return {"status": "skipped", "reason": "globally_disabled"}
    
    if not GLOBAL_SCRAPING_CONFIG.get("manual_scraping_enabled", True):
        logger.info("Manual scraping is disabled via manual_scraping_enabled flag")
        return {"status": "skipped", "reason": "manual_scraping_disabled"}
    
    logger.info("Running manual scrape from predefined configurations")
    
    today = datetime.now().strftime("%Y-%m-%d")
    scrapes_dir = Path(f"scrapes/{today}")
    
    # Filter enabled configurations
    enabled_configs = [config for config in MANUAL_SUBREDDIT_CONFIGS if config.get("enabled", True)]
    
    if not enabled_configs:
        logger.info("No enabled manual configurations found")
        return {"status": "skipped", "reason": "no_enabled_configs", "date": today}
    
    logger.info(f"Processing {len(enabled_configs)} enabled configurations out of {len(MANUAL_SUBREDDIT_CONFIGS)} total")
    
    results = []
    for config in enabled_configs:
        result = process_subreddit_config(config, scrapes_dir)
        results.append(result)
    
    # Create archive if any scrapes were successful
    successful_scrapes = [r for r in results if r["status"] == "success"]
    if successful_scrapes and scrapes_dir.exists():
        archive_path = create_archive(scrapes_dir, archive_type="manual", custom_name="manual_scrapes")
        
        # Upload to R2
        object_key = f"manual_scrapes/{today}/manual_scrapes_{today}.zip"
        upload_metadata = {
            'scrape_type': 'manual',
            'subreddits_processed': len(MANUAL_SUBREDDIT_CONFIGS),
            'successful_scrapes': len(successful_scrapes)
        }
        
        upload_success = upload_to_r2(file_path=archive_path, object_key=object_key, config=upload_metadata)
        
        if upload_success:
            archive_path.unlink()  # Clean up
            logger.info("Manual scraping completed and uploaded successfully")
        
        return {
            "status": "success",
            "date": today,
            "results": results,
            "archive_uploaded": object_key if upload_success else None
        }
    
    return {
        "status": "completed",
        "date": today,
        "results": results,
        "message": "No successful scrapes to archive"
    }


# Utility tasks
@app.task
def get_scraping_status():
    """Get the current status of all scraping schedules"""
    from celery import current_app
    
    # Get active tasks
    inspect = current_app.control.inspect()
    active_tasks = inspect.active()
    scheduled_tasks = inspect.scheduled()
    
    status = {
        "timestamp": datetime.now().isoformat(),
        "available_schedules": list(SUBREDDIT_SCHEDULES.keys()),
        "active_tasks": active_tasks,
        "scheduled_tasks": scheduled_tasks,
        "configurations": {
            "comment_scraping": COMMENT_SCRAPING_CONFIG,
            "archive_config": ARCHIVE_CONFIG,
            "task_config": TASK_CONFIG
        }
    }
    
    return status


# Manual trigger task for testing
@app.task
def test_scrape_task():
    """Test task to manually trigger scraping"""
    logger.info("Running test scrape task")
    # Call the daily scrape task for testing
    result = daily_scrape_task.apply_async().get()
    logger.info(f"Test scrape task completed with result: {result}")
    return result
