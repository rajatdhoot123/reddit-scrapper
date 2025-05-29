#!/usr/bin/env python3

from celery_config import app
import os
import json
import boto3
import zipfile
import shutil
import hashlib
from datetime import datetime
from pathlib import Path
from celery import current_app
from celery.utils.log import get_task_logger
from dotenv import load_dotenv
import subprocess
import time
import pexpect
import sys
import random
from typing import Dict, List, Optional, Union
from argparse import Namespace

# Load environment variables
load_dotenv()

# Import configurations
from subreddit_config import (
    SUBREDDIT_CONFIGS, 
    MANUAL_SUBREDDIT_CONFIGS, 
    COMMENT_SCRAPING_CONFIG,
    ARCHIVE_CONFIG,
    TASK_CONFIG,
    GLOBAL_SCRAPING_CONFIG,
    get_enabled_scheduled_configs
)

# Import URS utilities for filename generation
try:
    from urs.utils.Export import NameFile
    from urs.utils.Global import short_cat
    URS_UTILS_AVAILABLE = True
except ImportError as e:
    URS_UTILS_AVAILABLE = False
    print(f"URS utilities not available: {e}")

# Import database integration
try:
    from database_integration import (
        get_database_processor, save_scraping_results_to_db,
        ScrapingDataProcessor
    )
    DATABASE_INTEGRATION_AVAILABLE = True
    logger_db = get_task_logger("database_integration")
except ImportError as e:
    DATABASE_INTEGRATION_AVAILABLE = False
    logger_db = None
    print(f"Database integration not available: {e}")

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
                          time_filter: Optional[str] = None, use_csv: bool = False, rules: bool = False) -> Optional[Path]:
    """Get the most recent scrape file for a subreddit using Export.py filename logic"""
    today = datetime.now().strftime("%Y-%m-%d")
    scrapes_dir = Path(f"scrapes/{today}/subreddits")

    logger.info(f"Looking for scrape file in: {scrapes_dir}")
    if not scrapes_dir.exists():
        return None

    if not URS_UTILS_AVAILABLE:
        logger.error("URS utilities not available, cannot generate filename")
        return None

    try:
        # Create a NameFile instance for filename generation
        name_file = NameFile()
        
        # Create a mock args object for the Export.py interface
        args = Namespace()
        args.rules = rules
        
        # Prepare each_sub format expected by Export.py: [subreddit, n_results_or_keywords, time_filter]
        each_sub = [subreddit, n_results_or_keywords, time_filter]
        
        # Generate filename using Export.py logic
        expected_filename = name_file.r_fname(args, category, each_sub, subreddit)
        
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
        
    except Exception as e:
        logger.error(f"Error using Export.py filename generation: {e}")
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
    
    # Convert category to uppercase for URS compatibility
    category_upper = category.upper()
    category_lower = category.lower()
    
    # Build command description for logging
    if category_lower == "s":
        logger.info(f"Searching r/{subreddit} for '{n_results_or_keywords}' with time filter: {time_filter}")
    else:
        logger.info(f"Scraping r/{subreddit} with category {category_upper}, {n_results_or_keywords} results, time filter: {time_filter}")

    original_dir = os.getcwd()

    try:
        os.chdir('urs')
        
        # Build the base command - use uppercase category for URS
        cmd_parts = ["poetry", "run", "python", "Urs.py", "-r", subreddit, category_upper]
        
        # Add n_results or keywords
        cmd_parts.append(str(n_results_or_keywords))
        
        # Add time filter if specified (for top, controversial categories)
        if time_filter and category_lower in ["t", "c"]:
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
            subreddit, category_upper, n_results_or_keywords, 
            time_filter, options.get("csv", False), options.get("rules", False)
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

    # 4. Save to database if integration is available
    result = {
        "status": "success",
        "subreddit": subreddit,
        "category": category,
        "submissions_found": len(urls),
        "comments_scraped": comments_scraped,
        "scrape_file": str(scrape_file)
    }
    
    if DATABASE_INTEGRATION_AVAILABLE:
        try:
            db_processor = get_database_processor()
            if db_processor:
                # Generate a task ID for this processing session
                task_id = f"process_{subreddit}_{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                save_scraping_results_to_db(
                    processor=db_processor,
                    task_id=task_id,
                    task_type="scheduled",  # This will be overridden in the calling function if needed
                    config=config,
                    result=result,
                    scrape_file=scrape_file
                )
                logger.info(f"Saved scraping data to database for r/{subreddit}")
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            # Don't fail the entire process if database save fails
    
    return result


def generate_unique_object_key(configs_to_process: List[Dict], scrape_type: str, 
                              today: str, timestamp: str, results: List[Dict] = None) -> str:
    """Generate a unique object key for R2 upload that differentiates between configurations"""
    
    if len(configs_to_process) == 1:
        # Single configuration - create detailed path
        config = configs_to_process[0]
        subreddit = config["name"]
        category = config["category"]
        
        # Category mapping for clarity
        category_names = {
            "h": "hot",
            "n": "new", 
            "t": "top",
            "r": "rising",
            "c": "controversial",
            "s": "search"
        }
        category_name = category_names.get(category, category)
        
        # Build path components
        path_parts = [subreddit, category_name]
        
        # Add time filter if present
        if config.get("time_filter"):
            path_parts.append(config["time_filter"])
        
        # Add n_results or keywords
        if category == "s" and config.get("keywords"):
            # For search, truncate long keywords and add hash if needed
            keywords = str(config["keywords"])
            if len(keywords) > 30:
                keywords_hash = hashlib.md5(keywords.encode()).hexdigest()[:8]
                path_parts.append(f"search_{keywords_hash}")
            else:
                safe_keywords = keywords.replace(" ", "_").replace("/", "_")
                path_parts.append(f"search_{safe_keywords}")
        elif config.get("n_results"):
            path_parts.append(f"{config['n_results']}results")
        
        # Join with underscores for the config part
        config_path = "_".join(path_parts)
        
        return f"{scrape_type}_scrapes/{subreddit}/{config_path}/{today}/{scrape_type}_{config_path}_{timestamp}.zip"
    
    else:
        # Multiple configurations - use combined approach
        if results:
            subreddit_names = [r["subreddit"] for r in results if r["status"] == "success"]
        else:
            subreddit_names = [config["name"] for config in configs_to_process]
        
        unique_subreddits = sorted(set(subreddit_names))
        
        if len(unique_subreddits) == 1:
            subreddit_path = unique_subreddits[0]
        else:
            subreddit_path = "_".join(unique_subreddits)
            if len(subreddit_path) > 100:
                subreddit_hash = hashlib.md5("_".join(unique_subreddits).encode()).hexdigest()[:8]
                subreddit_path = f"multi_subreddits_{subreddit_hash}"
        
        # Create a hash of all configs to ensure uniqueness
        config_signature = hashlib.md5(
            str(sorted([(c.get("name"), c.get("category"), c.get("time_filter"), 
                        c.get("n_results"), c.get("keywords")) for c in configs_to_process])).encode()
        ).hexdigest()[:8]
        
        return f"{scrape_type}_scrapes/{subreddit_path}/multi_config_{config_signature}/{today}/{scrape_type}_multi_{config_signature}_{timestamp}.zip"


@app.task(bind=True, max_retries=None)
def scheduled_scrape_task(self, config_id: int = None):
    """Enhanced scheduled scraping task for individual subreddit configurations"""
    try:
        # Check global controls first
        if not GLOBAL_SCRAPING_CONFIG.get("master_enabled", True):
            logger.info("Scraping is globally disabled via master_enabled flag")
            return {"status": "skipped", "reason": "globally_disabled"}
        
        if not GLOBAL_SCRAPING_CONFIG.get("scheduled_scraping_enabled", True):
            logger.info("Scheduled scraping is disabled via scheduled_scraping_enabled flag")
            return {"status": "skipped", "reason": "scheduled_scraping_disabled"}
        
        max_retries = TASK_CONFIG.get("max_retries", 3)
        retry_delay = TASK_CONFIG.get("retry_delay", 300)
        
        logger.info(f"Starting scheduled Reddit scraping task for config ID: {config_id}")

        # Validate R2 configuration
        if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
            raise ValueError("R2 configuration is incomplete. Please check environment variables.")

        # Get all enabled scheduled configs
        enabled_configs = get_enabled_scheduled_configs()
        
        if config_id is not None:
            # Process specific config by ID
            if config_id >= len(enabled_configs):
                raise ValueError(f"Invalid config ID: {config_id}")
            configs_to_process = [enabled_configs[config_id]]
        else:
            # Process all enabled configs (for manual runs)
            configs_to_process = enabled_configs

        today = datetime.now().strftime("%Y-%m-%d")
        scrapes_dir = Path(f"scrapes/{today}")

        # Process each subreddit configuration
        results = []
        for config in configs_to_process:
            result = process_subreddit_config(config, scrapes_dir)
            results.append(result)

        # Create archive of all scraped data
        successful_results = [r for r in results if r["status"] == "success"]
        
        if scrapes_dir.exists() and successful_results:
            # Check if archiving is enabled
            if GLOBAL_SCRAPING_CONFIG.get("create_archives_enabled", True):
                # Create archive name based on configs processed
                if len(configs_to_process) == 1:
                    config = configs_to_process[0]
                    archive_name = f"{config['name']}_{config['category']}"
                else:
                    archive_name = "multiple_configs"
                
                archive_path = create_archive(scrapes_dir, archive_type=archive_name)

                # Upload to R2 (if enabled)
                if GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True):
                    # Generate timestamp for unique naming
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                    
                    object_key = generate_unique_object_key(configs_to_process, "scheduled", today, timestamp, results)
                    
                    # Build metadata for upload
                    upload_metadata = {
                        'config_type': 'scheduled',
                        'subreddits': ",".join(set(r["subreddit"] for r in successful_results)),
                        'configs_processed': len(configs_to_process),
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
                logger.info(f"Successfully completed scheduled scraping")

                # Clean up local archive file if it was created and uploaded
                if archive_path and upload_success:
                    # Save archive info to database before cleanup
                    if DATABASE_INTEGRATION_AVAILABLE and object_key:
                        try:
                            db_processor = get_database_processor()
                            if db_processor:
                                db_processor.create_archive_record(
                                    archive_path=archive_path,
                                    archive_type="scheduled",
                                    r2_object_key=object_key,
                                    metadata=upload_metadata
                                )
                                logger.info("Saved archive information to database")
                        except Exception as e:
                            logger.error(f"Failed to save archive info to database: {e}")
                    
                    archive_path.unlink()
                    logger.info("Cleaned up local archive file")

                return {
                    "status": "success",
                    "config_id": config_id,
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
            logger.info(f"No successful scrapes or scrapes directory doesn't exist")
            return {
                "status": "completed_no_data",
                "config_id": config_id,
                "date": today,
                "results": results,
                "message": "No successful scrapes to process"
            }

    except Exception as exc:
        logger.error(f"Scheduled scraping task failed: {exc}")
        
        # Handle retries
        if self.request.retries < max_retries:
            logger.info(f"Retrying in {retry_delay} seconds (attempt {self.request.retries + 1}/{max_retries})")
            raise self.retry(countdown=retry_delay, exc=exc)
        else:
            logger.error(f"Scheduled scraping task failed after {max_retries} retries")
            return {
                "status": "failed",
                "config_id": config_id,
                "error": str(exc),
                "retries_exhausted": True
            }


# Legacy task functions for backward compatibility with celery beat
@app.task(bind=True, max_retries=None)
def daily_scrape_task(self):
    """Legacy task - runs all daily configs for backward compatibility"""
    enabled_configs = get_enabled_scheduled_configs()
    daily_configs = []
    
    # Find configs that look like daily schedules (run every day)
    for i, config in enumerate(enabled_configs):
        schedule = config.get('schedule')
        if hasattr(schedule, 'hour') and hasattr(schedule, 'minute'):
            # This is a crontab that runs daily if no day restrictions
            if not hasattr(schedule, 'day_of_week') or schedule.day_of_week is None:
                if not hasattr(schedule, 'day_of_month') or schedule.day_of_month is None:
                    daily_configs.append(i)
    
    if daily_configs:
        # Run the first daily config found
        return scheduled_scrape_task.apply_async(args=[daily_configs[0]]).get()
    return {"status": "no_daily_configs_found"}


@app.task(bind=True, max_retries=None)
def weekly_scrape_task(self):
    """Legacy task - runs all weekly configs for backward compatibility"""
    enabled_configs = get_enabled_scheduled_configs()
    weekly_configs = []
    
    # Find configs that have day_of_week set (weekly schedules)
    for i, config in enumerate(enabled_configs):
        schedule = config.get('schedule')
        if hasattr(schedule, 'day_of_week') and schedule.day_of_week is not None:
            weekly_configs.append(i)
    
    if weekly_configs:
        # Run the first weekly config found
        return scheduled_scrape_task.apply_async(args=[weekly_configs[0]]).get()
    return {"status": "no_weekly_configs_found"}


@app.task(bind=True, max_retries=None)
def hourly_hot_scrape_task(self):
    """Legacy task - runs all hourly configs for backward compatibility"""
    enabled_configs = get_enabled_scheduled_configs()
    hourly_configs = []
    
    # Find configs that run every hour (minute=0, no hour specified or hour='*')
    for i, config in enumerate(enabled_configs):
        schedule = config.get('schedule')
        if hasattr(schedule, 'minute') and schedule.minute == 0:
            if not hasattr(schedule, 'hour') or schedule.hour == '*' or schedule.hour is None:
                hourly_configs.append(i)
    
    if hourly_configs:
        # Run the first hourly config found
        return scheduled_scrape_task.apply_async(args=[hourly_configs[0]]).get()
    return {"status": "no_hourly_configs_found"}


@app.task(bind=True, max_retries=None)
def custom_interval_scrape_task(self):
    """Legacy task - runs configs with timedelta schedules for backward compatibility"""
    enabled_configs = get_enabled_scheduled_configs()
    interval_configs = []
    
    # Find configs that use timedelta schedules
    for i, config in enumerate(enabled_configs):
        schedule = config.get('schedule')
        if hasattr(schedule, 'total_seconds'):  # This is a timedelta
            interval_configs.append(i)
    
    if interval_configs:
        # Run the first interval config found
        return scheduled_scrape_task.apply_async(args=[interval_configs[0]]).get()
    return {"status": "no_interval_configs_found"}


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
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        object_key = generate_unique_object_key(enabled_configs, "manual", today, timestamp, results)
        upload_metadata = {
            'scrape_type': 'manual',
            'subreddits': ",".join(r["subreddit"] for r in successful_scrapes),  # Add subreddit list to metadata
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
    
    # Get active tasks
    inspect = current_app.control.inspect()
    active_tasks = inspect.active()
    scheduled_tasks = inspect.scheduled()
    
    # Get enabled configs for status reporting
    enabled_configs = get_enabled_scheduled_configs()
    
    status = {
        "timestamp": datetime.now().isoformat(),
        "total_configs": len(SUBREDDIT_CONFIGS),
        "enabled_configs": len(enabled_configs),
        "subreddits": list(set(config['name'] for config in enabled_configs)),
        "active_tasks": active_tasks,
        "scheduled_tasks": scheduled_tasks,
        "configurations": {
            "comment_scraping": COMMENT_SCRAPING_CONFIG,
            "archive_config": ARCHIVE_CONFIG,
            "task_config": TASK_CONFIG,
            "global_config": GLOBAL_SCRAPING_CONFIG
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
