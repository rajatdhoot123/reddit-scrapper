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

# Helper function to make config objects JSON-serializable
def make_config_serializable(config: Dict) -> Dict:
    """
    Create a JSON-serializable copy of a config object by removing fields that contain
    non-serializable objects like crontab instances.
    """
    serializable_config = {}
    for key, value in config.items():
        # Skip schedule field as it contains crontab objects which are not JSON serializable
        if key == 'schedule':
            continue
        # Skip any other potentially problematic fields
        elif hasattr(value, '__dict__') and not isinstance(value, (str, int, float, bool, list, dict, type(None))):
            # Skip complex objects that might not be serializable
            continue
        else:
            serializable_config[key] = value
    return serializable_config

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
                  custom_name: Optional[str] = None, configs_processed: List[Dict] = None,
                  timestamp: Optional[str] = None) -> Path:
    """Create a zip archive of all scraped data with unified naming"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    if custom_name:
        # Use custom name if provided
        archive_name = f"{custom_name}_{today}.zip"
    elif configs_processed:
        # Use unified naming system if configs are provided
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        archive_name = generate_unified_filename(configs_processed, archive_type, today, timestamp)
    else:
        # Require configs_processed for proper naming
        raise ValueError("Either custom_name or configs_processed must be provided for proper archive naming")
    
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
                "source_directory": str(scrapes_dir),
                "unified_naming": True  # Flag to indicate this uses the new naming system
            }
            
            # Add config info if available
            if configs_processed:
                metadata["configs_processed"] = len(configs_processed)
                metadata["subreddits"] = [config.get("name") for config in configs_processed]
            
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
    """Process a single subreddit configuration - SCRAPING ONLY"""
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

    # Return result with all necessary information for separate database processing
    # Create a serializable copy of config (exclude schedule field which contains crontab objects)
    config_serializable = make_config_serializable(config)
    
    result = {
        "status": "success",
        "subreddit": subreddit,
        "category": category,
        "submissions_found": len(urls),
        "comments_scraped": comments_scraped,
        "scrape_file": str(scrape_file),
        "config_used": config_serializable  # Include the config for database processing (without schedule)
    }
    
    logger.info(f"Scraping completed for r/{subreddit}: {len(urls)} submissions, {comments_scraped} comments")
    return result


def generate_unified_filename(configs_to_process: List[Dict], scrape_type: str, 
                            today: str, timestamp: str, results: List[Dict] = None,
                            include_extension: bool = True) -> str:
    """
    Generate a unified filename for both local archives and R2 object keys.
    This ensures consistent naming between database records and R2 storage.
    """
    
    if len(configs_to_process) == 1:
        # Single configuration - create detailed filename
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
        
        # Build filename components
        filename_parts = [scrape_type, subreddit, category_name]
        
        # Add time filter if present
        if config.get("time_filter"):
            filename_parts.append(config["time_filter"])
        
        # Add n_results or keywords
        if category == "s" and config.get("keywords"):
            # For search, truncate long keywords and add hash if needed
            keywords = str(config["keywords"])
            if len(keywords) > 30:
                keywords_hash = hashlib.md5(keywords.encode()).hexdigest()[:8]
                filename_parts.append(f"search_{keywords_hash}")
            else:
                safe_keywords = keywords.replace(" ", "_").replace("/", "_")
                filename_parts.append(f"search_{safe_keywords}")
        elif config.get("n_results"):
            filename_parts.append(f"{config['n_results']}results")
        
        # Add timestamp
        filename_parts.append(timestamp)
        
        # Join with underscores
        filename = "_".join(filename_parts)
        
    else:
        # Multiple configurations - use combined approach
        if results:
            subreddit_names = [r["subreddit"] for r in results if r["status"] == "success"]
        else:
            subreddit_names = [config["name"] for config in configs_to_process]
        
        unique_subreddits = sorted(set(subreddit_names))
        
        if len(unique_subreddits) == 1:
            subreddit_part = unique_subreddits[0]
        else:
            subreddit_part = "_".join(unique_subreddits)
            if len(subreddit_part) > 50:  # Limit length for filesystem compatibility
                subreddit_hash = hashlib.md5("_".join(unique_subreddits).encode()).hexdigest()[:8]
                subreddit_part = f"multi_subreddits_{subreddit_hash}"
        
        # Create a hash of all configs to ensure uniqueness
        config_signature = hashlib.md5(
            str(sorted([(c.get("name"), c.get("category"), c.get("time_filter"), 
                        c.get("n_results"), c.get("keywords")) for c in configs_to_process])).encode()
        ).hexdigest()[:8]
        
        filename = f"{scrape_type}_{subreddit_part}_multi_{config_signature}_{timestamp}"
    
    # Add extension if requested
    if include_extension:
        filename += ".zip"
    
    return filename


def generate_unique_object_key(configs_to_process: List[Dict], scrape_type: str, 
                              today: str, timestamp: str, results: List[Dict] = None) -> str:
    """Generate a unique object key for R2 upload using unified naming"""
    
    # Generate the base filename (without extension for path building)
    base_filename = generate_unified_filename(
        configs_to_process, scrape_type, today, timestamp, results, include_extension=False
    )
    
    if len(configs_to_process) == 1:
        # Single configuration - create organized directory structure
        config = configs_to_process[0]
        subreddit = config["name"]
        
        # Category mapping for directory names
        category_names = {
            "h": "hot",
            "n": "new", 
            "t": "top",
            "r": "rising",
            "c": "controversial",
            "s": "search"
        }
        category_name = category_names.get(config["category"], config["category"])
        
        return f"{scrape_type}_scrapes/{subreddit}/{category_name}/{today}/{base_filename}.zip"
    
    else:
        # Multiple configurations - simpler directory structure
        if results:
            subreddit_names = [r["subreddit"] for r in results if r["status"] == "success"]
        else:
            subreddit_names = [config["name"] for config in configs_to_process]
        
        unique_subreddits = sorted(set(subreddit_names))
        
        if len(unique_subreddits) == 1:
            subreddit_path = unique_subreddits[0]
        else:
            subreddit_path = "multi_subreddits"
        
        return f"{scrape_type}_scrapes/{subreddit_path}/multi_config/{today}/{base_filename}.zip"


# ================================
# INDEPENDENT TASK DEFINITIONS
# ================================

@app.task(bind=True, max_retries=2)
def database_only_task(self, task_id: str, task_type: str, config: Dict, 
                      result: Dict, scrape_file: str = None):
    """Independent task to handle only database operations"""
    try:
        if not DATABASE_INTEGRATION_AVAILABLE:
            logger.warning("Database integration not available, skipping database task")
            return {"status": "skipped", "reason": "database_not_available"}
        
        logger.info(f"Starting database-only task for {config.get('name', 'unknown')}")
        
        db_processor = get_database_processor()
        if not db_processor:
            raise Exception("Could not initialize database processor")
        
        # Convert string path back to Path object if provided
        scrape_file = Path(scrape_file) if scrape_file else None
        
        save_scraping_results_to_db(
            processor=db_processor,
            task_id=task_id,
            task_type=task_type,
            config=config,
            result=result,
            scrape_file=scrape_file
        )
        
        logger.info(f"Database task completed successfully for {config.get('name')}")
        return {
            "status": "success",
            "subreddit": config.get('name'),
            "task_id": task_id,
            "database_saved": True
        }
        
    except Exception as e:
        logger.error(f"Database task failed: {e}")
        
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying database task (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(countdown=60, exc=e)
        else:
            return {
                "status": "failed",
                "subreddit": config.get('name'),
                "task_id": task_id,
                "error": str(e),
                "database_saved": False
            }


@app.task(bind=True, max_retries=2)
def upload_only_task(self, archive_path: str, object_key: str, 
                    upload_metadata: Dict = None, cleanup_after_upload: bool = True):
    """Independent task to handle only upload operations"""
    try:
        if upload_metadata is None:
            upload_metadata = {}
            
        logger.info(f"Starting upload-only task for {archive_path}")
        
        # Validate R2 configuration
        if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
            raise ValueError("R2 configuration is incomplete")
        
        if not GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True):
            logger.info("R2 upload is disabled via configuration")
            return {
                "status": "skipped", 
                "reason": "upload_disabled",
                "file": archive_path
            }
        
        # Convert string path back to Path object
        file_path = Path(archive_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Archive file not found: {archive_path}")
        
        # Perform upload
        upload_success = upload_to_r2(
            file_path=file_path, 
            object_key=object_key, 
            config=upload_metadata
        )
        
        if upload_success:
            logger.info(f"Upload completed successfully: {object_key}")
            
            # Save archive info to database if available
            if DATABASE_INTEGRATION_AVAILABLE:
                try:
                    db_processor = get_database_processor()
                    if db_processor:
                        db_processor.create_archive_record(
                            archive_path=file_path,
                            archive_type=upload_metadata.get('config_type', 'unknown'),
                            r2_object_key=object_key,
                            metadata=upload_metadata
                        )
                        logger.info("Saved archive information to database")
                except Exception as e:
                    logger.error(f"Failed to save archive info to database: {e}")
            
            # Clean up local file if requested
            if cleanup_after_upload:
                try:
                    file_path.unlink()
                    logger.info(f"Cleaned up local archive file: {archive_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up local file: {e}")
            
            return {
                "status": "success",
                "file": archive_path,
                "object_key": object_key,
                "uploaded": True,
                "cleaned_up": cleanup_after_upload
            }
        else:
            raise Exception("Upload to R2 failed")
            
    except Exception as e:
        logger.error(f"Upload task failed: {e}")
        
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying upload task (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(countdown=60, exc=e)
        else:
            return {
                "status": "failed",
                "file": archive_path,
                "error": str(e),
                "uploaded": False
            }


@app.task(bind=True, max_retries=2)  
def archive_and_upload_task(self, scrapes_dir_path: str, archive_type: str = "daily",
                          custom_name: str = None, configs_processed: List[Dict] = None,
                          results: List[Dict] = None, upload_metadata: Dict = None,
                          cleanup_after_upload: bool = True):
    """Independent task to create archive and upload (combines archive creation and upload)"""
    try:
        if upload_metadata is None:
            upload_metadata = {}
            
        logger.info(f"Starting archive and upload task for {scrapes_dir_path}")
        
        scrapes_dir = Path(scrapes_dir_path)
        if not scrapes_dir.exists():
            raise FileNotFoundError(f"Scrapes directory not found: {scrapes_dir_path}")
        
        # Check if archiving is enabled
        if not GLOBAL_SCRAPING_CONFIG.get("create_archives_enabled", True):
            logger.info("Archive creation is disabled via configuration")
            return {
                "status": "skipped",
                "reason": "archiving_disabled",
                "directory": scrapes_dir_path
            }
        
        # Create archive
        today = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        
        archive_path = create_archive(
            scrapes_dir, 
            archive_type=archive_type, 
            custom_name=custom_name, 
            configs_processed=configs_processed,
            timestamp=timestamp
        )
        
        # Generate upload object key
        if configs_processed:
            object_key = generate_unique_object_key(configs_processed, archive_type, today, timestamp, results)
        else:
            # Require configs_processed for proper unified naming
            raise ValueError("configs_processed is required for proper object key generation")
        
        # Upload the archive
        upload_result = upload_only_task.apply_async(
            args=[str(archive_path), object_key, upload_metadata, cleanup_after_upload]
        ).get()
        
        return {
            "status": "success",
            "archive_created": str(archive_path),
            "upload_result": upload_result,
            "object_key": object_key
        }
        
    except Exception as e:
        logger.error(f"Archive and upload task failed: {e}")
        
        if self.request.retries < self.max_retries:
            logger.info(f"Retrying archive and upload task (attempt {self.request.retries + 1}/{self.max_retries})")
            raise self.retry(countdown=60, exc=e)
        else:
            return {
                "status": "failed",
                "directory": scrapes_dir_path,
                "error": str(e)
            }


# ================================
# MAIN SCHEDULED TASK DEFINITIONS
# ================================

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
                # Generate timestamp for unique naming
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                
                # Create archive with unified naming
                if len(configs_to_process) == 1:
                    config = configs_to_process[0]
                    archive_type = f"{config['name']}_{config['category']}"
                else:
                    archive_type = "multiple_configs"
                
                archive_path = create_archive(
                    scrapes_dir, 
                    archive_type=archive_type,
                    configs_processed=configs_to_process,
                    timestamp=timestamp
                )

                # Upload to R2 (if enabled)
                if GLOBAL_SCRAPING_CONFIG.get("upload_to_r2_enabled", True):
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


@app.task(bind=True, max_retries=None)
def scheduled_scrape_task_modular(self, config_id: int = None):
    """NEW: Modular scheduled scraping task using independent database and upload tasks"""
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
        
        logger.info(f"Starting modular scheduled Reddit scraping task for config ID: {config_id}")

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

        # STEP 1: SCRAPING ONLY - Process each subreddit configuration
        logger.info("Step 1: Starting scraping phase")
        results = []
        database_tasks = []
        
        for config in configs_to_process:
            # Scrape only (no database operations)
            result = process_subreddit_config(config, scrapes_dir)
            results.append(result)
            
            # STEP 2: LAUNCH INDEPENDENT DATABASE TASKS for successful scrapes
            if result["status"] == "success" and DATABASE_INTEGRATION_AVAILABLE:
                try:
                    task_id = f"scheduled_{result['subreddit']}_{result['category']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    
                    # Create serializable config (remove schedule fields)
                    config_serializable = make_config_serializable(config)
                    
                    db_task = database_only_task.apply_async(
                        args=[task_id, "scheduled", config_serializable, result, result.get("scrape_file")]
                    )
                    
                    database_tasks.append({
                        "task_id": db_task.id,
                        "subreddit": result['subreddit'],
                        "config": config_serializable
                    })
                    
                    logger.info(f"Launched database task {db_task.id} for r/{result['subreddit']}")
                    
                except Exception as e:
                    logger.error(f"Failed to launch database task for r/{result['subreddit']}: {e}")

        # STEP 3: ARCHIVE AND UPLOAD PHASE
        successful_results = [r for r in results if r["status"] == "success"]
        
        upload_task_result = None
        if scrapes_dir.exists() and successful_results:
            logger.info("Step 3: Starting archive and upload phase")
            
            # Build upload metadata
            upload_metadata = {
                'config_type': 'scheduled',
                'subreddits': ",".join(set(r["subreddit"] for r in successful_results)),
                'configs_processed': len(configs_to_process),
                'successful_scrapes': len(successful_results),
                'skipped_scrapes': len([r for r in results if r["status"] == "skipped"]),
                'total_submissions': sum(r.get("submissions_found", 0) for r in results),
                'total_comments_scraped': sum(r.get("comments_scraped", 0) for r in results)
            }
            
            # Determine archive type
            if len(configs_to_process) == 1:
                config = configs_to_process[0]
                archive_type = f"{config['name']}_{config['category']}"
            else:
                archive_type = "multiple_configs"
            
            # Launch independent archive and upload task
            try:
                # Create serializable configs (remove schedule fields)
                configs_serializable = [make_config_serializable(config) for config in configs_to_process]
                
                upload_task = archive_and_upload_task.apply_async(
                    args=[str(scrapes_dir), archive_type, None, configs_serializable, results, upload_metadata, True]
                )
                
                # Wait for upload task to complete
                upload_task_result = upload_task.get()
                logger.info(f"Archive and upload task completed: {upload_task_result['status']}")
                
            except Exception as e:
                logger.error(f"Archive and upload task failed: {e}")
                upload_task_result = {"status": "failed", "error": str(e)}

        # STEP 4: COLLECT DATABASE TASK RESULTS (optional - don't wait)
        database_results = []
        for db_task_info in database_tasks:
            try:
                # Check status without waiting (for reporting purposes)
                from celery.result import AsyncResult
                task_result = AsyncResult(db_task_info["task_id"])
                database_results.append({
                    "subreddit": db_task_info["subreddit"],
                    "task_id": db_task_info["task_id"],
                    "status": task_result.state,
                    "ready": task_result.ready()
                })
            except Exception as e:
                logger.warning(f"Could not check database task status: {e}")
                database_results.append({
                    "subreddit": db_task_info["subreddit"],
                    "task_id": db_task_info["task_id"],
                    "status": "unknown",
                    "error": str(e)
                })

        # Return comprehensive results
        return {
            "status": "success",
            "config_id": config_id,
            "date": today,
            "scraping_results": results,
            "successful_scrapes": len(successful_results),
            "skipped_scrapes": len([r for r in results if r["status"] == "skipped"]),
            "total_submissions": sum(r.get("submissions_found", 0) for r in results),
            "total_comments_scraped": sum(r.get("comments_scraped", 0) for r in results),
            "database_tasks": database_results,
            "upload_task": upload_task_result,
            "modular_execution": True
        }

    except Exception as exc:
        logger.error(f"Modular scheduled scraping task failed: {exc}")
        
        # Handle retries
        if self.request.retries < max_retries:
            logger.info(f"Retrying in {retry_delay} seconds (attempt {self.request.retries + 1}/{max_retries})")
            raise self.retry(countdown=retry_delay, exc=exc)
        else:
            logger.error(f"Modular scheduled scraping task failed after {max_retries} retries")
            return {
                "status": "failed",
                "config_id": config_id,
                "error": str(exc),
                "retries_exhausted": True,
                "modular_execution": True
            }


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
        # Use unified naming for manual scrapes
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        archive_path = create_archive(
            scrapes_dir, 
            archive_type="manual", 
            configs_processed=enabled_configs,
            timestamp=timestamp
        )
        
        # Upload to R2
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
