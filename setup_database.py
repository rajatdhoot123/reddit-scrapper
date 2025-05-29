#!/usr/bin/env python3
"""
Database setup script for Reddit Scraping System
This script creates all necessary tables and can be used for initial setup.
"""

import os
import sys
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import models
from models import Base, DatabaseConfig
from database_integration import DatabaseManager

def create_database_if_not_exists(database_url: str):
    """Create database if it doesn't exist (PostgreSQL)"""
    
    # Parse database URL to get database name
    from urllib.parse import urlparse
    parsed = urlparse(database_url)
    
    if parsed.scheme.startswith('postgresql'):
        # Connect to postgres (default) database to create our target database
        postgres_url = database_url.replace(f"/{parsed.path[1:]}", "/postgres")
        
        try:
            engine = create_engine(postgres_url, isolation_level='AUTOCOMMIT')
            with engine.connect() as conn:
                # Check if database exists
                result = conn.execute(text(
                    f"SELECT 1 FROM pg_database WHERE datname = '{parsed.path[1:]}'"
                ))
                
                if not result.fetchone():
                    # Create database
                    conn.execute(text(f'CREATE DATABASE "{parsed.path[1:]}"'))
                    print(f"Created database: {parsed.path[1:]}")
                else:
                    print(f"Database {parsed.path[1:]} already exists")
                    
        except Exception as e:
            print(f"Note: Could not create database automatically: {e}")
            print("Please ensure the database exists before running this script.")


def setup_database():
    """Set up the database with all necessary tables"""
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("Error: DATABASE_URL environment variable not set")
        print("Please set DATABASE_URL in your .env file or environment")
        sys.exit(1)
    
    print(f"Setting up database...")
    print(f"Database URL: {database_url.split('@')[0]}@[REDACTED]")
    
    try:
        # Try to create database if it doesn't exist
        create_database_if_not_exists(database_url)
        
        # Initialize database manager
        db_manager = DatabaseManager(database_url)
        
        # Create all tables
        print("Creating database tables...")
        Base.metadata.create_all(bind=db_manager.engine)
        
        # Test connection
        with db_manager.get_session() as session:
            result = session.execute(text("SELECT version()"))
            db_version = result.fetchone()[0]
            print(f"Database connection successful!")
            print(f"Database version: {db_version}")
        
        print("\n✅ Database setup completed successfully!")
        print("\nTables created:")
        for table_name in Base.metadata.tables.keys():
            print(f"  - {table_name}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Database setup failed: {e}")
        return False


def create_sample_data():
    """Create some sample data for testing"""
    
    from models import Subreddit, ScrapeSession, TaskType, CategoryType, ScrapeStatus
    
    database_url = os.getenv('DATABASE_URL')
    db_manager = DatabaseManager(database_url)
    
    with db_manager.get_session() as session:
        # Create sample subreddit
        subreddit = Subreddit(
            name="CreditCardsIndia",
            display_name="CreditCardsIndia",
            description="Sample subreddit for testing",
            is_active=True
        )
        session.add(subreddit)
        session.flush()
        
        # Create sample scrape session
        scrape_session = ScrapeSession(
            task_id="sample_task_001",
            task_type=TaskType.MANUAL,
            subreddit_id=subreddit.id,
            category=CategoryType.TOP,
            n_results=25,
            status=ScrapeStatus.SUCCESS,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            duration_seconds=120,
            submissions_found=25,
            submissions_scraped=25,
            comments_scraped=150
        )
        session.add(scrape_session)
        
        print("✅ Sample data created successfully!")


def show_database_info():
    """Show information about the current database setup"""
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("DATABASE_URL not set")
        return
    
    try:
        db_manager = DatabaseManager(database_url)
        
        with db_manager.get_session() as session:
            # Count records in each table
            from models import Subreddit, ScrapeSession, Submission, Comment, Archive, ProcessingQueue
            
            tables_info = [
                ("Subreddits", session.query(Subreddit).count()),
                ("Scrape Sessions", session.query(ScrapeSession).count()),
                ("Submissions", session.query(Submission).count()),
                ("Comments", session.query(Comment).count()),
                ("Archives", session.query(Archive).count()),
                ("Processing Queue", session.query(ProcessingQueue).count()),
            ]
            
            print("\n📊 Database Statistics:")
            print("-" * 30)
            for table_name, count in tables_info:
                print(f"{table_name:20}: {count:>6} records")
            
    except Exception as e:
        print(f"Error getting database info: {e}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Reddit Scraper Database Setup")
    parser.add_argument("--setup", action="store_true", help="Set up database tables")
    parser.add_argument("--sample-data", action="store_true", help="Create sample data")
    parser.add_argument("--info", action="store_true", help="Show database information")
    
    args = parser.parse_args()
    
    if args.setup:
        setup_database()
    elif args.sample_data:
        create_sample_data()
    elif args.info:
        show_database_info()
    else:
        print("Reddit Scraper Database Setup")
        print("Usage:")
        print("  python setup_database.py --setup      # Create database tables")
        print("  python setup_database.py --sample-data # Create sample data")
        print("  python setup_database.py --info       # Show database stats") 