#!/bin/bash

# Start Celery services for Reddit scraping automation

echo "Starting Celery services..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found. Please copy env.example to .env and configure it."
    echo "cp env.example .env"
    exit 1
fi

# Load environment variables
source .env

# Check if required Redis environment variables are set
if [ -z "$REDIS_HOST" ] || [ -z "$REDIS_PORT" ]; then
    echo "Error: Redis environment variables not set."
    echo "Please ensure REDIS_HOST and REDIS_PORT are configured in your .env file."
    exit 1
fi

# Test local Redis connection
echo "Testing local Redis connection..."
REDIS_URL="redis://${REDIS_HOST}:${REDIS_PORT}"

# Add password to URL if provided
if [ ! -z "$REDIS_PASSWORD" ]; then
    REDIS_URL="redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}"
fi

# Use Python to test the Redis connection
python3 -c "
import redis
import sys
import os
from urllib.parse import urlparse

try:
    # Parse the Redis URL
    url = '$REDIS_URL'
    parsed = urlparse(url)
    
    # Create Redis connection for local Redis (no SSL)
    r = redis.Redis(
        host=parsed.hostname,
        port=parsed.port,
        password=parsed.password if parsed.password else None,
        decode_responses=True
    )
    
    # Test the connection
    r.ping()
    print('✓ Local Redis connection successful')
    sys.exit(0)
except Exception as e:
    print(f'✗ Local Redis connection failed: {e}')
    print('Make sure Redis is running locally with: redis-server')
    sys.exit(1)
"

if [ $? -ne 0 ]; then
    echo "Error: Cannot connect to local Redis. Please check your Redis configuration and ensure Redis is running."
    echo "To start Redis locally, run: redis-server"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Start Celery worker in background
echo "Starting Celery worker..."
celery -A celery_config worker --loglevel=info --logfile=logs/celery_worker.log --detach

# Start Celery beat scheduler in background
echo "Starting Celery beat scheduler..."
celery -A celery_config beat --loglevel=info --logfile=logs/celery_beat.log --detach

echo "Celery services started successfully!"
echo "Worker log: logs/celery_worker.log"
echo "Beat log: logs/celery_beat.log"
echo ""
echo "To stop the services, run: ./stop_celery.sh"
echo "To monitor tasks, run: ./start_flower_improved.sh" 