#!/bin/bash

# Improved Flower startup script with better Redis URL handling

echo "Starting Flower monitoring interface..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Warning: .env file not found. Please copy env.example to .env and configure it."
    echo "cp env.example .env"
    exit 1
fi

# Load environment variables
source .env

# Construct Redis URL (same as in celery_config.py)
REDIS_URL="rediss://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}?ssl_cert_reqs=none"

echo "Redis URL: ${REDIS_URL}"
echo "Starting Flower on http://localhost:5555"
echo "Press Ctrl+C to stop"

# Start Flower with comprehensive configuration
celery -A celery_config flower \
    --port=5555 \
    --broker="${REDIS_URL}" \
    --persistent=true \
    --db=flower.db \
    --max_tasks=10000 \
    --enable_events \
    --auto_refresh=true \
    --debug=true 