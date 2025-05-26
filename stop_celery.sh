#!/bin/bash

# Stop Celery services

echo "Stopping Celery services..."

# Stop Celery worker
echo "Stopping Celery worker..."
pkill -f "celery.*worker"

# Stop Celery beat
echo "Stopping Celery beat..."
pkill -f "celery.*beat"

# Stop Flower if running
echo "Stopping Celery Flower (if running)..."
pkill -f "celery.*flower"

echo "Celery services stopped." 