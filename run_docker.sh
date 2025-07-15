#!/bin/bash

# Build and run CSV Manager Docker container

echo "Building CSV Manager Docker image..."
docker build -t csv-manager:latest .

echo "Stopping any existing container..."
docker stop csv_manager_app 2>/dev/null || true
docker rm csv_manager_app 2>/dev/null || true

echo "Starting CSV Manager container on port 17653..."
docker run -d \
  --name csv_manager_app \
  -p 17653:17653 \
  -v "$(pwd)/uploads:/app/uploads" \
  csv-manager:latest

echo "CSV Manager is now running at http://localhost:17653"
echo "To view logs: docker logs csv_manager_app"
echo "To stop: docker stop csv_manager_app"
