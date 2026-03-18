#!/bin/bash

# Stop API Server Script
# This script stops the capture system API server

echo "Stopping Capture System API Server..."

# Check if API server is running
if pgrep -f "api_server.py" > /dev/null; then
    echo "Found running API server. Stopping..."
    
    # Try to stop gracefully first
    pkill -f "api_server.py"
    
    # Wait a moment
    sleep 2
    
    # Check if it's still running
    if pgrep -f "api_server.py" > /dev/null; then
        echo "API server still running. Force killing..."
        pkill -9 -f "api_server.py"
    fi
    
    echo "API server stopped successfully."
else
    echo "No API server found running."
fi

# Remove PID file if it exists
if [ -f "/tmp/capture_api_server.pid" ]; then
    rm /tmp/capture_api_server.pid
    echo "Removed PID file."
fi

echo "API server shutdown complete."
