#!/bin/bash

# Start API Server Script
# This script starts the capture system API server

echo "Starting Capture System API Server..."

# Change to the source_code directory
cd /home/pi/source_code

# Check if virtual environment exists, create if it doesn't
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Check and install required packages
echo "Checking and installing required packages..."
pip install -r requirements.txt

# Check if the API server is already running
if pgrep -f "api_server.py" > /dev/null; then
    echo "API server is already running. Stopping existing instance..."
    pkill -f "api_server.py"
    sleep 2
fi

# Start the API server using the virtual environment's Python
echo "Starting API server on port 5000..."
nohup venv/bin/python api_server.py > api_server.log 2>&1 &

# Get the PID
API_PID=$!
echo "API server started with PID: $API_PID"

# Save PID to file for later use
echo $API_PID > /tmp/capture_api_server.pid

# Wait a moment for server to start
sleep 2

# Test if server is running
if curl -s http://localhost:5000/api/health > /dev/null; then
    echo "API server is running successfully!"
else
    echo "Warning: API server may not have started properly. Check api_server.log for details."
fi

echo "API server is now running at: http://0.0.0.0:5000"
echo "Available endpoints:"
echo "  POST /api/prepare - Prepare cameras"
echo "  POST /api/start   - Start capture"
echo "  POST /api/stop    - Stop capture"
echo "  GET  /api/status  - Get system status"
echo "  GET  /api/health  - Health check"
echo "  GET  /api/config  - Get configuration"
echo "  POST /api/config  - Update configuration"
echo "  GET  /api/temperature - Get Pi temperatures"
echo ""
echo "To stop the API server: kill $API_PID"
echo "Or use: ./stop_api_server.sh"

# Deactivate virtual environment
deactivate
