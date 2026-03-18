# Capture System API

This API provides programmatic control over the capture system, replacing the need to manually run shell scripts.

## Simple Example

**Most common use case - change camera setup and prepare system:**
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"SOURCE_IP": "192.168.0.15", "SINK_IPS": ["192.168.0.13"], "directory": "GameTheoryGachibowli_badminton_court1"}' \
  http://localhost:5000/api/prepare
```

This single command will:
1. Update your `variable_files/config.yaml` with the new IP addresses
2. Update your `variable_files/info.yaml` with the new directory
3. Run the prepare script with the new configuration
4. Return success/failure status

## Quick Start

### 1. Start the API Server
```bash
cd /home/pi/source_code
./start_api_server.sh
```

### 2. Test the API
```bash
python3 test_api.py
```

### 3. Use the API (Simplified Workflow)
```bash
# Prepare cameras with new configuration (recommended)
curl -X POST -H "Content-Type: application/json" \
  -d '{"SOURCE_IP": "192.168.0.15", "SINK_IPS": ["192.168.0.13"], "directory": "GameTheoryGachibowli_badminton_court1"}' \
  http://localhost:5000/api/prepare

# Start capture
curl -X POST http://localhost:5000/api/start

# Stop capture
curl -X POST http://localhost:5000/api/stop

# Check status
curl -X GET http://localhost:5000/api/status

# Get temperatures of all Pi's
curl -X GET http://localhost:5000/api/temperature
```

**Note**: You can now send configuration directly with the prepare command - no need to call config API separately!

## Configuration Management

### **Option 1: Prepare with Configuration (Recommended)**
Update configuration and prepare cameras in one call:
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"SOURCE_IP": "192.168.0.15", "SINK_IPS": ["192.168.0.13"], "directory": "GameTheoryGachibowli_badminton_court1"}' \
  http://localhost:5000/api/prepare
```

**When to use**: Most common use case - you want to change camera setup and immediately prepare the system.

### **Option 2: Separate Configuration Update**
Update configuration first, then prepare separately:
```bash
# Step 1: Update configuration
curl -X POST -H "Content-Type: application/json" \
  -d '{"SOURCE_IP": "192.168.0.15", "SINK_IPS": ["192.168.0.13"], "directory": "GameTheoryGachibowli_badminton_court1"}' \
  http://localhost:5000/api/config

# Step 2: Prepare cameras (uses updated configuration)
curl -X POST http://localhost:5000/api/prepare
```

**When to use**: When you want to update configuration without immediately preparing cameras, or when you need to update config multiple times before preparing.

## Troubleshooting

### Flask Installation Error
If you encounter an error like:
```
error: externally-managed-environment
× This environment is externally managed
```

This is a common issue on newer Raspberry Pi OS versions. The solution is already implemented in the startup script:

1. **Virtual Environment**: The script automatically creates a Python virtual environment (`venv/`) in your project directory
2. **Dependencies**: All required packages (Flask, PyYAML, requests) are installed in this virtual environment
3. **Isolation**: This keeps your system Python clean while providing all needed dependencies

**Manual Fix** (if needed):
```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run API server
venv/bin/python api_server.py
```

### Dependencies
The API server requires these Python packages (automatically installed):
- `flask` - Web framework
- `pyyaml` - YAML configuration parsing  
- `requests` - HTTP client for camera communication

## API Endpoints

### GET /api/config
Gets current configuration from variable_files/config.yaml and variable_files/info.yaml

**Response:**
```json
{
  "status": "success",
  "config": {
    "SOURCE_IP": "192.168.0.11",
    "SINK_IPS": ["192.168.0.13", "192.168.0.12"]
  },
  "info": {
    "directory": "GameTheoryGachibowli_badminton_court1"
  },
  "timestamp": "2024-01-01T12:00:00"
}
```

### POST /api/config
Updates configuration files without running prepare script

**Request Body:**
```json
{
  "SOURCE_IP": "192.168.0.11",
  "SINK_IPS": ["192.168.0.13", "192.168.0.12"],
  "directory": "GameTheoryGachibowli_badminton_court1"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Configuration updated successfully",
  "new_config": {
    "SOURCE_IP": "192.168.0.11",
    "SINK_IPS": ["192.168.0.13", "192.168.0.12"]
  },
  "new_info": {
    "directory": "GameTheoryGachibowli_badminton_court1"
  },
  "timestamp": "2024-01-01T12:00:00"
}
```

### POST /api/prepare
Prepares cameras for capture and optionally updates configuration (equivalent to `./prepare_cameras.sh`)

**This is the main endpoint for most use cases!** It can:
- Update configuration files (variable_files/config.yaml and variable_files/info.yaml)
- Run the prepare script with new settings
- All in one API call

**Request Body (optional but recommended):**
```json
{
  "SOURCE_IP": "192.168.0.15",
  "SINK_IPS": ["192.168.0.13"],
  "directory": "GameTheoryGachibowli_badminton_court1"
}
```

**Response (with config update):**
```json
{
  "status": "success",
  "message": "Cameras prepared successfully",
  "output": "...",
  "config_updated": true,
  "new_config": {
    "SOURCE_IP": "192.168.0.15",
    "SINK_IPS": ["192.168.0.13"]
  }
}
```

**Response (without config update - just prepare):**
```json
{
  "status": "success",
  "message": "Cameras prepared successfully",
  "output": "..."
}
```

### POST /api/start
Starts the capture process (equivalent to `./start_capture.sh`)

**Response:**
```json
{
  "status": "success", 
  "message": "Capture started successfully",
  "track_id": 6736,
  "output": "..."
}
```

### POST /api/stop
Sends stop signal to the capture system (creates `/tmp/capture_stop_flag`)

**Response:**
```json
{
  "status": "success",
  "message": "Stop signal sent successfully", 
  "timestamp": "2024-01-01T12:00:00"
}
```

### GET /api/status
Gets current system status

**Response:**
```json
{
  "status": "idle|capturing|preparing",
  "current_track_id": 6735,
  "sessions": {
    "server": true,
    "testing": false,
    "upload": true
  },
  "cameras": {
    "source": "IDLE|CAPTURE|UNREACHABLE",
    "sinks": {
      "192.168.1.12": "IDLE",
      "192.168.1.13": "CAPTURE"
    }
  },
  "stop_flag_exists": false,
  "last_error": null,
  "timestamp": "2024-01-01T12:00:00"
}
```

### GET /api/health
Simple health check endpoint

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:00:00"
}
```

### GET /api/temperature
Get temperature of source and all sink Pi's

**Response:**
```json
{
  "status": "success",
  "temperatures": {
    "source": {
      "ip": "192.168.0.15",
      "temperature": 55.5,
      "unit": "°C"
    },
    "sinks": {
      "192.168.0.13": {
        "temperature": 48.2,
        "unit": "°C"
      }
    }
  },
  "timestamp": "2024-01-01T12:00:00"
}
```

**Error Response (if temperature reading fails):**
```json
{
  "status": "success",
  "temperatures": {
    "source": {
      "ip": "192.168.0.15",
      "temperature": 55.5,
      "unit": "°C"
    },
    "sinks": {
      "192.168.0.13": {
        "temperature": null,
        "error": "Failed to get temperature"
      }
    }
  },
  "timestamp": "2024-01-01T12:00:00"
}
```

## Management Scripts

### Start API Server
```bash
./start_api_server.sh
```

### Stop API Server  
```bash
./stop_api_server.sh
```

### Check if API Server is Running
```bash
pgrep -f "api_server.py"
```

## Network Access

The API server runs on `0.0.0.0:5000`, making it accessible from any device on the network.

**Local access:** `http://localhost:5000`
**Network access:** `http://192.168.1.11:5000`

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200`: Success
- `500`: Server error

Error responses include:
```json
{
  "status": "error",
  "message": "Error description",
  "error": "Detailed error information"
}
```

## Integration Examples

### Python
```python
import requests

# Prepare cameras
response = requests.post("http://192.168.1.11:5000/api/prepare")
if response.status_code == 200:
    print("Cameras prepared successfully")

# Start capture
response = requests.post("http://192.168.1.11:5000/api/start")
if response.status_code == 200:
    print("Capture started")

# Monitor status
response = requests.get("http://192.168.1.11:5000/api/status")
status = response.json()
print(f"System status: {status['status']}")
```

### JavaScript/Node.js
```javascript
const axios = require('axios');

// Prepare cameras
axios.post('http://192.168.1.11:5000/api/prepare')
  .then(response => console.log('Cameras prepared'))
  .catch(error => console.error('Error:', error));

// Start capture
axios.post('http://192.168.1.11:5000/api/start')
  .then(response => console.log('Capture started'))
  .catch(error => console.error('Error:', error));
```

## Troubleshooting

### API Server Won't Start
1. Check if Flask is installed: `pip3 install flask`
2. Check if port 5000 is available: `netstat -tlnp | grep 5000`
3. Check logs for errors

### Can't Access from Network
1. Check firewall: `sudo ufw status`
2. Allow port 5000: `sudo ufw allow 5000`
3. Verify IP address: `ip addr show`

### Scripts Not Found
1. Ensure you're in the correct directory: `/home/pi/source_code`
2. Check script permissions: `ls -la *.sh`
3. Make scripts executable: `chmod +x *.sh`
