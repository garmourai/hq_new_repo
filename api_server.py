#!/usr/bin/env python3

import flask
from flask import Flask, jsonify, request
import subprocess
import os
import json
import yaml
import threading
import time
import requests
import logging
from datetime import datetime

app = Flask(__name__)

# Setup logging with more detailed format
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Configuration
CONFIG_FILE = "variable_files/config.yaml"
INFO_FILE = "variable_files/info.yaml"
PREPARE_SCRIPT = "./prepare_cameras.sh"
START_SCRIPT = "./start_capture.sh"
STOP_FLAG_FILE = "/tmp/capture_stop_flag"
TRACK_INDEX_FILE = "variable_files/track_video_index.json"

class CaptureController:
    def __init__(self):
        self.load_config()
        self.capture_status = "idle"  # idle, preparing, capturing
        self.current_track_id = None
        self.last_error = None
        
    def load_config(self):
        """Load configuration from YAML file"""
        try:
            with open(CONFIG_FILE, 'r') as file:
                self.config = yaml.safe_load(file)
            self.source_ip = self.config['SOURCE_IP']
            self.sink_ips = self.config['SINK_IPS']
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            self.config = {}
            self.source_ip = "192.168.1.11"
            self.sink_ips = ["192.168.1.12", "192.168.1.13"]
    
    def update_config_files(self, config_data):
        """Update config.yaml and info.yaml with new data"""
        try:
            # Update config.yaml with IP information
            config_update = {
                'SOURCE_IP': config_data.get('SOURCE_IP'),
                'SINK_IPS': config_data.get('SINK_IPS', [])
            }
            
            with open(CONFIG_FILE, 'w') as file:
                yaml.dump(config_update, file, default_flow_style=False, indent=2)
            
            # Update info.yaml with directory and unique_id information
            info_update = {
                'directory': config_data.get('directory'),
                'unique_id': config_data.get('unique_id')
            }
            
            logging.info(f"Updating info.yaml with: {info_update}")
            with open(INFO_FILE, 'w') as file:
                yaml.dump(info_update, file, default_flow_style=False, indent=2)
            logging.info(f"Successfully updated {INFO_FILE}")
            
            # Reload config after updating files
            self.load_config()
            
            return True
        except Exception as e:
            logging.error(f"Error updating config files: {e}")
            return False
    
    def get_track_id(self):
        """Get current track ID from JSON file"""
        try:
            with open(TRACK_INDEX_FILE, 'r') as file:
                data = json.load(file)
                return data.get('counter', 'unknown')
        except:
            return 'unknown'
    
    def check_tmux_session(self, session_name):
        """Check if a tmux session exists and is running"""
        try:
            result = subprocess.run(
                ['tmux', 'has-session', '-t', session_name],
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except:
            return False
    
    def get_camera_status(self, ip):
        """Get camera status via HTTP API"""
        try:
            response = requests.get(f"http://{ip}:8080/status", timeout=5)
            if response.ok:
                return response.json().get("state", "UNKNOWN")
            return "UNREACHABLE"
        except:
            return "UNREACHABLE"
    
    def get_pi_temperature(self, ip):
        """Get temperature of a Pi via SSH"""
        try:
            # Use SSH to run vcgencmd measure_temp on the remote Pi
            ssh_command = [
                "sshpass", "-p", "qwaszx99", 
                "ssh", 
                f"pi@{ip}", 
                "vcgencmd measure_temp"
            ]
            
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                # Extract temperature value from output like "temp=55.5'C"
                temp_output = result.stdout.strip()
                if temp_output.startswith("temp="):
                    temp_value = temp_output.replace("temp=", "").replace("'C", "")
                    return float(temp_value)
                else:
                    return None
            else:
                return None
                
        except Exception as e:
            logging.error(f"Error getting temperature from {ip}: {e}")
            return None

# Global controller instance
controller = CaptureController()

@app.route('/api/config', methods=['GET'])
def get_config():
    """Get current configuration"""
    try:
        # Load current config files
        config_data = {}
        info_data = {}
        
        try:
            with open(CONFIG_FILE, 'r') as file:
                config_data = yaml.safe_load(file)
        except:
            config_data = {}
            
        try:
            with open(INFO_FILE, 'r') as file:
                info_data = yaml.safe_load(file)
        except:
            info_data = {}
        
        return jsonify({
            "status": "success",
            "config": config_data,
            "info": info_data,
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to get configuration: {str(e)}"
        }), 500

@app.route('/api/config', methods=['POST'])
def update_config():
    """Update configuration files without running prepare script"""
    try:
        if not request.is_json:
            return jsonify({
                "status": "error",
                "message": "JSON data required"
            }), 400
        
        config_data = request.get_json()
        logging.info(f"Received config data: {config_data}")
        
        # Validate required fields
        required_fields = ['SOURCE_IP', 'SINK_IPS', 'directory']
        missing_fields = [field for field in required_fields if field not in config_data]
        
        if missing_fields:
            return jsonify({
                "status": "error",
                "message": f"Missing required fields: {', '.join(missing_fields)}",
                "required_fields": required_fields
            }), 400
        
        # Update configuration files
        if not controller.update_config_files(config_data):
            return jsonify({
                "status": "error",
                "message": "Failed to update configuration files"
            }), 500
        
        print(f"Configuration updated: SOURCE_IP={config_data['SOURCE_IP']}, SINK_IPS={config_data['SINK_IPS']}, directory={config_data['directory']}, unique_id={config_data.get('unique_id')}")
        
        return jsonify({
            "status": "success",
            "message": "Configuration updated successfully",
            "new_config": {
                "SOURCE_IP": controller.source_ip,
                "SINK_IPS": controller.sink_ips
            },
            "new_info": {
                "directory": config_data.get('directory'),
                "unique_id": config_data.get('unique_id')
            },
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to update configuration: {str(e)}"
        }), 500

@app.route('/api/prepare', methods=['POST'])
def prepare_cameras():
    """Prepare cameras - equivalent to ./prepare_cameras.sh"""
    logging.info("=== PREPARE OPERATION STARTED ===")
    start_time = datetime.now()
    
    try:
        # Check if JSON data was sent
        if request.is_json:
            config_data = request.get_json()
            logging.info(f"Received configuration data: SOURCE_IP={config_data.get('SOURCE_IP')}, SINK_IPS={config_data.get('SINK_IPS')}, directory={config_data.get('directory')}")
            
            # Validate required fields
            required_fields = ['SOURCE_IP', 'SINK_IPS', 'directory']
            missing_fields = [field for field in required_fields if field not in config_data]
            
            if missing_fields:
                logging.error(f"Missing required fields: {missing_fields}")
                return jsonify({
                    "status": "error",
                    "message": f"Missing required fields: {', '.join(missing_fields)}",
                    "required_fields": required_fields
                }), 400
            
            # Update configuration files
            logging.info("Updating configuration files...")
            if not controller.update_config_files(config_data):
                logging.error("Failed to update configuration files")
                return jsonify({
                    "status": "error",
                    "message": "Failed to update configuration files"
                }), 500
            
            logging.info(f"Configuration updated successfully: SOURCE_IP={config_data['SOURCE_IP']}, SINK_IPS={config_data['SINK_IPS']}, directory={config_data['directory']}")
        
        logging.info("Setting capture status to 'preparing'")
        controller.capture_status = "preparing"
        controller.last_error = None
        
        # Run prepare script with proper environment
        env = os.environ.copy()
        env['PYTHONPATH'] = os.getcwd()
        
        logging.info(f"Executing prepare script: {PREPARE_SCRIPT}")
        logging.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(
            [PREPARE_SCRIPT],
            cwd=os.getcwd(),
            env=env,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"Prepare script execution completed in {execution_time:.2f} seconds with return code: {result.returncode}")
        
        if result.returncode == 0:
            logging.info("Prepare script executed successfully")
            if result.stdout:
                logging.info(f"Prepare script output: {result.stdout.strip()}")
            
            controller.capture_status = "idle"
            response_data = {
                "status": "success",
                "message": "Cameras prepared successfully",
                "output": result.stdout,
                "execution_time_seconds": execution_time
            }
            
            # Add configuration info if it was updated
            if request.is_json:
                response_data["config_updated"] = True
                response_data["new_config"] = {
                    "SOURCE_IP": controller.source_ip,
                    "SINK_IPS": controller.sink_ips
                }
            
            logging.info("=== PREPARE OPERATION COMPLETED SUCCESSFULLY ===")
            return jsonify(response_data), 200
        else:
            logging.error(f"Prepare script failed with return code: {result.returncode}")
            logging.error(f"Prepare script error output: {result.stderr}")
            controller.capture_status = "idle"
            controller.last_error = result.stderr
            logging.error("=== PREPARE OPERATION FAILED ===")
            return jsonify({
                "status": "error",
                "message": "Failed to prepare cameras",
                "error": result.stderr,
                "execution_time_seconds": execution_time
            }), 500
            
    except subprocess.TimeoutExpired:
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.error(f"Prepare operation timed out after {execution_time:.2f} seconds")
        controller.capture_status = "idle"
        logging.error("=== PREPARE OPERATION TIMED OUT ===")
        return jsonify({
            "status": "error",
            "message": "Prepare operation timed out",
            "execution_time_seconds": execution_time
        }), 500
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.error(f"Unexpected error during prepare operation: {str(e)}")
        controller.capture_status = "idle"
        controller.last_error = str(e)
        logging.error("=== PREPARE OPERATION FAILED WITH EXCEPTION ===")
        return jsonify({
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "execution_time_seconds": execution_time
        }), 500

@app.route('/api/start', methods=['POST'])
def start_capture():
    """Start capture - equivalent to ./start_capture.sh"""
    logging.info("=== START OPERATION STARTED ===")
    start_time = datetime.now()
    
    try:
        logging.info("Setting capture status to 'capturing'")
        controller.capture_status = "capturing"
        controller.last_error = None
        
        # Get current track ID
        controller.current_track_id = controller.get_track_id()
        logging.info(f"Current track ID: {controller.current_track_id}")
        
        # Run start script with proper environment
        env = os.environ.copy()
        env['PYTHONPATH'] = os.getcwd()
        
        logging.info(f"Executing start script: {START_SCRIPT}")
        logging.info(f"Working directory: {os.getcwd()}")
        
        result = subprocess.run(
            [START_SCRIPT],
            cwd=os.getcwd(),
            env=env,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"Start script execution completed in {execution_time:.2f} seconds with return code: {result.returncode}")
        
        if result.returncode == 0:
            logging.info("Start script executed successfully")
            if result.stdout:
                logging.info(f"Start script output: {result.stdout.strip()}")
            
            logging.info("=== START OPERATION COMPLETED SUCCESSFULLY ===")
            return jsonify({
                "status": "success",
                "message": "Capture started successfully",
                "track_id": controller.current_track_id,
                "output": result.stdout,
                "execution_time_seconds": execution_time
            }), 200
        else:
            logging.error(f"Start script failed with return code: {result.returncode}")
            logging.error(f"Start script error output: {result.stderr}")
            controller.capture_status = "idle"
            controller.last_error = result.stderr
            logging.error("=== START OPERATION FAILED ===")
            return jsonify({
                "status": "error",
                "message": "Failed to start capture",
                "error": result.stderr,
                "execution_time_seconds": execution_time
            }), 500
            
    except subprocess.TimeoutExpired:
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.error(f"Start operation timed out after {execution_time:.2f} seconds")
        controller.capture_status = "idle"
        logging.error("=== START OPERATION TIMED OUT ===")
        return jsonify({
            "status": "error",
            "message": "Start operation timed out",
            "execution_time_seconds": execution_time
        }), 500
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.error(f"Unexpected error during start operation: {str(e)}")
        controller.capture_status = "idle"
        controller.last_error = str(e)
        logging.error("=== START OPERATION FAILED WITH EXCEPTION ===")
        return jsonify({
            "status": "error",
            "message": f"Unexpected error: {str(e)}",
            "execution_time_seconds": execution_time
        }), 500

@app.route('/api/stop', methods=['POST'])
def stop_capture():
    """Stop capture - creates stop flag file"""
    logging.info("=== STOP OPERATION STARTED ===")
    start_time = datetime.now()
    
    try:
        # Check if stop flag already exists
        if os.path.exists(STOP_FLAG_FILE):
            logging.warning(f"Stop flag file already exists: {STOP_FLAG_FILE}")
        
        # Create stop flag file
        stop_message = f"Stop requested at {datetime.now().isoformat()}"
        logging.info(f"Creating stop flag file: {STOP_FLAG_FILE}")
        
        with open(STOP_FLAG_FILE, 'w') as f:
            f.write(stop_message)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"Stop flag file created successfully in {execution_time:.2f} seconds")
        logging.info("=== STOP OPERATION COMPLETED SUCCESSFULLY ===")
        
        return jsonify({
            "status": "success",
            "message": "Stop signal sent successfully",
            "timestamp": datetime.now().isoformat(),
            "execution_time_seconds": execution_time
        }), 200
        
    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        logging.error(f"Failed to create stop flag file: {str(e)}")
        logging.error("=== STOP OPERATION FAILED ===")
        return jsonify({
            "status": "error",
            "message": f"Failed to send stop signal: {str(e)}",
            "execution_time_seconds": execution_time
        }), 500

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current system status"""
    try:
        # Check tmux sessions
        sessions = {
            "server": controller.check_tmux_session("server"),
            "testing": controller.check_tmux_session("testing"),
            "upload": controller.check_tmux_session("upload")
        }
        
        # Check camera statuses
        cameras = {
            "source": controller.get_camera_status(controller.source_ip),
            "sinks": {}
        }
        
        for sink_ip in controller.sink_ips:
            cameras["sinks"][sink_ip] = controller.get_camera_status(sink_ip)
        
        # Check if stop flag exists
        stop_flag_exists = os.path.exists(STOP_FLAG_FILE)
        
        return jsonify({
            "status": controller.capture_status,
            "current_track_id": controller.get_track_id(),
            "sessions": sessions,
            "cameras": cameras,
            "stop_flag_exists": stop_flag_exists,
            "last_error": controller.last_error,
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logging.error(f"Failed to get status: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to get status: {str(e)}"
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/api/temperature', methods=['GET'])
def get_temperatures():
    """Get temperature of source and all sink Pi's"""
    try:
        temperatures = {}
        
        # Get source Pi temperature (local)
        try:
            source_temp_result = subprocess.run(
                ["vcgencmd", "measure_temp"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if source_temp_result.returncode == 0:
                temp_output = source_temp_result.stdout.strip()
                if temp_output.startswith("temp="):
                    temp_value = temp_output.replace("temp=", "").replace("'C", "")
                    temperatures["source"] = {
                        "ip": controller.source_ip,
                        "temperature": float(temp_value),
                        "unit": "°C"
                    }
                else:
                    temperatures["source"] = {
                        "ip": controller.source_ip,
                        "temperature": None,
                        "error": "Invalid temperature format"
                    }
            else:
                temperatures["source"] = {
                    "ip": controller.source_ip,
                    "temperature": None,
                    "error": "Failed to get temperature"
                }
        except Exception as e:
            temperatures["source"] = {
                "ip": controller.source_ip,
                "temperature": None,
                "error": str(e)
            }
        
        # Get sink Pi temperatures
        temperatures["sinks"] = {}
        for sink_ip in controller.sink_ips:
            temp_value = controller.get_pi_temperature(sink_ip)
            if temp_value is not None:
                temperatures["sinks"][sink_ip] = {
                    "temperature": temp_value,
                    "unit": "°C"
                }
            else:
                temperatures["sinks"][sink_ip] = {
                    "temperature": None,
                    "error": "Failed to get temperature"
                }
        
        return jsonify({
            "status": "success",
            "temperatures": temperatures,
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to get temperatures: {str(e)}"
        }), 500

@app.route('/', methods=['GET'])
def root():
    """API root endpoint with available endpoints"""
    return jsonify({
        "message": "Capture System API",
        "endpoints": {
            "POST /api/prepare": "Prepare cameras",
            "POST /api/start": "Start capture",
            "POST /api/stop": "Stop capture",
            "GET /api/status": "Get system status",
            "GET /api/health": "Health check"
        },
        "timestamp": datetime.now().isoformat()
    }), 200

if __name__ == '__main__':
    logging.info("Starting Capture System API Server...")
    logging.info("Available endpoints:")
    logging.info("  POST /api/prepare - Prepare cameras")
    logging.info("  POST /api/start   - Start capture")
    logging.info("  POST /api/stop    - Stop capture")
    logging.info("  GET  /api/status  - Get system status")
    logging.info("  GET  /api/health  - Health check")
    logging.info(f"Server will be available at: http://0.0.0.0:5000")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000, debug=False)
