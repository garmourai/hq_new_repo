import os
import re
import shutil
import time
import logging
import subprocess
import json
import yaml
from datetime import datetime
import boto3
# from pymongo import MongoClient
# from pymongo.errors import ConnectionFailure

# -------------------------------
# Environment (.env) loading
# -------------------------------
def _load_env_file(dotenv_path: str) -> None:
    """
    Minimal .env loader (KEY=VALUE lines, supports comments and blank lines).
    Does not override already-set environment variables.
    """
    try:
        if not os.path.exists(dotenv_path):
            return
        with open(dotenv_path, "r") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                os.environ.setdefault(key, value)
    except Exception as e:
        logging.warning(f"Could not load env file {dotenv_path}: {e}")


_load_env_file(os.path.join(os.path.dirname(__file__), ".env"))

# -------------------------------
# Configuration
# -------------------------------
BASE_DIR = "/home/pi/source_code/ready_to_upload_source_content"
UPLOAD_BASE_DIR = "/home/pi/source_code/upload_staged_files"
# State file lives inside upload_staged_files
STATE_DIR = "/home/pi/source_code/upload_staged_files"
INFO_YAML_PATH = "/home/pi/source_code/variable_files/info.yaml"
LOGS_DIR = "/home/pi/source_code/logs"

# AWS S3 configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_BASE_KEY = os.getenv("S3_BASE_KEY", "")

_missing_env = [k for k, v in {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
    "S3_BUCKET": S3_BUCKET,
    "S3_BASE_KEY": S3_BASE_KEY,
}.items() if not v]
if _missing_env:
    raise RuntimeError(
        "Missing required environment variables for upload_files.py: "
        + ", ".join(_missing_env)
        + ". Set them in /home/pi/source_code/.env or in the environment."
    )

# MongoDB Configuration
# MONGO_URI = "mongodb+srv://gametheoryadmin:gametheory123@gametheory.soidl53.mongodb.net/?retryWrites=true&w=majority&appName=GameTheory"
# MONGO_DB_NAME = "my_database"
# MONGO_COLLECTION_NAME = "capture_data"

# Establish MongoDB Connection
# try:
#     mongo_client = MongoClient(MONGO_URI)
#     db = mongo_client[MONGO_DB_NAME]
#     collection = db[MONGO_COLLECTION_NAME]
#     # The ismaster command is cheap and does not require auth.
#     mongo_client.admin.command('ismaster')
#     logging.info("MongoDB connection successful.")
# except ConnectionFailure as e:
#     logging.error(f"Could not connect to MongoDB: {e}")
#     mongo_client = None
#     db = None
#     collection = None


def get_mac_address():
    """Get the MAC address of the Pi"""
    try:
        # Get MAC address of the first network interface
        result = subprocess.run(['cat', '/sys/class/net/eth0/address'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip().replace(':', '-')
        
        # Fallback to wlan0 if eth0 doesn't exist
        result = subprocess.run(['cat', '/sys/class/net/wlan0/address'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip().replace(':', '-')
            
        return "unknown_mac"
    except Exception as e:
        logging.error(f"Error getting MAC address: {e}")
        return "unknown_mac"

KEY_PREFIX = get_mac_address()

CHUNK_SIZE = 10_485_760  # 10 MB
MAX_RETRIES = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------
# Utility Functions
# -------------------------------
def ensure_directories(*dirs):
    for d in dirs:
        os.makedirs(d, exist_ok=True)

def extract_x(filename):
    match = re.search(r'_(\d+)\.(h264|json)$', filename)
    return int(match.group(1)) if match else None

def extract_date_from_filename(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    return match.group(1) if match else datetime.now().strftime("%Y-%m-%d")

def parse_directory_info(directory_string):
    """Parses the directory string into center, sport, and court."""
    try:
        parts = directory_string.split('_')
        if len(parts) >= 3:
            return {
                "center": parts[0],
                "sport": parts[1],
                "court": "_".join(parts[2:])
            }
    except Exception as e:
        logging.error(f"Could not parse directory string '{directory_string}': {e}")
    return {"center": "unknown", "sport": "unknown", "court": "unknown"}


def load_directory_from_yaml():
    try:
        with open(INFO_YAML_PATH, 'r') as file:
            data = yaml.safe_load(file)
            directory = data.get('directory', 'default')
            logging.info(f"Loaded directory from YAML: {directory}")
            return directory
    except Exception as e:
        logging.error(f"Error loading directory from {INFO_YAML_PATH}: {e}")
        return 'default'

def load_unique_id_from_yaml():
    try:
        with open(INFO_YAML_PATH, 'r') as file:
            data = yaml.safe_load(file)
            unique_id = data.get('unique_id', None)
            logging.info(f"Loaded unique_id from YAML: {unique_id}")
            return unique_id
    except Exception as e:
        logging.error(f"Error loading unique_id from {INFO_YAML_PATH}: {e}")
        return None

def build_s3_key(filename, x):
    date_str = extract_date_from_filename(filename)
    directory = load_directory_from_yaml()
    s3_key = f"{S3_BASE_KEY}/{directory}/{x}/{KEY_PREFIX}/{filename}"
    logging.info(f"Built S3 key: {s3_key}")
    return s3_key

def build_offset_s3_key(offset_filename, x):
    date_str = extract_date_from_filename(offset_filename)
    directory = load_directory_from_yaml()
    s3_key = f"{S3_BASE_KEY}/{directory}/{x}/{offset_filename}"
    logging.info(f"Built Offset S3 key: {s3_key}")
    return s3_key

def move_files(src_dir, dst_dir, filename):
    src_path = os.path.join(src_dir, filename)
    dst_path = os.path.join(dst_dir, filename)
    logging.info(f"📦 Moving file: {filename}")
    logging.info(f"   From: {src_path}")
    logging.info(f"   To: {dst_path}")
    
    try:
        shutil.move(src_path, dst_path)
        logging.info(f"✅ Successfully moved {filename}")
        return True
    except Exception as e:
        logging.error(f"❌ Error moving {filename}: {e}")
        return False

# def log_capture_to_mongodb(metadata, s3_key):
#     """Logs capture details to MongoDB after a successful upload."""
#     logging.info(f"💾 Starting MongoDB logging for track {metadata.get('x')}")
#     
#     if collection is None:
#         logging.error("❌ MongoDB collection not available. Skipping log.")
#         return
# 
#     try:
#         directory_string = load_directory_from_yaml()
#         dir_info = parse_directory_info(directory_string)
#         unique_id = load_unique_id_from_yaml()
#         
#         logging.info(f"📋 Directory info: {dir_info}")
#         logging.info(f"🆔 Unique ID: {unique_id}")
# 
#         doc = {
#             "track_index": metadata.get("x"),
#             "center_name": dir_info.get("center"),
#             "sport": dir_info.get("sport"),
#             "court": dir_info.get("court"),
#             "unique_id": unique_id,
#             "camera_mac": KEY_PREFIX,
#             "aws_bucket": S3_BUCKET,
#             "aws_path": s3_key,
#             "metadata": metadata,
#             "timestamp": datetime.utcnow()
#         }
#         
#         logging.info(f"📝 MongoDB document prepared: {doc}")
#         
#         # Use update_one with upsert to avoid duplicate entries for the same video file
#         result = collection.update_one(
#             {"metadata.files.video": metadata["files"]["video"]},
#             {"$set": doc},
#             upsert=True
#         )
#         
#         if result.upserted_id:
#             logging.info(f"✅ Successfully inserted new capture record for track {metadata.get('x')} to MongoDB")
#         else:
#             logging.info(f"✅ Successfully updated existing capture record for track {metadata.get('x')} to MongoDB")
# 
#     except Exception as e:
#         logging.error(f"❌ Failed to log capture to MongoDB: {e}")

# -------------------------------
# S3 Multipart Upload with Progress
# -------------------------------
def upload_file(filepath, s3key, chunk_size=CHUNK_SIZE):
    logging.info(f"📤 Starting upload process for: {os.path.basename(filepath)}")
    logging.info(f"   Local path: {filepath}")
    logging.info(f"   S3 key: {s3key}")
    print(f"📤 Starting upload: {os.path.basename(filepath)}")
    print(f"   S3 key: {s3key}")
    
    if not os.path.exists(filepath):
        logging.warning(f"❌ File {filepath} no longer exists. Skipping.")
        return False
        
    try:
        file_size = os.path.getsize(filepath)
        logging.info(f"📊 File size: {file_size:,} bytes ({file_size / (1024*1024):.2f} MB)")
        
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3_client = session.client('s3')
        logging.info(f"🔗 AWS S3 client initialized for region: {AWS_REGION}")
        
        # Calculate total chunks
        total_chunks = (file_size + chunk_size - 1) // chunk_size
        logging.info(f"🚀 Starting multipart upload for {os.path.basename(filepath)} ({file_size:,} bytes, {total_chunks} parts)")
        
        # Initiate multipart upload
        multipart_upload = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3key
        )
        upload_id = multipart_upload['UploadId']
        parts = []
        
        with open(filepath, 'rb') as f:
            part_number = 1
            while True:
                start_time = time.time()
                data = f.read(chunk_size)
                if not data:
                    break
                    
                # Upload part with progress tracking
                part_size = len(data)
                logging.info(f"Uploading part {part_number}/{total_chunks} ({part_size} bytes)...")
                
                part = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                
                upload_time = time.time() - start_time
                speed = part_size / (1024 * 1024) / upload_time  # MB/s
                logging.info(
                    f"Completed part {part_number}/{total_chunks} "
                    f"(ETag: {part['ETag']}, "
                    f"Speed: {speed:.2f} MB/s)"
                )
                
                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                part_number += 1
        
        # Complete the upload
        logging.info(f"🏁 Completing multipart upload with {len(parts)} parts...")
        s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        logging.info(f"✅ Successfully completed upload of {os.path.basename(filepath)} to S3")
        logging.info(f"🗑️  Removing local file: {filepath}")
        print(f"✅ Upload completed: {os.path.basename(filepath)}")
        os.remove(filepath)
        logging.info(f"🎉 Upload process completed successfully for {os.path.basename(filepath)}")
        print(f"🎉 Upload process completed successfully for {os.path.basename(filepath)}")
        return True
        
    except Exception as e:
        logging.error(f"Error during multipart upload: {e}")
        try:
            if 'upload_id' in locals():
                s3_client.abort_multipart_upload(
                    Bucket=S3_BUCKET,
                    Key=s3key,
                    UploadId=upload_id
                )
                logging.info("Aborted incomplete multipart upload")
        except Exception as abort_e:
            logging.error(f"Error aborting upload: {abort_e}")
        return False
# -------------------------------
# State Persistence Functions
# -------------------------------
def load_state():
    state = {}
    ensure_directories(STATE_DIR)
    state_file = os.path.join(STATE_DIR, "transferred_files.json")
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                state = json.load(f)
            logging.info(f"Loaded state from {state_file}")
        except Exception as e:
            logging.error(f"Error loading state from {state_file}: {e}")
    return state

def save_state(state):
    ensure_directories(STATE_DIR)
    timestamp = int(datetime.now().timestamp())
    state["timestamp"] = timestamp
    state_file = os.path.join(STATE_DIR, "transferred_files.json")
    try:
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)
        logging.info(f"State saved to {state_file}")
    except Exception as e:
        logging.error(f"Error saving state: {e}")

def upload_state_file():
    state_file = os.path.join(STATE_DIR, "transferred_files.json")
    date_str = datetime.now().strftime("%Y-%m-%d")
    directory = load_directory_from_yaml()
    s3_key = f"{S3_BASE_KEY}/{directory}/transferred_files_{KEY_PREFIX}.json"
    try:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3 = session.client('s3')
        logging.info(f"Uploading state file {state_file} to S3 with key {s3_key}")
        s3.upload_file(state_file, S3_BUCKET, s3_key)
        logging.info("State file uploaded successfully to S3.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload state file to S3: {e}")
        return False

# -------------------------------
# Process a Video: Staging & Upload
# -------------------------------
def process_offset_for_x(x):
    staging_dir = os.path.join(UPLOAD_BASE_DIR, str(x))
    if not os.path.exists(staging_dir):
        return False
    # Process offset files
    offset_files = [f for f in os.listdir(staging_dir)
                    if os.path.isfile(os.path.join(staging_dir, f)) and f.startswith("offset_") and f.endswith(".json")]
    for offset_file in offset_files:
        offset_path = os.path.join(staging_dir, offset_file)
        if upload_file(offset_path, build_offset_s3_key(offset_file, x)):
            logging.info(f"Offset file {offset_file} for x={x} uploaded successfully.")
    # Process check_mismatch.json
    mismatch_file = "check_mismatch.json"
    mismatch_path = os.path.join(staging_dir, mismatch_file)
    if os.path.exists(mismatch_path) and os.path.isfile(mismatch_path):
        if upload_file(mismatch_path, build_offset_s3_key(mismatch_file, x)):
            logging.info(f"Mismatch file {mismatch_file} for x={x} uploaded successfully.")
    # Process check_restart.json
    restart_file = "check_restart.json"
    restart_path = os.path.join(staging_dir, restart_file)
    if os.path.exists(restart_path) and os.path.isfile(restart_path):
        if upload_file(restart_path, build_offset_s3_key(restart_file, x)):
            logging.info(f"Restart file {restart_file} for x={x} uploaded successfully.")
    # Upload state file
    upload_state_file()
    # Cleanup
    for f in [f for f in os.listdir(staging_dir) if f.startswith("offset_") and f.endswith(".json")]:
        os.remove(os.path.join(staging_dir, f))
    if os.path.exists(mismatch_path):
        os.remove(mismatch_path)
    if os.path.exists(restart_path):
        os.remove(restart_path)
    logging.info(f"Offset and mismatch files for x={x} removed from staging.")
    return True

def extract_date_from_logs(log_dir):
    """Extract date from log files with pattern CAPTURE_START_* or similar"""
    if not os.path.exists(log_dir):
        return None
        
    for filename in os.listdir(log_dir):
        if filename.startswith(("CAPTURE_START_", "CAPTURE_SUCCESS_")):
            match = re.search(r'(\d{4}-\d{2}-\d{2})T', filename)
            if match:
                return match.group(1)  # Returns YYYY-MM-DD
    
    return datetime.now().strftime("%Y-%m-%d")

def upload_logs_for_track(x):
    """Upload both app logs and tmux logs for a given track ID"""
    directory = load_directory_from_yaml()

    # Upload app logs (numbered folder like 6413/)
    app_logs_path = os.path.join(LOGS_DIR, str(x))
    if os.path.exists(app_logs_path) and os.path.isdir(app_logs_path):
        date_str = extract_date_from_logs(app_logs_path)
        s3_app_logs_prefix = f"{S3_BASE_KEY}/{directory}/{x}/logs/app_logs/"
        upload_directory_to_s3(app_logs_path, s3_app_logs_prefix)
        logging.info(f"App logs for track {x} uploaded successfully")
    
    # Upload tmux logs (track_ prefixed folder like track_6413/)
    tmux_logs_path = os.path.join(LOGS_DIR, f"track_{x}")
    if os.path.exists(tmux_logs_path) and os.path.isdir(tmux_logs_path):
        date_str = extract_date_from_logs(tmux_logs_path) or extract_date_from_logs(app_logs_path) or datetime.now().strftime("%Y-%m-%d")
        s3_tmux_logs_prefix = f"{S3_BASE_KEY}/{directory}/{x}/logs/tmux_logs/"
        upload_directory_to_s3(tmux_logs_path, s3_tmux_logs_prefix)
        logging.info(f"Tmux logs for track {x} uploaded successfully")

def upload_directory_to_s3(local_dir, s3_prefix):
    """Upload entire directory structure to S3"""
    try:
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3_client = session.client('s3')
        
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_path = os.path.join(root, file)
                # Calculate relative path from the base directory
                relative_path = os.path.relpath(local_path, local_dir)
                s3_key = s3_prefix + relative_path.replace(os.sep, '/')
                
                # Upload file
                s3_client.upload_file(local_path, S3_BUCKET, s3_key)
                logging.info(f"Uploaded log file: {relative_path} to {s3_key}")
                
    except Exception as e:
        logging.error(f"Error uploading directory {local_dir} to S3: {e}")

def process_video(metadata):
    x = metadata["x"]
    metadata_only = metadata.get("metadata_only", False) or metadata["files"].get("video") is None
    video_filename = metadata["files"].get("video")
    json_filename = metadata["files"]["json"]

    logging.info(f"🎬 Processing {'metadata only' if metadata_only else 'video'} for track x={x}")
    logging.info(f"   JSON file: {json_filename}")
    if not metadata_only:
        logging.info(f"   Video file: {video_filename}")

    staging_dir = os.path.join(UPLOAD_BASE_DIR, str(x))
    ensure_directories(staging_dir)
    logging.info(f"📁 Staging directory: {staging_dir}")

    staging_json = os.path.join(staging_dir, json_filename)

    # Check if files are already in staging
    if metadata_only:
        files_ready = os.path.exists(staging_json)
    else:
        staging_video = os.path.join(staging_dir, video_filename)
        files_ready = os.path.exists(staging_video) and os.path.exists(staging_json)

    if files_ready:
        logging.info(f"📋 Files for x={x} already in staging. Skipping move.")
    else:
        # Move files from BASE_DIR to staging
        logging.info(f"📦 Moving files from {BASE_DIR} to {staging_dir}")
        if metadata_only:
            if not move_files(BASE_DIR, staging_dir, json_filename):
                logging.error(f"❌ Failed to move JSON for x={x}")
                return False
        else:
            if not (move_files(BASE_DIR, staging_dir, video_filename) and
                    move_files(BASE_DIR, staging_dir, json_filename)):
                logging.error(f"❌ Failed to move files for x={x}")
                return False
        logging.info(f"✅ Files moved successfully to staging for x={x}")

    # Upload source files
    logging.info(f"☁️  Building S3 keys for x={x}")
    json_s3_key = build_s3_key(json_filename, x)

    if metadata_only:
        logging.info(f"📤 Starting upload of JSON file for x={x}")
        if not upload_file(staging_json, json_s3_key):
            logging.error(f"❌ Failed to upload JSON for x={x}")
            return False
        logging.info(f"✅ Successfully uploaded JSON file for x={x}")
    else:
        video_s3_key = build_s3_key(video_filename, x)
        staging_video = os.path.join(staging_dir, video_filename)
        logging.info(f"📤 Starting upload of video file for x={x}")
        if not (upload_file(staging_video, video_s3_key) and
                upload_file(staging_json, json_s3_key)):
            logging.error(f"❌ Failed to upload files for x={x}")
            return False
        logging.info(f"✅ Successfully uploaded video and JSON files for x={x}")

    # Log to MongoDB on successful upload
    # logging.info(f"💾 Logging capture to MongoDB for x={x}")
    # log_capture_to_mongodb(metadata, video_s3_key)

    # Upload logs for this track ID
    logging.info(f"📋 Uploading logs for track x={x}")
    upload_logs_for_track(x)

    # Process offset and check files
    logging.info(f"🔧 Processing offset and check files for x={x}")
    process_offset_for_x(x)
    
    # Cleanup staging directory if empty
    if os.path.exists(staging_dir) and not os.listdir(staging_dir):
        os.rmdir(staging_dir)
        logging.info(f"🗑️  Staging directory {staging_dir} removed after processing.")
    else:
        logging.info(f"📁 Staging directory {staging_dir} still contains files, keeping it.")
    
    logging.info(f"🎉 Successfully completed processing for track x={x}")
    return True

def retry_uploads_dir(state, verbose=True):
    processed = {}
    log = logging.info if verbose else logging.debug
    log(f"🔄 Starting retry process for upload_staged_files directory: {UPLOAD_BASE_DIR}")

    if not os.path.exists(UPLOAD_BASE_DIR):
        log(f"📁 Upload base directory does not exist: {UPLOAD_BASE_DIR}")
        return processed

    # Get all directories in UPLOAD_BASE_DIR sorted by x value
    x_dirs = sorted([d for d in os.listdir(UPLOAD_BASE_DIR) if os.path.isdir(os.path.join(UPLOAD_BASE_DIR, d))],
                    key=lambda x: int(x) if x.isdigit() else 0)
    log(f"📂 Found {len(x_dirs)} directories to process: {x_dirs}")

    for x_dir in x_dirs:
        x_path = os.path.join(UPLOAD_BASE_DIR, x_dir)
        log(f"📁 Processing directory: {x_dir} (path: {x_path})")

        try:
            x = int(x_dir)
            log(f"🎯 Processing track x={x}")
        except ValueError:
            logging.warning(f"⚠️  Skipping non-numeric directory: {x_dir}")
            continue
            
        files_in_dir = os.listdir(x_path)
        video_files = [f for f in files_in_dir if f.endswith('.h264')]
        json_files = [f for f in files_in_dir if f.endswith('.json') and not f.startswith('offset_') and f not in ['check_mismatch.json', 'check_restart.json']]

        log(f"📄 Found {len(files_in_dir)} total files in directory")
        log(f"🎬 Video files: {video_files}")
        log(f"📋 JSON files: {json_files}")
        
        # Track if we uploaded logs for this x value
        logs_uploaded_for_x = False
        
        for video_file in video_files:
            json_file = next((f for f in json_files if extract_x(f) == x), None)
            
            if not json_file:
                continue
                
            if video_file in state:
                metadata = state[video_file]
                if metadata.get('retries', 0) >= MAX_RETRIES:
                    continue
            else:
                metadata = {
                    'x': x,
                    'files': {
                        'video': video_file,
                        'json': json_file
                    },
                    'state': 0,
                    'retries': 0
                }
            
            logging.info(f"Retrying upload for video '{video_file}' in folder '{x_path}'. Current retries: {metadata.get('retries', 0)}")
            if process_video(metadata):
                metadata['state'] = 2
                metadata['retries'] = 0
                logs_uploaded_for_x = True
                logging.info(f"Successfully processed {video_file} and its JSON from upload_staged_files")
            else:
                metadata['retries'] += 1
                logging.warning(f"Failed to upload '{video_file}' from folder '{x_path}'. Retry count now {metadata['retries']}.")
                if metadata['retries'] >= MAX_RETRIES:
                    metadata['state'] = 'failed'
                    logging.warning(f"Permanently failed {video_file} after {MAX_RETRIES} retries")
            
            state[video_file] = metadata
            save_state(state)
            processed[video_file] = metadata

        # Handle JSON-only files (metadata-only captures: no video, or video already processed)
        for json_file in json_files:
            json_path = os.path.join(x_path, json_file)
            if not os.path.exists(json_path):
                continue
            # Find state entry for this json (key can be video_file or json_file for metadata-only)
            state_entry = next((f for f in state.values() if isinstance(f, dict) and f.get('files', {}).get('json') == json_file), None)
            # Skip only if already successfully uploaded (state==2)
            if state_entry is not None and state_entry.get('state') == 2:
                logging.info(f"Skipping JSON {json_file} - already uploaded successfully")
                continue
            # Retry: upload json (new or previously failed)
            s3_key = build_s3_key(json_file, x)
            logging.info(f"Retrying upload for JSON '{json_file}' in folder '{x_path}' with S3 key '{s3_key}'")
            if upload_file(json_path, s3_key):
                logging.info(f"Successfully uploaded JSON {json_file} for x={x}")
                if state_entry is not None:
                    state_entry['state'] = 2
                    state_entry['retries'] = 0
                    save_state(state)
                processed[json_file] = state_entry or {'x': x, 'files': {'video': None, 'json': json_file}, 'state': 2}
                try:
                    os.remove(json_path)
                    logging.info(f"Removed uploaded JSON from staging: {json_file}")
                except OSError as e:
                    logging.warning(f"Could not remove {json_path} after upload: {e}")
                if not logs_uploaded_for_x:
                    upload_logs_for_track(x)
                    logs_uploaded_for_x = True
            else:
                logging.error(f"Failed to upload JSON {json_file} for x={x}")
                if state_entry is not None:
                    state_entry['retries'] = state_entry.get('retries', 0) + 1
                    save_state(state)
        
        # Upload logs even if no files were processed (in case logs exist but files don't)
        if not logs_uploaded_for_x:
            upload_logs_for_track(x)
        
        # Process offset and check files
        process_offset_for_x(x)
        
        # Delete the x folder if it's empty
        if os.path.exists(x_path) and not os.listdir(x_path):
            os.rmdir(x_path)
            log(f"Upload folder {x_path} removed because it is empty.")
    
    return processed


def process_videos(state, verbose=True):
    processed = {}
    log = logging.info if verbose else logging.debug
    log(f"🔍 Scanning source directory for new videos: {BASE_DIR}")

    if not os.path.exists(BASE_DIR):
        logging.warning(f"⚠️  Source directory does not exist: {BASE_DIR}")
        return processed

    file_list = os.listdir(BASE_DIR)
    sorted_files = sorted(file_list)
    log(f"📄 Found {len(file_list)} files in source directory")

    video_files = [f for f in sorted_files if f.endswith('.h264')]
    json_files = [f for f in sorted_files if f.endswith('.json')]
    log(f"🎬 Found {len(video_files)} video files: {video_files}")
    log(f"📋 Found {len(json_files)} JSON files: {json_files}")
    
    # Set of x values that have a matching .h264 (for video+json pairs)
    video_x_set = {extract_x(f) for f in video_files if extract_x(f) is not None}

    for video_file in sorted_files:
        if video_file.endswith(".h264"):
            logging.info(f"🎬 Processing video file: {video_file}")
            x = extract_x(video_file)
            if x is None:
                logging.warning(f"⚠️  Could not extract track number from {video_file}, skipping")
                continue
            logging.info(f"🎯 Extracted track number x={x} from {video_file}")
            
            json_file = None
            for f in file_list:
                if f.endswith(".json") and extract_x(f) == x:
                    json_file = f
                    break
            if not json_file:
                logging.warning(f"⚠️  No matching JSON file found for {video_file} (x={x}), skipping")
                continue
            logging.info(f"📋 Found matching JSON file: {json_file}")
            
            size_info = {
                "video": os.path.getsize(os.path.join(BASE_DIR, video_file)),
                "json": os.path.getsize(os.path.join(BASE_DIR, json_file)),
            }
            logging.info(f"📊 File sizes - Video: {size_info['video']:,} bytes, JSON: {size_info['json']:,} bytes")
            
            metadata = {
                "x": x,
                "files": {
                    "video": video_file,
                    "json": json_file,
                },
                "size": size_info,
                "state": 0,
                "retries": 0
            }
            logging.info(f"📝 Created metadata for x={x}: {metadata}")
            
            logging.info(f"🚀 Starting video processing for x={x}")
            if process_video(metadata):
                metadata["state"] = 2
                logging.info(f"🗑️  Removing processed files from source directory")
                files_to_remove = [video_file, json_file]
                for fname in files_to_remove:
                    fpath = os.path.join(BASE_DIR, fname)
                    if os.path.exists(fpath):
                        os.remove(fpath)
                        logging.info(f"   Removed: {fname}")
                logging.info(f"✅ Video capture for x={x} processed successfully.")
                state[video_file] = metadata
                save_state(state)
                processed[video_file] = metadata
            else:
                metadata["retries"] += 1
                logging.warning(f"❌ Video processing failed for x={x}, retry count: {metadata['retries']}")
                if metadata["retries"] >= MAX_RETRIES:
                    metadata["state"] = "failed"
                    logging.warning(f"💀 Video capture for {video_file} failed after {metadata['retries']} retries. Discarding.")
                state[video_file] = metadata
                save_state(state)

    # Process standalone JSON files (metadata-only captures: no matching .h264)
    for json_file in json_files:
        if json_file.startswith("offset_") or json_file in ("check_mismatch.json", "check_restart.json"):
            continue
        x = extract_x(json_file)
        if x is None:
            continue
        if x in video_x_set:
            continue  # Already processed as part of video+json pair
        logging.info(f"📋 Processing metadata-only JSON: {json_file} (x={x})")
        size_info = {"video": 0, "json": os.path.getsize(os.path.join(BASE_DIR, json_file))}
        metadata = {
            "x": x,
            "files": {"video": None, "json": json_file},
            "metadata_only": True,
            "size": size_info,
            "state": 0,
            "retries": 0
        }
        if process_video(metadata):
            logging.info(f"🗑️  Removing processed JSON from source directory")
            fpath = os.path.join(BASE_DIR, json_file)
            if os.path.exists(fpath):
                os.remove(fpath)
                logging.info(f"   Removed: {json_file}")
            logging.info(f"✅ Metadata-only capture for x={x} processed successfully.")
            state[json_file] = metadata
            save_state(state)
            processed[json_file] = metadata
        else:
            metadata["retries"] += 1
            if metadata["retries"] >= MAX_RETRIES:
                metadata["state"] = "failed"
                logging.warning(f"💀 Metadata-only {json_file} failed after {metadata['retries']} retries. Discarding.")
            state[json_file] = metadata
            save_state(state)

    return processed

# -------------------------------
# Main Application
# -------------------------------
def main():
    logging.info("=" * 60)
    logging.info("Starting 3D Vision Uploader Service...")
    logging.info(f"Base directory: {BASE_DIR}")
    logging.info(f"Upload directory: {UPLOAD_BASE_DIR}")
    logging.info(f"State directory: {STATE_DIR}")
    logging.info(f"Key prefix: {KEY_PREFIX}")
    logging.info("=" * 60)
    print("=" * 60)
    print("🚀 Starting 3D Vision Uploader Service...")
    print(f"📁 Base directory: {BASE_DIR}")
    print(f"📁 Upload directory: {UPLOAD_BASE_DIR}")
    print(f"📁 State directory: {STATE_DIR}")
    print(f"🔑 Key prefix: {KEY_PREFIX}")
    print("=" * 60)
    
    ensure_directories(BASE_DIR, UPLOAD_BASE_DIR, STATE_DIR)
    state = load_state()
    found_videos = False
    loop_count = 0

    while True:
        try:
            loop_count += 1
            quiet = loop_count % 50 != 0  # Only log/print step details every 50th iteration when idle

            if not quiet:
                logging.info(f"--- Main loop iteration #{loop_count} ---")
                print(f"🔄 Main loop iteration #{loop_count}")

            # First process all existing uploads from the beginning
            if not quiet:
                logging.info("Step 1: Processing existing uploads from upload_staged_files folder...")
                print("📁 Step 1: Processing existing uploads from upload_staged_files folder...")
            retried_videos = retry_uploads_dir(state, verbose=not quiet)
            if not quiet:
                logging.info(f"Retried {len(retried_videos)} files from upload_staged_files folder")
                print(f"📁 Retried {len(retried_videos)} files from upload_staged_files folder")

            # Then process any new videos in the source directory
            if not quiet:
                logging.info("Step 2: Processing new videos from source directory...")
                print("🎬 Step 2: Processing new videos from source directory...")
            processed_videos = process_videos(state, verbose=not quiet)
            if not quiet:
                logging.info(f"Processed {len(processed_videos)} new videos from source directory")
                print(f"🎬 Processed {len(processed_videos)} new videos from source directory")

            # Combine results
            all_processed = {**processed_videos, **retried_videos}

            if not all_processed:
                found_videos = False
                if not quiet:
                    logging.info("No new videos found. Checking again in 2 seconds...")
                    print("⏳ No new videos found. Checking again in 2 seconds...")
            else:
                found_videos = True
                if processed_videos:
                    logging.info(f"✓ New videos processed from source directory: {list(processed_videos.keys())}")
                    print(f"✅ New videos processed from source directory: {list(processed_videos.keys())}")
                if retried_videos:
                    logging.info(f"✓ Retried pending uploads from upload_staged_files folder: {list(retried_videos.keys())}")
                    print(f"✅ Retried pending uploads from upload_staged_files folder: {list(retried_videos.keys())}")
                
            for video_file, metadata in processed_videos.items():
                state[video_file] = metadata
            save_state(state)
            if not quiet:
                logging.info(f"State saved. Total files in state: {len(state)}")
            time.sleep(2)
        except KeyboardInterrupt:
            logging.info("Graceful shutdown initiated.")
            save_state(state)
            break
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()