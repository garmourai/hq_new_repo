import os
import time
import ffmpeg
import logging
import boto3
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from env_utils import load_env_file, require_env

# Configuration
WATCH_SOURCE_DIR = "/home/pi/upload/source"
WATCH_SINK_DIR = "/home/pi/upload/sink"
load_env_file()
require_env("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ACCOUNT_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_SOURCE_ROOT = '3d_vision_testing/source/28_Feb'
S3_KEY_SINK_ROOT = '3d_vision_testing/sink/28_Feb'
CHECK_INTERVAL = 60  # Time interval to check internet (seconds)
MAX_WORKERS = 1  # Number of parallel uploads
FILE_STABILITY_CHECK_TIME = 5  # Seconds to wait and check file stability
INTERNET_RETRY_TIME = 60

# Logging setup
logging.basicConfig(
    filename="/var/log/mp4_uploader.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Thread pool for parallel uploads
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

def is_connected():
    """Check if the system has a stable internet connection."""
    while True:
        try:
            response = requests.get("https://www.google.com", timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            # return False
            logging.warning(f"No internet connection. Retrying...")
            time.sleep(INTERNET_RETRY_TIME)

def is_file_stable(file_path):
    """Check if the file has stopped growing."""
    previous_size = -1
    while True:
        current_size = os.path.getsize(file_path)
        if current_size == previous_size:
            return True  # File is stable
        previous_size = current_size
        time.sleep(FILE_STABILITY_CHECK_TIME)

def upload_file(filepath, s3key, chunk_size=10_485_760):
    """Upload a file to S3 asynchronously."""
    if not os.path.exists(filepath):
        logging.warning(f"File {file_path} no longer exists. Skipping.")
        return

    if not is_file_stable(filepath):
        logging.warning(f"File {file_path} is still being written. Skipping for now.")
        return

    if not is_connected():
        logging.warning(f"No internet connection. Skipping {filepath} for now.")
        return  # File will remain and be retried when detected again

    try:
        logging.info(f"Preparing to upload large file {filepath} to {s3key}")
        # Create a multipart upload
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID,
            region_name=AWS_ACCOUNT_REGION
        )

        s3_client = session.client('s3')

        # Initiate multipart upload
        multipart_upload = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3key
        )
        upload_id = multipart_upload['UploadId']

        # Track uploaded parts
        parts = []

        # Open the file and start uploading parts
        with open(filepath, 'rb') as f:
            part_number = 1

            while True:
                # Read a chunk of the file
                data = f.read(chunk_size)

                if not data:
                    # Reached end of file
                    break

                # Upload this part
                part = s3_client.upload_part(
                    Bucket=S3_BUCKET,
                    Key=s3key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )

                # Store the part info
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['ETag']
                })

                logging.info(f"Uploaded part {part_number}")
                part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        logging.info(f"Successfully uploaded large file {filepath}")

    except Exception as e:
        logging.error(f"Error uploading large file: {e}")

        # Attempt to abort the multipart upload in case of failure
        try:
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3key,
                UploadId=upload_id
            )
        except:
            pass

        raise

def process_file(filepath, s3_key_root):
    filename = os.path.basename(filepath)
    root_dir = os.path.dirname(filepath)
    to_delete_filepaths = [filepath]
    logging.info(f"Processing {filename}...")

    # Upload the h264 video to AWS
    slot_time = filename.split(':')[0] + ':00'
    s3_vid_key = f"{s3_key_root}/{slot_time}/{filename}"
    upload_file(filepath, s3_vid_key)

    # Upload metadata to AWS if generated
    filename_json = filename[:-4] + 'json'
    filepath_json = os.path.join(root_dir, filename_json)
    if os.path.exists(filepath_json):
        logging.info(f"Detected metadata file {filename_json}")
        to_delete_filepaths.append(filepath_json)
        s3_timestamps_key = f"{s3_key_root}/{slot_time}/{filename_json}"
        upload_file(filepath_json, s3_timestamps_key)

    # Upload offset file
    filename_offsets = 'offsets.json'
    filepath_offsets = os.path.join(root_dir, filename_offsets)
    if os.path.exists(filepath_offsets):
        logging.info(f"Detected offset file {filename_offsets}")
        s3_offsets_key = f"{s3_key_root}/{filename_offsets}"
        upload_file(filepath_offsets, s3_offsets_key)

    # Delete files after upload
    for path in to_delete_filepaths:
        os.remove(path)

def process_existing_files():
    """Check and upload existing .h264 and  .mp4 files in the watch directory."""
    for watch_dir, s3_key_root in [(WATCH_SOURCE_DIR, S3_KEY_SOURCE_ROOT), (WATCH_SINK_DIR, S3_KEY_SINK_ROOT)]:
        logging.info(f"Scanning for existing .h264 files in {watch_dir}...")
        for file_path in Path(watch_dir).glob("*.h264"):
            logging.info(f"Found existing file: {file_path}")
            executor.submit(process_file, str(file_path), s3_key_root)

class UploadHandler(FileSystemEventHandler):
    def __init__(self, s3_key_root):
        super().__init__()
        self.s3_key_root = s3_key_root

    """Handle new .h264 files in the directory."""
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".h264"):
            logging.info(f"New file detected: {event.src_path}")
            executor.submit(process_file, event.src_path, self.s3_key_root)  # Run in parallel

if __name__ == "__main__":
    logging.info("Starting MP4 Uploader Daemon with Parallel Uploads")

    # Process existing .mp4 files before starting the watcher
    process_existing_files()
    
    # Watchdog setup
    observer = Observer()
    observer.schedule(UploadHandler(S3_KEY_SOURCE_ROOT), WATCH_SOURCE_DIR, recursive=False)
    observer.schedule(UploadHandler(S3_KEY_SINK_ROOT), WATCH_SINK_DIR, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
