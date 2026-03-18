import boto3
import os
from env_utils import load_env_file, require_env

def sync_selected_folders(bucket_name, s3_folder, local_dir, access_key, secret_key, region_name, selected_folders):
    """
    Syncs files from the S3 bucket for only the selected camera folders.

    Parameters:
      bucket_name (str): The S3 bucket name.
      s3_folder (str): The S3 prefix (e.g., '3d_vision_testing/').
      local_dir (str): The local directory to store files.
      access_key (str): AWS access key.
      secret_key (str): AWS secret key.
      region_name (str): AWS region.
      selected_folders (list): List of camera folder names (e.g., ['1915', '1916']).
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=access_key,
                             aws_secret_access_key=secret_key,
                             region_name=region_name)

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_folder)

    for page in pages:
        if "Contents" not in page:
            print("No contents found in the specified S3 folder.")
            return

        for obj in page["Contents"]:
            key = obj["Key"]
            # Skip if the key is a directory placeholder
            if key.endswith('/'):
                continue

            parts = key.split('/')
            # Check if the key has at least three parts: [prefix, date, camera, ...]
            if len(parts) < 3:
                continue

            # The camera folder is expected to be the third element
            camera_folder = parts[2]
            if camera_folder not in selected_folders:
                print(f"camera folder {camera_folder} not in {selected_folders}")
                continue

            # Create a relative path (e.g., "2025-03-18/1915/sink_1/filename")
            relative_path = os.path.relpath(key, s3_folder)
            local_file_path = os.path.join(local_dir, relative_path)
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            if not os.path.exists(local_file_path):
                print(f"Downloading {key} to {local_file_path}...")
                s3_client.download_file(bucket_name, key, local_file_path)
            else:
                print(f"{local_file_path} already exists. Skipping download.")

if __name__ == "__main__":
    load_env_file()
    require_env("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")

    bucket_name = os.getenv("S3_BUCKET", "gt-cctv-prod")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("AWS_REGION")
    # Keep these as script parameters / defaults (not secrets)
    s3_folder = os.getenv("S3_FOLDER", "3d_vision_testing/")
    local_dir = os.getenv("LOCAL_DIR", "./3d_vision_testing")
    os.makedirs(local_dir, exist_ok=True)

    # Input selected folders
    selected_folders = []
    range_of_folders_to_download = [3672, 3900]

    for i in range(range_of_folders_to_download[0], range_of_folders_to_download[1]):
        folder_to_download = f"{i}"
        selected_folders.append(folder_to_download)

    sync_selected_folders(bucket_name, s3_folder, local_dir, access_key, secret_key, region_name, selected_folders)
