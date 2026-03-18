import os

def ensure_directories_exist(directories):
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        else:
            print(f"Directory already exists: {directory}")

# Example usage:
dirs = [
    "/home/pi/source_code/ready_to_upload_source_content",
    "/home/pi/source_code/temporary_metadata",
    "/home/pi/source_code/upload_staged_files",
    "/home/pi/source_code/temporary_videos",
    "/home/pi/source_code/variable_files",
    "/home/pi/source_code/streamed_packets",
]


ensure_directories_exist(dirs)
