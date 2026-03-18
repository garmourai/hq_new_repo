import time
import subprocess

import RPi.GPIO as GPIO
import cv2
import typer
from picamera2 import Picamera2, Preview, MappedArray
from picamera2.encoders import H264Encoder

import os
import logging
import threading
import ffmpeg
import pytz
import sys
import boto3
import requests
import uuid
from env_utils import load_env_file, require_env

from datetime import datetime
from pathlib import Path

# Define file upload configuration
SLOT_DATETIME_FMT = '%Y-%m-%d_%H:%M'
FILENAME_DATETIME_FMT = '%Y-%m-%d_%H:%M:%S:%f'

LOG_FORMATTER = logging.Formatter("%(asctime)s  ^`^t %(name)s  ^`^t %(levelname)s  ^`^t %(message)s")

load_env_file()
require_env("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ACCOUNT_REGION = os.getenv("AWS_REGION")

S3_BUCKET = os.getenv("S3_BUCKET")

FILEPATH = '/home/pi/videos'
LOGPATH = '/home/pi/videos'
TIMESTAMPSPATH = '/home/pi/timestamps'

app = typer.Typer()

# Configure the GPIO Pins for communication
INPUT_PIN = 17  # Trigger pin
CLONE_PIN = 23  # Mirror trigger pin
GPIO.setmode(GPIO.BCM)
GPIO.setup(INPUT_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
GPIO.setup(CLONE_PIN, GPIO.OUT)

# Config of overlay timestamp images
colour = (0, 255, 0)
origin = (0, 30)
font = cv2.FONT_HERSHEY_SIMPLEX
scale = 0.6
thickness = 1


def apply_timestamp(request):
    timestamp = time.strftime("%Y-%m-%d %X")
    with MappedArray(request, "main") as m:
        cv2.putText(m.array, timestamp, origin, font, scale, colour, thickness, cv2.LINE_AA)

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(LOG_FORMATTER)
   return console_handler

def get_file_handler(logfile):
   file_handler = logging.FileHandler(logfile)
   file_handler.setFormatter(LOG_FORMATTER)
   return file_handler

def get_logger(logger_name, logfile):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler(logfile))
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger

# Initialize the Camera
# picam2 = Picamera2()
# camera_config = picam2.create_preview_configuration()
# video_config = picam2.create_video_configuration()
# encoder = H264Encoder(bitrate=10000000)

logpath = f"{LOGPATH}/3d_vision_testing"
logfile = f"{logpath}/pi_10m_long.log"
Path(logpath).mkdir(parents=True, exist_ok=True)
pi_logger = get_logger('pi', logfile)

def convert_datetime_to_slot_format(date_time):
    date_time = date_time.replace(minute=0)
    return date_time.strftime(SLOT_DATETIME_FMT)

def convert_datetime_to_filename_format(date_time):
    return date_time.strftime(FILENAME_DATETIME_FMT)

def convert_h264_to_mp4(input_file: str, output_file: str, framerate: int):
    """
    Converts an H.264 file to MP4 using the ffmpeg-python library.

    Args:
        input_file (str): Path to the input H.264 file.
        output_file (str): Path to the output MP4 file.
        framerate (int): Frame rate of the video.
    """
    try:
        # Build and execute the FFmpeg command using the ffmpeg-python library
        ffmpeg.input(input_file, framerate=framerate).output(output_file, c="copy").run(overwrite_output=True)
        print(f"Conversion successful: {output_file}")
    except ffmpeg.Error as e:
        print(f"Error during conversion: {e.stderr.decode('utf-8')}")

def delete_files(filepaths):
    for filepath in filepaths:
        # pi_logger.info(f"Deleting file: {filepath}")
        os.remove(filepath)
        # pi_logger.info(f"File Deleted: {filepath}")

def upload_file(filepath, s3key, chunk_size=10_485_760):
    try:
        # pi_logger.info(f"Preparing to upload large file {filepath} to {s3key}")

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

                # pi_logger.info(f"Uploaded part {part_number}")
                part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        # pi_logger.info(f"Successfully uploaded large file {filepath}")

    except Exception as e:
        pi_logger.error(f"Error uploading large file: {e}")

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

def post_process(thread_id, time_now, filename):
    file_path_h264 = os.path.join(FILEPATH, f'{filename}.h264')
    file_path_mp4 = os.path.join(FILEPATH, f'{filename}.mp4')
    convert_h264_to_mp4(file_path_h264, file_path_mp4, 30)

    slot_time = convert_datetime_to_slot_format(time_now)
    s3key = f"3d_vision_testing/source/30s_long/{slot_time}/{filename}.mp4"
    upload_file(file_path_mp4, s3key)

    delete_files([file_path_h264, file_path_mp4])

def configure_img_camera():
    typer.echo("Configuring Camera...")
    picam2.configure(camera_config)
    picam2.pre_callback = apply_timestamp
    picam2.start_preview(Preview.NULL)
    picam2.start()
    time.sleep(2)
    typer.echo("Camera Configured!")


def configure_vid_camera():
    typer.echo("Configuring Camera...")
    picam2.configure(video_config)
    # print(picam2.capture_metadata()["FrameDuration"])
    time.sleep(2)
    typer.echo("Camera Configured!")


@app.command()
def master():
    """
    Configure this device to run as the Master
    """
    # configure_img_camera()
    configure_vid_camera()
    typer.echo("Master camera daemon initialized.")
    try:
        GPIO.output(CLONE_PIN, GPIO.HIGH)

        one_shot_video()

        GPIO.output(CLONE_PIN, GPIO.LOW)
        time.sleep(1 / 30)
    except KeyboardInterrupt:
        GPIO.cleanup()
    # end while loop
    GPIO.cleanup()


@app.command()
def slave():
    """
    Configure this device to run as the Slave
    """
    # configure_img_camera()
    # configure_vid_camera()
    video_capture_count = 0
    missed_high_signal_count = 0
    missed_low_signal_count = 0
    typer.echo("Slave camera daemon initialized. Waiting for signal...")
    try:
        # while True:
        for i in range(100):
            pi_logger.info(f"Capture no {video_capture_count + 1}")
            # pi_logger.info("Waiting for HIGH signal from master")
            # typer.echo("Waiting for HIGH signal from master")
            # high_wait_start = time.time()
            # GPIO.wait_for_edge(INPUT_PIN, GPIO.RISING)
            # pi_logger.info("Got HIGH signal from master")
            # typer.echo("Got HIGH signal from master")

            # pi_logger.info("Sending HIGH signal to sink")
            # typer.echo("Sending HIGH signal to sink")
            # GPIO.output(CLONE_PIN, GPIO.HIGH)

            # if(video_capture_count > 0 and time.time() - high_wait_start > 30):
            #     pi_logger.info(f"Missed a HIGH signal from master at capture no {video_capture_count + 1}")
            #     missed_high_signal_count += 1

            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            filename = convert_datetime_to_filename_format(now)
            filename_path = os.path.join(FILEPATH, f'{filename}.h264')
            timestamps_path = os.path.join(TIMESTAMPSPATH, f'{filename}.txt')
            # one_shot_video()
            pi_logger.info("waiting for 60 sec")
            time.sleep(60)
            # source_video()
            
            video_capture(filename_path, timestamps_path)
            video_capture_count += 1
            
            # t = threading.Thread(target=post_process, args=(1, now, filename))
            # t.start()

            # pi_logger.info("Waiting for LOW signal from master")
            # typer.echo("Waiting for LOW signal from master")
            # low_wait_start = time.time()
            # GPIO.wait_for_edge(INPUT_PIN, GPIO.FALLING)
            # pi_logger.info("Got LOW signal from master")
            # typer.echo("Got LOW signal from master")
            # pi_logger.info("Sending LOW signal to sink")
            # typer.echo("Sending LOW signal to sink")
            # GPIO.output(CLONE_PIN, GPIO.LOW)

            # if time.time() - low_wait_start > 30:
            #     pi_logger.info(f"Missed a LOW signal from master at capture no {video_capture_count + 1}")
            #     missed_low_signal_count += 1
            pi_logger.info("")
            pi_logger.info("------------------------------------------------------------------------------")
            pi_logger.info("")

        pi_logger.info(f"Number of HIGH signal misses: {missed_high_signal_count}")
        pi_logger.info(f"Number of LOW signal misses: {missed_low_signal_count}")
        GPIO.cleanup()

    except KeyboardInterrupt:
        pi_logger.info(f"Number of HIGH signal misses: {missed_high_signal_count}")
        pi_logger.info(f"Number of LOW signal misses: {missed_low_signal_count}")
        GPIO.cleanup()

@app.command()
def test():
    for i in range(6):
        filename = f"test_{i}.h264"
        filepath = os.path.join(FILEPATH, filename)
        video_capture(filepath)

@app.command()
def source_video():
    output = f'vid_{time.time_ns() // 1_000_000}.h264'
    subprocess.run(["rpicam-vid", "--frames", "9000", "-o", output, "--width", "1280", "--height", "720"])

@app.command()
def one_shot():
    """
    Take a single picture in standalone mode
    """
    configure_img_camera()
    picam2.capture_file(f'img_{time.time_ns() // 1_000_000}.jpg')
    typer.echo(f"Image img_{time.time_ns() // 1_000_000}.jpg captured in Standalone mode")

@app.command()
def sink_one_shot_video():
    try:
        configure_vid_camera()
        while True:
            typer.echo("Waiting for source signal")
            output = f'vid_{time.time_ns() // 1_000_000}.h264'
            picam2.start_recording(encoder, output)
            time.sleep(0)
            picam2.stop_recording()
            typer.echo(f"Created video {output}")
            time.sleep(5)
    except KeyboardInterrupt:
        GPIO.cleanup()

@app.command()
def video_capture(output, timestamp_path):
    os.system(f"rpicam-vid --frames 18000 -o {output} --save-pts {timestamp_path}") 

    # os.system(f"rpicam-vid -t 600s -o {output}")
    # os.system(f"rpicam-vid --level 4.2 --framerate 30 --width 1280 --height 720 --save-pts {output[:-5]}_timestamp.pts -o {output} -t 600s --denoise cdn_off -n")
    # picam2.start_recording(encoder, output)
    # time.sleep(600)
    # picam2.stop_recording()
    typer.echo(f"Created video {output}")

@app.command()
def one_shot_video():
    """
    Take a single video of 10 seconds in standalone mode
    """
    output = f'30s_tests/vid_{time.time_ns() // 1_000_000}.h264'
    picam2.start_recording(encoder, output)
    time.sleep(30)
    picam2.stop_recording()
    typer.echo(f"Created video {output}")


if __name__ == "__main__":
    if not typer.main.get_command(app):
        typer.echo("Available commands: master, slave, one-shot, one-shot-video, sink-one-shot-video, test")
    else:
        app()
