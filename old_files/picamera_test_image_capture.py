import time
from picamera2 import Picamera2, Preview

picam2 = Picamera2()

# Create configuration
camera_config = picam2.create_preview_configuration()
picam2.configure(camera_config)

# Manually set exposure and gain (disable auto exposure)
picam2.set_controls({
    "AeEnable": False,           # Disable auto-exposure
    "ExposureTime": 10000,       # In microseconds (20ms = 1/50s)
    "AnalogueGain": 3.0          # Increase gain (higher = brighter, but more noise)
})

picam2.start_preview(Preview.NULL)
picam2.start()
time.sleep(2)  # Allow camera to settle
picam2.capture_file("test_py.jpg")
