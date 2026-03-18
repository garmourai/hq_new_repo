import requests
import time
import os
import yaml


def check_stop_flag():
    return os.path.exists("/tmp/capture_stop_flag")


class CameraController:
    def __init__(self, pi_ip):
        self.pi_ip = pi_ip
        self.base_url = f"http://{pi_ip}:8080"

    def start_capture(self):
        try:
            r = requests.post(f"{self.base_url}/start", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def stop_capture(self):
        try:
            r = requests.post(f"{self.base_url}/stop", timeout=15)
            return r.status_code == 200
        except Exception:
            return False


def execute_cycle(source_ip):
    source_controller = CameraController(source_ip)

    if check_stop_flag():
        return

    if not source_controller.start_capture():
        return

    while not check_stop_flag():
        time.sleep(1)
    
    source_controller.stop_capture()


def main():
    print("hello")
    with open("/home/pi/source_code/variable_files/config.yaml", 'r') as file:
        config = yaml.safe_load(file)
    
    source_ip = config['SOURCE_IPS_CHECK'][0]
    print(source_ip)
    execute_cycle(source_ip)

    try:
        os.remove("/tmp/capture_stop_flag")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    main()