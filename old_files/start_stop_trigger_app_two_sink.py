import requests
import time
import subprocess
import sys
import os
import threading
import queue
import json
import socket
import random
import retrieve_files_utils
###################################
# Rsync Queue Implementation
###################################
class RsyncQueue:
    def __init__(self):
        self._queue = queue.Queue()
        self._worker = threading.Thread(target=self._worker_func, daemon=True)
        self._worker.start()

    def _worker_func(self):
        while True:
            cmd = self._queue.get()
            try:
                print("[RsyncQueue] Executing:", " ".join(cmd))
                subprocess.run(cmd, check=True)
                print("[RsyncQueue] Completed:", " ".join(cmd))
            except subprocess.CalledProcessError as e:
                print("[RsyncQueue] Rsync command failed:", e)
            self._queue.task_done()

    def enqueue(self, cmd):
        self._queue.put(cmd)

###################################
# Integrated CameraController Class
###################################
class CameraController:
    def __init__(self, pi_ip):
        self.pi_ip = pi_ip
        self.base_url = f"http://{pi_ip}:8080"
        self.scp_timeout = 300  # seconds for transfers
        self.password = "qwaszx99"  # WARNING: Do not hardcode passwords in production!
        self.ssh_port = 22
        self.offsets = {}

    def get_state(self):
        try:
            response = requests.get(f"{self.base_url}/status", timeout=5)
            if response.ok:
                return response.json().get("state")
            return "UNREACHABLE"
        except requests.RequestException:
            return "UNREACHABLE"

    def update_ntp(self, user, max_retries=5, delay=5):
    
        command = "sudo ntpdate -u 192.168.0.104"

        for attempt in range(max_retries):
            try:
                result = subprocess.run(
                    [
                        "sshpass", "-p", self.password,
                        "ssh", "-p", str(self.ssh_port),
                        f"{user}@{self.pi_ip}", command
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30
                )

                if result.returncode == 0:
                    print(f"[{self.pi_ip}] NTP update successful: {result.stdout.strip()}")
                    return True
                else:
                    print(f"[{self.pi_ip}] NTP update failed (Attempt {attempt + 1}/{max_retries}): {result.stderr.strip()}")
            
            except Exception as e:
                print(f"[{self.pi_ip}] Exception during NTP update (Attempt {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                time.sleep(delay)  # Wait before retrying

        print(f"[{self.pi_ip}] NTP update failed after {max_retries} attempts.")
        return False
        
    def start_capture(self):
        state = self.get_state()
        # if state != "IDLE":
        #     print(f"[{self.pi_ip}] Cannot start capture. Current state: {state}")
        #     return False
            
        try:

            r = requests.post(f"{self.base_url}/start", timeout=5)
            if r.status_code == 200:
                print(f"[{self.pi_ip}] Capture started successfully")

                return True
            print(f"[{self.pi_ip}] Start failed: {r.text}")
            return False
        except Exception as e:
            print(f"[{self.pi_ip}] Start error: {e}")
            return False

    def stop_capture(self, user):
        state = self.get_state()
        num_of_tries = 50
        try_count = 0
        while (True):
            if state == "CAPTURE":
                try:
                    r = requests.post(f"{self.base_url}/stop", timeout=15)
                    print("r.status_code: ", r.status_code)
                    if r.status_code != 200:
                        print(f"[{self.pi_ip}] Stop failed: {r.text}")
                        try_count += 1
                        if try_count > num_of_tries:
                            return False
                    return True
                except Exception as e:
                    print(f"[{self.pi_ip}] Stop error: {e}")
                    return False
            elif state == "IDLE":
                print(f"[{self.pi_ip}] Capture already stopped. Proceeding with file transfer.")
                return True
            else:
                print(f"[{self.pi_ip}] Cannot stop capture. Current state: {state}")
                return False

    def restart(self):
        restart_command = [
            "sshpass", "-p", self.password, 
            "ssh", 
            f"pi@{self.pi_ip}", 
            f"bash /home/pi/sink_code/restart.sh"
        ]

        try:
            subprocess.run(restart_command, check=True)
            print(f"[{self.pi_ip}] Restarted successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"[{self.pi_ip}] Restart failed: {e}")
            return False

    def store_offsets(self, offsets, filename, key):
        data = {}
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as f:
                    data = json.load(f)
            except Exception:
                data = {}
        data[key] = offsets
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

    def store_track_index(self):
        with open("track_video_index.json", "r") as file:
            track_index = json.load(file)
        self.track_index = track_index
        return track_index
    
    def change_track_index(self,id):
        file_path = "/home/pi/source_code/track_video_index.json"  
        with open(file_path, "w") as file:
            self.track_index["counter"] = id
            json.dump(self.track_index, file, indent=4)  
        
    def get_num_frames_captured(self):
        return self.track_index['num_frames']

    def get_and_store_offset(self, sink_key, relay_ip="192.168.0.34", relay_port=8081):
        offsets = []
        
        while len(offsets) < 5:  # Keep looping until we have 5 valid offsets
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            try:
                s.connect((relay_ip, relay_port))
                s.sendall(b"TRIGGER")
                d = s.recv(64)
                if d:
                    try:
                        parsed = json.loads(d.decode("utf-8"))
                        offset = parsed.get("offset", -1)
                        if offset != -1:
                            offsets.append(offset)
                    except Exception:
                        pass  # Ignore errors, don't append -1
            except Exception:
                pass  # Ignore connection errors, don't append -1
            finally:
                s.close()
            
            time.sleep(0.5)  # Small delay before retrying
        print(offsets)
        self.offsets[sink_key] = offsets  # Store valid offsets


    def save_capture(self, user, remote_path, local_path, rsync_queue):
        """
        Waits for video finalization, retrieves the video filename, renames the latest offset file,
        optionally polls a relay for additional offsets, and enqueues rsync transfers for video,
        metadata, and (if requested) offset file.
        """

        try:

            offset_path = f"/home/pi/source_code/uploads/{self.track_index['counter']}"
            os.makedirs(offset_path, exist_ok=True)

            for key, offsets in self.offsets.items():
                offset_file_path = os.path.join(offset_path, f"offset_{key}.json")

                # Save offsets to the file
                with open(offset_file_path, "w") as f:
                    json.dump(self.offsets[key], f)

            return True
        except Exception as e:
            print(f"Not able to copy offset_{video_name}.json")
            return False

###################################
# Execution Cycle (Scheduled Mode)
###################################
def execute_cycle(source_ip, sink_1_ip, sink_2_ip, user,
                  source_remote_dir, sink_remote_dir,
                  source_local_dir, sink_1_local_dir, sink_2_local_dir,
                  schedule, rsync_queue):
    source_controller = CameraController(source_ip)
    sink_1_controller = CameraController(sink_1_ip)
    sink_2_controller = CameraController(sink_2_ip)

    last_capture_stop_time = None

    for i, (start_delay, stop_delay) in enumerate(schedule, start=1):
        print(f"\n=== Cycle {i} ===")
        
       

        print(f"[Cycle {i}] Starting cameras...")

       
        # Ensure the sink camera is idle before proceeding.
        time_before_idle = time.time()
        is_IDLE = True
        while source_controller.get_state() != "IDLE" or sink_1_controller.get_state() != "IDLE" or sink_2_controller.get_state() != "IDLE":
            time.sleep(1)
            if time.time() - time_before_idle > 600: ##10sec ##log this
                is_IDLE = False
                ##TODO restart the server
                break   
        if is_IDLE:   
            ####shift here       
            print(f"Sinks and Source reached IDLE state in {time.time() - time_before_idle} seconds")

            time3 = time.time()

            track_index_dict=source_controller.store_track_index()
            track_index=track_index_dict["counter"]

            content_1=retrieve_files_utils.read_remote_file(sink_1_ip, user, "qwaszx99", 35, "/home/pi/sink_code/track_video_index.json")
            try:
                content_1_dict = json.loads(content_1)
            except:
                content_1_dict = {"counter": -1, "num_frames": 0}
            sink_1_id=content_1_dict['counter']
            new_id = max(track_index, sink_1_id)

            content_2=retrieve_files_utils.read_remote_file(sink_2_ip, user, "qwaszx99", 35, "/home/pi/sink_code/track_video_index.json")
            # Parse string as json
            try:
                content_2_dict = json.loads(content_2)
            except:
                content_2_dict = {"counter": -1, "num_frames": 0}
            sink_2_id=content_2_dict['counter']
            new_id = max(new_id, sink_2_id)

            if new_id != track_index:
                source_controller.change_track_index(new_id)
                status_line_mismatch = f"ID Mismatch!!! source_id:{track_index} sink_1_id:{sink_1_id} sink_2_id:{sink_2_id}"
                mismatch_flag = 1
            elif new_id != sink_1_id:
                if sink_1_controller.restart():
                    print("Sink 1 restarted successfully because of id mismatch")
                else:
                    print("Sink 1 restart failed")
                content_1_dict['counter'] = new_id
                retrieve_files_utils.write_remote_json(sink_1_ip, user, "qwaszx99", "/home/pi/sink_code/track_video_index.json", content_1_dict)
                status_line_mismatch = f"ID Mismatch!!! source_id:{track_index} sink_1_id:{sink_1_id} sink_2_id:{sink_2_id}"
                mismatch_flag = 1
            elif new_id != sink_2_id:
                if sink_2_controller.restart():
                    print("Sink 2 restarted successfully becaues of id mismatch")
                else:
                    print("Sink 2 restart failed")
                content_2_dict['counter'] = new_id
                retrieve_files_utils.write_remote_json(sink_2_ip, user, "qwaszx99", "/home/pi/sink_code/track_video_index.json", content_2_dict)
                status_line_mismatch = f"ID Mismatch!!! source_id:{track_index} sink_1_id:{sink_1_id} sink_2_id:{sink_2_id}"
                mismatch_flag = 1
            else:
                status_line_mismatch = f"ID Matched source_id:{track_index} sink_1_id:{sink_1_id} sink_2_id:{sink_2_id}"
                mismatch_flag = 0

            source_frame_count = source_controller.get_num_frames_captured()
            sink_1_frame_count = content_1_dict['num_frames']
            sink_2_frame_count = content_2_dict['num_frames']
            threshold = int(0.9 * source_frame_count)

            print(f"Number of frames captured by source: {source_frame_count}, sink 1: {sink_1_frame_count}, sink 2: {sink_2_frame_count}")

            status_line_restart = f" Frame Count Matched source_frame_count:{source_frame_count} sink_1_frame_count:{sink_1_frame_count} sink_2_frame_count:{sink_2_frame_count}"
            restart_flag = 0
            if content_1_dict['num_frames'] < threshold:
                status_line_restart = f" Frame Count Mismatch for Sink 1!!! source_frame_count:{source_frame_count} sink_1_frame_count:{sink_1_frame_count} sink_2_frame_count:{sink_2_frame_count}"
                restart_flag = 1
                if sink_1_controller.restart():
                    print("Sink 1 restarted successfully because of frame count mismatch")

                else:
                    print("Sink 1 restart failed")

            if content_2_dict['num_frames'] < threshold:
                status_line_restart = f" Frame Count Mismatch for Sink 2!!! source_frame_count:{source_frame_count} sink_1_frame_count:{sink_1_frame_count} sink_2_frame_count:{sink_2_frame_count}"
                restart_flag = 1
                if sink_2_controller.restart():
                    print("Sink 2 restarted successfully because of frame count mismatch")
                else:
                    print("Sink 2 restart failed")

            # Define folder and file paths
            folder_path = f"/home/pi/source_code/uploads/{new_id}"
            json_file_path_mismatch = os.path.join(folder_path, "check_mismatch.json")
            json_file_path_restart = os.path.join(folder_path, "check_restart.json")

            # Ensure the folder exists
            os.makedirs(folder_path, exist_ok=True)

            # JSON data structure
            json_data_mismatch = {
                "mismatch": mismatch_flag,  # 1 if mismatch, 0 if matched
                "message": status_line_mismatch
            }

            json_data_restart = {
                "restart": restart_flag,  # 1 if restart, 0 if no restart
                "message": status_line_restart
            }
       
            # Write the structured JSON data to the file
            with open(json_file_path_mismatch, "w") as file:
                json.dump(json_data_mismatch, file, indent=4)

            with open(json_file_path_restart, "w") as file:
                json.dump(json_data_restart, file, indent=4)

            print(f"Status saved in {json_file_path_mismatch} and {json_file_path_restart}")   

            print("time to check track index mismatch and crashes: ", time.time() - time3)


        time_before_idle = time.time()
        while source_controller.get_state() != "IDLE" or sink_1_controller.get_state() != "IDLE" or sink_2_controller.get_state() != "IDLE":
            time.sleep(1)
            if time.time() - time_before_idle > 600: ##10sec ##log this
                is_IDLE = False
                ##TODO restart the server
                break  

        print(f"Sinks and Source reached IDLE state in {time.time() - time_before_idle} seconds")

        time1 = time.time()
        # Update NTP on the sink camera to obtain a fresh offset for this cycle.
        print(f"[Cycle {i}] Updating NTP on sink 1 camera for new offset...")
        if not sink_1_controller.update_ntp(user):
            print(f"[Cycle {i}] Failed to update NTP on sink camera. Aborting cycle after 1 second.")
            time.sleep(1)
            continue

        print(f"[Cycle {i}] Updating NTP on sink 2 camera for new offset...")
        if not sink_2_controller.update_ntp(user):
            print(f"[Cycle {i}] Failed to update NTP on sink camera. Aborting cycle after 1 second.")
            time.sleep(1)
            continue

        print("time to do NTP update: ", time.time() - time1)
        
        
        time2 = time.time()
        source_controller.get_and_store_offset("sink_1", sink_1_ip)  ##TODO should have 5 non -1 offset values
        source_controller.get_and_store_offset("sink_2", sink_2_ip)
        source_save_ok = source_controller.save_capture(user, source_remote_dir, source_local_dir, rsync_queue)


        print("time to store offset values: ", time.time() - time2)
    
        if last_capture_stop_time is not None:
            print("Time to start this capture after stopping previous capture: ", time.time() - last_capture_stop_time)
        ok= source_controller.start_capture()
        if not ok:
            print(f"[Cycle {i}] Failed to start source camera. Aborting this cycle after {stop_delay} seconds.")
            ##TODO restart
            time.sleep(stop_delay)
            continue


        print(f"[Cycle {i}] Waiting {start_delay} seconds before stopping...")
        time.sleep(start_delay)

        print(f"[Cycle {i}] Stopping cameras & enqueuing file transfers...")
        source_ok = source_controller.stop_capture(user)
        sink1_ok = sink_1_controller.stop_capture(user)
        sink2_ok = sink_2_controller.stop_capture(user)
        ##TODO will send stop signal to sink as well, timeout value or stop signal both can stop the capture
        print("state of source camera: ",source_controller.get_state())
        # For the source camera, we retrieve and enqueue its offset file.
        # For the sink camera, we do not retrieve additional offset info.
        # sink_save_ok = sink_controller.save_capture(user, sink_remote_dir, sink_local_dir, rsync_queue, get_offset=False)

        # Check for crashes or failures during the cycle.



        if not (source_ok):
            print(f"[Cycle {i}] Failed to stop cameras.")
            ##TODO restart
        if not (source_save_ok):
            print(f"[Cycle {i}] Failed to save offsets file.")

        last_capture_stop_time = time.time()

        # print(f"[Cycle {i}] Waiting {stop_delay} seconds before next cycle...")
        # time.sleep(stop_delay)

###################################
# Main (Hardcoded Configuration)
###################################
def main():
    # Hardcoded configuration parameters
    source_ip = "192.168.0.104"
    sink_1_ip = "192.168.0.10"
    sink_2_ip = "192.168.0.25"
    user = "pi"

    source_remote_dir = "/home/pi/source_code/final_source_videos"
    sink_remote_dir   = "/home/pi/sink_code/final_sink_videos"
    source_local_dir  = "./upload/source"
    sink_1_local_dir    = "./upload/sink_1"
    sink_2_local_dir    = "./upload/sink_2"

    # Define your schedule as a list of (start_delay, stop_delay) tuples.
    SCHEDULE = [(random.randint(30, 900), 0) for _ in range(300)]
    # Create one persistent rsync queue.
    rsync_queue = RsyncQueue()

    # Execute the scheduled capture cycles.
    execute_cycle(
        source_ip,
        sink_1_ip,
        sink_2_ip,
        user,
        source_remote_dir,
        sink_remote_dir,
        source_local_dir,
        sink_1_local_dir,
        sink_2_local_dir,
        SCHEDULE,
        rsync_queue
    )

if __name__ == "__main__":
    main()