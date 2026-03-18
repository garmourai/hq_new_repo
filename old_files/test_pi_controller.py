import time
import subprocess

def run_command(command):
    """Run a shell command and return True if it succeeds, False otherwise."""
    print("Running command:", " ".join(command))
    try:
        result = subprocess.run(command, check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False

def execute_cycle(source_ip, sink_ip, user,
                  source_remote_dir, sink_remote_dir,
                  source_local_dir, sink_local_dir,
                  schedule):
   
    for i, (start_delay, stop_delay) in enumerate(schedule, start=1):
        print(f"\n=== Cycle {i} ===")

        
        print(f"[Cycle {i}] Starting cameras...")
        start_success = run_command([
            "python3", "pi_controller_saksham.py", "start", source_ip, sink_ip
        ])

        if not start_success:
            print(f"[Cycle {i}] Failed to start cameras. Aborting this cycle.")
            continue

        
        print(f"[Cycle {i}] Waiting {start_delay} seconds before stopping...")
        time.sleep(start_delay)

        
        print(f"[Cycle {i}] Stopping cameras & transferring files...")
        stop_success = run_command([
            "python3", "pi_controller_saksham.py", "stop", source_ip, sink_ip,
            "--user", user,
            "--source-remote-dir", source_remote_dir,
            "--sink-remote-dir", sink_remote_dir,
            "--source-local-dir", source_local_dir,
            "--sink-local-dir", sink_local_dir
        ])

        if not stop_success:
            print(f"[Cycle {i}] Failed to stop cameras or transfer files.")
        
        
        print(f"[Cycle {i}] Waiting {stop_delay} seconds before next cycle...")
        time.sleep(stop_delay)

def main():
    
    source_ip = "192.168.0.104"
    sink_ip   = "192.168.0.34"
    user = "pi"

    source_remote_dir = "/home/pi/videos"
    sink_remote_dir   = "/home/pi/videos"
    source_local_dir  = "./upload/source"
    sink_local_dir    = "./upload/sink"

    
    
    SCHEDULE=[(10,5),(5,0)]

    execute_cycle(
        source_ip,
        sink_ip,
        user,
        source_remote_dir,
        sink_remote_dir,
        source_local_dir,
        sink_local_dir,
        SCHEDULE
    )

if __name__ == "__main__":
    main()