#!/bin/bash

echo "Running ensure_dir script..."
python3 /home/pi/source_code/ensure_dirs.py
# Local TMUX session settings
SESSION_NAME1="server"
SESSION_NAME2="testing"
SESSION_NAME3="upload"

# Logging scripts
START_LOGGING="/home/pi/.tmux/plugins/tmux-logging/scripts/start_logging.sh"
STOP_LOGGING="/home/pi/.tmux/plugins/tmux-logging/scripts/stop_logging.sh"

LOCAL_COMMAND1="rpicam-source --level 4.2 --framerate 30 --width 1280 --height 720 -t 60000s --denoise cdn_off -n" 
LOCAL_COMMAND2="python /home/pi/source_code/start_stop_trigger_app_multi_sinks.py"
LOCAL_COMMAND3="source /home/pi/3d_vision/bin/activate && python /home/pi/source_code/upload_files.py"

# Remote SSH settings
DEFAULT_REMOTE_USER="pi"
REMOTE_PASSWORD="qwaszx99" 
REMOTE_COMMANDS="bash /home/pi/sink_code/start.sh"

# Sink IPs (for the last IP, the user is rpi)

# SINK_IPS=("192.168.1.100")
readarray -t SINK_IPS < <(python3 <<EOF
import yaml

with open("/home/pi/source_code/variable_files/config.yaml") as f:
    data = yaml.safe_load(f)

for ip in data.get("SINK_IPS", []):
    print(str(ip))
EOF
)

# Kill existing tmux sessions if they exist
for SESSION in "$SESSION_NAME1" "$SESSION_NAME2" "$SESSION_NAME3"; do
    tmux has-session -t $SESSION 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Deleting existing tmux session: $SESSION"
        tmux send-keys -t $SESSION "$STOP_LOGGING" Enter
        tmux kill-session -t $SESSION
        echo "Deleted: $SESSION"
    fi
done