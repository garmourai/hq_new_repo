#!/bin/bash


# Clear ALL existing logging states to ensure clean start
echo "Clearing all existing logging states..."
tmux show-options -g | grep "@.*logging" | while read line; do
    option_name=$(echo "$line" | cut -d' ' -f1)
    if [ ! -z "$option_name" ]; then
        echo "Clearing $option_name"
        tmux set-option -gq "$option_name" "not logging"
    fi
done

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Adaptive camera configuration - automatically detects lighting conditions
ADAPTIVE_CAMERA="$SCRIPT_DIR/adaptive_camera.py"

echo "Running ensure_dir script..."
python3 ./ensure_dirs.py

# Clear all existing logging states
echo "Clearing existing logging states..."
tmux show-options -g | grep "@.*logging" | while read line; do
    option_name=$(echo "$line" | cut -d' ' -f1)
    if [ ! -z "$option_name" ]; then
        echo "Clearing $option_name"
        tmux set-option -gq "$option_name" "not logging"
    fi
done

# Local TMUX session settings
SESSION_NAME1="server"
SESSION_NAME3="upload"
SESSION_NAME4="hls"

# Logging scripts
START_LOGGING="/home/pi/source_code/.tmux/plugins/tmux-logging/scripts/start_logging.sh"
STOP_LOGGING="/home/pi/source_code/.tmux/plugins/tmux-logging/scripts/stop_logging.sh"
 
# Camera detection will be done after tmux sessions are killed
LOCAL_COMMAND3="source /home/pi/3d_vision/bin/activate && python $SCRIPT_DIR/upload_files.py"
LOCAL_COMMAND4="python3 $SCRIPT_DIR/packet_buffer_to_hls.py"

LOCAL_COMMAND1="rpicam-source  --tuning-file /home/pi/source_code/imx477_new_arducam_160.json   --width 1440 --height 1080 --ev 1 --brightness 0.05 --saturation 1.3 \
    -t 60000s --denoise cdn_off -n --bitrate 4000000"
echo "Using fixed settings with custom tuning file"
echo "Camera command: $LOCAL_COMMAND1"

# Kill existing tmux sessions if they exist (server, upload, hls)
for SESSION in "$SESSION_NAME1" "$SESSION_NAME3" "$SESSION_NAME4"; do
    tmux has-session -t $SESSION 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Deleting existing tmux session: $SESSION"
        tmux send-keys -t $SESSION "$STOP_LOGGING" Enter
        tmux kill-session -t $SESSION
        echo "Deleted: $SESSION"
    fi
done
# Create and start tmux session for server
echo "Creating new tmux session: $SESSION_NAME1"
tmux new-session -d -s $SESSION_NAME1
tmux send-keys -t $SESSION_NAME1 "$START_LOGGING" Enter
echo "Running command in tmux: $LOCAL_COMMAND1"
tmux send-keys -t $SESSION_NAME1 "$LOCAL_COMMAND1" C-m


# Create and start tmux session for upload
echo "Creating new tmux session: $SESSION_NAME3"
tmux new-session -d -s $SESSION_NAME3
tmux send-keys -t $SESSION_NAME3 "$START_LOGGING" Enter
echo "Running command in tmux: $LOCAL_COMMAND3"
tmux send-keys -t $SESSION_NAME3 "$LOCAL_COMMAND3" C-m

# Create and start tmux session for HLS (socket -> ts_segments via packet_buffer_to_hls.py)
echo "Creating new tmux session: $SESSION_NAME4"
tmux new-session -d -s $SESSION_NAME4
tmux send-keys -t $SESSION_NAME4 "$START_LOGGING" Enter
echo "Running command in tmux: $LOCAL_COMMAND4"
tmux send-keys -t $SESSION_NAME4 "$LOCAL_COMMAND4" C-m

echo "All done!"



# Check and remove capture stop flag if it exists
if [ -f "/tmp/capture_stop_flag" ]; then
    echo "Found /tmp/capture_stop_flag, removing it..."
    rm "/tmp/capture_stop_flag"
    echo "Removed /tmp/capture_stop_flag"
else
    echo "No /tmp/capture_stop_flag found"
fi


LOCAL_COMMAND2="python /home/pi/source_code/newcode.py"
SESSION_NAME2="testing"
# Handle testing session only (hls is started above with server/upload)
for SESSION in "$SESSION_NAME2"; do
    tmux has-session -t $SESSION 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Deleting existing tmux session: $SESSION"
        tmux send-keys -t $SESSION "$STOP_LOGGING" Enter
        tmux kill-session -t $SESSION
        echo "Deleted: $SESSION"
    fi
done