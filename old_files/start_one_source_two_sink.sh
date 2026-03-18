#!/bin/bash

# Local TMUX session settings
SESSION_NAME1="server"  # Change this to your session name
SESSION_NAME2="testing"  # Change this to your session name
SESSION_NAME3="upload"

# Toggle logging script
TOGGLE_LOGGING="/home/pi/.tmux/plugins/tmux-logging/scripts/toggle_logging.sh"

LOCAL_COMMAND1="rpicam-source --level 4.2 --framerate 30 --width 1280 --height 720 -t 60000s --denoise cdn_off -n" 
LOCAL_COMMAND2="python start_stop_trigger_app_two_sink.py"       # Replace this with the command you want to run locally
LOCAL_COMMAND3="python upload_files.py"       # Replace this with the command you want to run locally

# Remote SSH settings
REMOTE_USER="pi"        # Change this to your SSH username
REMOTE_HOST_1="192.168.0.10"       # Change this to the remote server address
REMOTE_HOST_2="192.168.0.25"
REMOTE_PASSWORD="qwaszx99"    # WARNING: Storing passwords in plain text is insecure!
REMOTE_COMMANDS="bash /home/pi/sink_code/start.sh"    # Commands to run on the remote server

# Kill existing tmux session if it exists
tmux has-session -t $SESSION_NAME1 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Deleting existing tmux session: $SESSION_NAME1"
    tmux send-keys -t $SESSION_NAME1 "$TOGGLE_LOGGING" Enter
    tmux kill-session -t $SESSION_NAME1
fi

tmux has-session -t $SESSION_NAME2 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Deleting existing tmux session: $SESSION_NAME2"
    tmux send-keys -t $SESSION_NAME2 "$TOGGLE_LOGGING" Enter
    tmux kill-session -t $SESSION_NAME2
fi

tmux has-session -t $SESSION_NAME3 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Deleting existing tmux session: $SESSION_NAME3"
    tmux send-keys -t $SESSION_NAME3 "$TOGGLE_LOGGING" Enter
    tmux kill-session -t $SESSION_NAME3
fi

# Create a new tmux session
echo "Creating new tmux session: $SESSION_NAME1"
tmux new-session -d -s $SESSION_NAME1
tmux send-keys -t $SESSION_NAME1 "$TOGGLE_LOGGING" Enter

# Run a command inside the tmux session
echo "Running command in tmux: $LOCAL_COMMAND1"
tmux send-keys -t $SESSION_NAME1 "$LOCAL_COMMAND1" C-m


# SSH into the remote server with a password and run commands
echo "SSH into $REMOTE_HOST_1 and executing commands..."
echo sshpass -p "$REMOTE_PASSWORD" ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST_1" "$REMOTE_COMMANDS"
sshpass -p "$REMOTE_PASSWORD" ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST_1" "$REMOTE_COMMANDS"

echo "SSH into $REMOTE_HOST_2 and executing commands..."
echo sshpass -p "$REMOTE_PASSWORD" ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST_2" "$REMOTE_COMMANDS"
sshpass -p "$REMOTE_PASSWORD" ssh -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST_2" "$REMOTE_COMMANDS"

# Create a new tmux session
echo "Creating new tmux session: $SESSION_NAME2"
tmux new-session -d -s $SESSION_NAME2
tmux send-keys -t $SESSION_NAME2 "$TOGGLE_LOGGING" Enter

# Run a command inside the tmux session
echo "Running command in tmux: $LOCAL_COMMAND2"
tmux send-keys -t $SESSION_NAME2 "$LOCAL_COMMAND2" C-m

# Create a new tmux session
echo "Creating new tmux session: $SESSION_NAME3"
tmux new-session -d -s $SESSION_NAME3
tmux send-keys -t $SESSION_NAME3 "$TOGGLE_LOGGING" Enter

# Run a command inside the tmux session
echo "Running command in tmux: $LOCAL_COMMAND3"
tmux send-keys -t $SESSION_NAME3 "$LOCAL_COMMAND3" C-m

echo "All done!"