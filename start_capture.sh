#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_LOGGING="/home/pi/source_code/.tmux/plugins/tmux-logging/scripts/start_logging.sh"

# Session for capture control logic
LOCAL_COMMAND2="python $SCRIPT_DIR/newcode.py"
SESSION_NAME2="testing"

echo "Creating new tmux session: $SESSION_NAME2"
tmux new-session -d -s $SESSION_NAME2
tmux send-keys -t $SESSION_NAME2 "$START_LOGGING" Enter
echo "Running command in tmux: $LOCAL_COMMAND2"
tmux send-keys -t $SESSION_NAME2 "$LOCAL_COMMAND2" C-m

