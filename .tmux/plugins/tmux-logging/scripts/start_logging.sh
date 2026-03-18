#!/usr/bin/env bash

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "$CURRENT_DIR/variables.sh"
source "$CURRENT_DIR/shared.sh"

# Function to get track ID from JSON file
get_track_id() {
    local json_file="/home/pi/source_code/variable_files/track_video_index.json"
    if [ -f "$json_file" ]; then
        python3 -c "import json; data=json.load(open('$json_file')); print(data.get('counter', 'unknown'))"
    else
        echo "unknown"
    fi
}

# Function to create log directory for track
create_track_log_dir() {
    local track_id="$1"
    local log_dir="/home/pi/source_code/logs/track_${track_id}"
    mkdir -p "$log_dir"
    echo "$log_dir"
}

start_pipe_pane() {
    echo "Starting pipe pane logging..."
    local track_id=$(get_track_id)
    local log_dir=$(create_track_log_dir "$track_id")
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local pane_id=$(pane_unique_id)
    local log_file="${log_dir}/${pane_id}_${timestamp}.log"
    
    # Ensure the log file exists and has proper permissions
    touch "$log_file"
    chmod 644 "$log_file"
    
    # Use tmux pipe-pane directly instead of calling start_logging_old.sh
    tmux pipe-pane -o -t "$(tmux display-message -p '#{session_name}:#{window_index}.#{pane_index}')" "cat >> '$log_file'"
    
    display_message "Started logging to ${log_file} for track ${track_id}"
}

stop_pipe_pane() {
    local track_id=$(get_track_id)
    tmux pipe-pane
    display_message "Ended logging for track ${track_id}"
}

# returns a string unique to current pane
pane_unique_id() {
    tmux display-message -p "#{session_name}_#{window_index}_#{pane_index}"
}

# saving 'logging' 'not logging' status in a variable unique to pane
set_logging_variable() {
    local value="$1"
    local pane_unique_id="$(pane_unique_id)"
    tmux set-option -gq "@${pane_unique_id}" "$value"
}

# this function checks if logging is happening for the current pane
is_logging() {
    local pane_unique_id="$(pane_unique_id)"
    local current_pane_logging="$(get_tmux_option "@${pane_unique_id}" "not logging")"
    if [ "$current_pane_logging" == "logging" ]; then
        return 0
    else
        return 1
    fi
}

# starts/stop logging
start_logging() {
    if ! is_logging; then
        echo "Logging is starting now..."
        set_logging_variable "logging"
        start_pipe_pane
    else
        echo "Logging is already active, skipping..."
    fi
}

main() {
    echo "Logging started"
    if supported_tmux_version_ok; then
        start_logging
    fi
}
main