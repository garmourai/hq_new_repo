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

start_pipe_pane() {
    local track_id=$(get_track_id)
    local log_dir="/home/pi/source_code/logs/track_${track_id}"
    mkdir -p "$log_dir"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local pane_id=$(pane_unique_id)
    local log_file="${log_dir}/${pane_id}_${timestamp}.log"
    
    "$CURRENT_DIR/start_logging_old.sh" "${log_file}"
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
stop_logging() {
    if is_logging; then
        set_logging_variable "not logging"
        stop_pipe_pane
    fi
}

main() {
    if supported_tmux_version_ok; then
        stop_logging
    fi
}
main