#!/usr/bin/env bash
set -euo pipefail

# Clears generated streaming artifacts without touching upload_staged_files/.
#
# Removes contents of:
# - ts_segments/
# - streamed_packets/
# - packet_buffer_for_ts/
# - temporary_videos/
# - temporary_metadata/
#
# Safety:
# - Refuses to run unless /home/pi/source_code/.git exists

ROOT="/home/pi/source_code"

if [[ ! -d "$ROOT/.git" ]]; then
  echo "ERROR: $ROOT does not look like the repo root (.git missing)."
  exit 1
fi

paths=(
  "$ROOT/ts_segments"
  "$ROOT/streamed_packets"
  "$ROOT/packet_buffer_for_ts"
  "$ROOT/temporary_videos"
  "$ROOT/temporary_metadata"
)

for p in "${paths[@]}"; do
  if [[ -d "$p" ]]; then
    rm -rf -- "$p"/*
  fi
done

