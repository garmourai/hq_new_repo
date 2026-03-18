#!/usr/bin/env bash
set -euo pipefail

if command -v vcgencmd >/dev/null 2>&1; then
  vcgencmd measure_temp
  exit 0
fi

if [[ -r /sys/class/thermal/thermal_zone0/temp ]]; then
  raw="$(cat /sys/class/thermal/thermal_zone0/temp)"
  awk -v t="$raw" 'BEGIN { printf "temp=%.1f'\''C\n", (t/1000.0) }'
  exit 0
fi

echo "temp=unknown"

