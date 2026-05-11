#!/usr/bin/env python3
"""
Watches a growing playlist.m3u8 and produces a sliding-window live.m3u8.
Serves files from the ts_segments/ parent directory via HTTP so the URL
(http://localhost:8083/live.m3u8) never changes between tracks.
Cycles automatically to the next track when the current one ends.
"""

import json
import os
import time
import threading
import argparse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

BASE       = os.path.dirname(os.path.abspath(__file__))
INDEX_FILE = os.path.join(BASE, "variable_files", "track_video_index.json")
PORT       = 8083
WINDOW_SIZE = 30


def load_counter():
    with open(INDEX_FILE) as f:
        return json.load(f)["counter"]


def parse_playlist(path):
    """Return (segments, has_endlist). segments: list of (duration_float, filename)."""
    segments = []
    has_endlist = False
    try:
        with open(path) as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return segments, has_endlist
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF:"):
            duration = float(line.split(":")[1].rstrip(","))
            seg = lines[i + 1].strip()
            segments.append((duration, seg))
            i += 2
        elif line.strip() == "#EXT-X-ENDLIST":
            has_endlist = True
            i += 1
        else:
            i += 1
    return segments, has_endlist


def parse_target_duration(path):
    with open(path) as f:
        for line in f:
            if line.startswith("#EXT-X-TARGETDURATION:"):
                return int(line.strip().split(":")[1])
    return None


def write_playlist(live_path, window, media_sequence, target_duration, prefix="", done=False):
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}",
    ]
    for duration, name in window:
        lines.append(f"#EXTINF:{duration:.6f},")
        lines.append(f"{prefix}/{name}" if prefix else name)
    if done:
        lines.append("#EXT-X-ENDLIST")
    with open(live_path, "w") as f:
        f.write("\n".join(lines) + "\n")


def watcher_loop(ts_base, live_m3u8, window_size, poll_interval):
    while True:
        counter = load_counter()
        src_m3u8 = os.path.join(ts_base, str(counter), "playlist.m3u8")

        print(f"\nWaiting for track {counter}: {src_m3u8} ...")
        while not os.path.exists(src_m3u8):
            new_counter = load_counter()
            if new_counter != counter:
                print(f"Counter jumped to {new_counter} before playlist appeared — following")
                counter = new_counter
                src_m3u8 = os.path.join(ts_base, str(counter), "playlist.m3u8")
            time.sleep(poll_interval)

        target_duration = parse_target_duration(src_m3u8)
        print(f"Track {counter} started  (target duration: {target_duration}s)")

        released = []
        seen = 0
        media_seq = 0

        write_playlist(live_m3u8, [], 0, target_duration, prefix=str(counter))

        while True:
            all_segs, has_endlist = parse_playlist(src_m3u8)
            new_segs = all_segs[seen:]

            for duration, name in new_segs:
                released.append((duration, name))
                seen += 1
                if len(released) > window_size:
                    media_seq += 1
                window = released[-window_size:]
                write_playlist(live_m3u8, window, media_seq, target_duration,
                               prefix=str(counter),
                               done=has_endlist and seen == len(all_segs))
                print(f"  [{seen:>5}]  added {counter}/{name}  ({duration:.2f}s)  "
                      f"seq={media_seq}  window={len(window)}")

            new_counter = load_counter()
            if has_endlist or new_counter != counter:
                # Write a final ENDLIST so players know this track is done
                write_playlist(live_m3u8, released[-window_size:], media_seq,
                               target_duration, prefix=str(counter), done=True)
                print(f"Track {counter} done — switching to {new_counter}")
                break

            time.sleep(poll_interval)


class Handler(SimpleHTTPRequestHandler):
    track_dir = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=self.track_dir, **kwargs)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Cache-Control", "no-cache, no-store")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def address_string(self):
        return self.client_address[0]

    def log_message(self, fmt, *args):
        msg = fmt % args
        if ".ts" in msg or ".m3u8" in msg:
            print(f"  fetched: {self.path}")


def main():
    parser = argparse.ArgumentParser(description="Live HLS circular playlist — cycles through tracks automatically")
    parser.add_argument("--port",   type=int,   default=PORT)
    parser.add_argument("--window", type=int,   default=WINDOW_SIZE,
                        help="Sliding window size (number of segments in live playlist)")
    parser.add_argument("--poll",   type=float, default=1.0,
                        help="Poll interval in seconds (how often to check playlist.m3u8)")
    args = parser.parse_args()

    ts_base   = os.path.join(BASE, "ts_segments")
    live_m3u8 = os.path.join(ts_base, "live.m3u8")

    print(f"Segments root : {ts_base}")
    print(f"Live playlist : {live_m3u8}")
    print(f"Port          : {args.port}")
    print(f"Window        : {args.window} segments")
    print(f"Poll interval : {args.poll}s\n")

    Handler.track_dir = ts_base
    server = ThreadedHTTPServer(("", args.port), Handler)

    t = threading.Thread(
        target=watcher_loop,
        args=(ts_base, live_m3u8, args.window, args.poll),
        daemon=True,
    )
    t.start()

    print(f"Stream URL  →  http://localhost:{args.port}/live.m3u8\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
