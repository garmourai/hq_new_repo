#!/usr/bin/env python3
"""
Live Packet Buffer to HLS Converter (Streaming-Friendly, No FFmpeg wrapper outside)

Path-independent: all paths are relative to the directory containing this script.
- variable_files/udp_streaming_config.json – local_ts_segments must be true; C++ pushes packets via Unix socket (no .bin/.txt files).
- variable_files/track_video_index.json – track_id_index (counter)
- ts_segments/<track_id>/ – HLS output (m3u8 + .ts)
"""

import struct
import os
import time
import signal
import shutil
import subprocess
import socket
import fcntl
import termios
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import sys
import av  # PyAV - same as inspire.py
import threading
from collections import defaultdict as dd

# Base directory: same folder as this script (path-independent, works anywhere)
_SCRIPT_DIR = Path(__file__).resolve().parent

# Config shared with C++ (variable_files/udp_streaming_config.json)
def _is_local_ts_segments():
    """True if local TS segments are enabled via socket. C++ pushes packets to this script; no .bin/.txt files."""
    try:
        cfg_path = _SCRIPT_DIR / "variable_files" / "udp_streaming_config.json"
        if not cfg_path.exists():
            return False
        with open(cfg_path, "r") as f:
            import json
            data = json.load(f)
        return data.get("local_ts_segments", False)
    except Exception:
        return False


def _get_local_ts_socket_path():
    """Path for the Unix socket (must match C++)."""
    try:
        cfg_path = _SCRIPT_DIR / "variable_files" / "udp_streaming_config.json"
        if not cfg_path.exists():
            return "/tmp/rpicam_hls.sock"
        with open(cfg_path, "r") as f:
            import json
            data = json.load(f)
        return data.get("local_ts_socket_path", "/tmp/rpicam_hls.sock")
    except Exception:
        return "/tmp/rpicam_hls.sock"


HEADER_SIZE = 23  # 8 (sensor_ts) + 8 (wallclock_ts) + 4 (size) + 1 (keyframe) + 1 (packet_index) + 1 (total_packets)


def _read_packet_from_socket(conn):
    """Read one length-prefixed packet (4-byte big-endian length then payload). Returns bytes or None on EOF/error."""
    try:
        buf = b""
        while len(buf) < 4:
            chunk = conn.recv(4 - len(buf))
            if not chunk:
                print("[SOCKET_DEBUG] EOF: conn.recv() returned empty bytes (connection closed)", file=sys.stderr)
                return None
            buf += chunk
        length = struct.unpack(">I", buf)[0]
        if length < HEADER_SIZE or length > 1024 * 1024:
            print(
                f"[SOCKET_DEBUG] Invalid length: {length} (expected {HEADER_SIZE}..{1024*1024})",
                file=sys.stderr,
            )
            return None
        buf = b""
        while len(buf) < length:
            chunk = conn.recv(length - len(buf))
            if not chunk:
                print(
                    "[SOCKET_DEBUG] EOF mid-packet: conn.recv() returned empty while reading payload",
                    file=sys.stderr,
                )
                return None
            buf += chunk
        return buf
    except Exception as e:
        print(f"[SOCKET_DEBUG] Exception in _read_packet_from_socket: {type(e).__name__}: {e}", file=sys.stderr)
        return None

SEGMENT_DURATION = 4.0  # 4 seconds per segment


class TimeProfiler:
    """Enhanced time profiler to track operation timings and identify bottlenecks"""

    def __init__(self):
        self.timings = dd(list)  # {operation_name: [time1, time2, ...]}
        self.counts = dd(int)  # {operation_name: count}
        self.last_report_time = time.perf_counter()
        self.report_interval = 10.0  # Report every 10 seconds
        self.start_time = time.perf_counter()

    def time_operation(self, operation_name):
        """Context manager for timing operations"""
        return self._TimingContext(self, operation_name)

    class _TimingContext:
        def __init__(self, profiler, operation_name):
            self.profiler = profiler
            self.operation_name = operation_name
            self.start_time = None

        def __enter__(self):
            self.start_time = time.perf_counter()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            elapsed = time.perf_counter() - self.start_time
            self.profiler.timings[self.operation_name].append(elapsed)
            self.profiler.counts[self.operation_name] += 1
            return False

    def should_report(self):
        """Check if it's time to print a report"""
        now = time.perf_counter()
        if now - self.last_report_time >= self.report_interval:
            self.last_report_time = now
            return True
        return False

    def print_report(self):
        """Print enhanced timing statistics with bottleneck analysis"""
        if not self.timings:
            return

        # Calculate total time across all operations
        total_times = {}
        grand_total = 0
        for op_name, times in self.timings.items():
            total = sum(times)
            total_times[op_name] = total
            grand_total += total

        if grand_total == 0:
            return

        # Sort by total time (descending)
        sorted_ops = sorted(total_times.items(), key=lambda x: x[1], reverse=True)

        print("\n" + "=" * 80, file=sys.stderr)
        print("⏱️  TIME PROFILING REPORT - BOTTLENECK ANALYSIS", file=sys.stderr)
        print("=" * 80, file=sys.stderr)

        # Print all operations in order (by total time, descending)
        for op_name, total_time in sorted_ops:
            self._print_operation_stats(op_name, total_time, grand_total)

        # Summary
        print("\n" + "-" * 80, file=sys.stderr)
        print(
            f"📈 SUMMARY: {len(total_times)} operations tracked, {grand_total*1000:.2f}ms total",
            file=sys.stderr,
        )
        if sorted_ops:
            top_bottleneck = sorted_ops[0]
            print(
                f"🔴 TOP BOTTLENECK: {top_bottleneck[0]} ({top_bottleneck[1]*1000:.2f}ms, "
                f"{top_bottleneck[1]/grand_total*100:.1f}% of total time)",
                file=sys.stderr,
            )
        print("=" * 80 + "\n", file=sys.stderr)

    def _print_operation_stats(self, op_name, total_time, grand_total):
        """Print statistics for a single operation"""
        times = self.timings[op_name]
        count = self.counts[op_name]
        avg_time = total_time / count if count > 0 else 0
        min_time = min(times) if times else 0
        max_time = max(times) if times else 0
        pct = (total_time / grand_total * 100) if grand_total > 0 else 0
        ops_per_sec = count / (total_time) if total_time > 0 else 0

        if pct > 30:
            emoji = "🔴"
        elif pct > 15:
            emoji = "🟠"
        elif pct > 5:
            emoji = "🟡"
        else:
            emoji = "🟢"

        print(f"  {emoji} {op_name:30s}:", file=sys.stderr)
        print(
            f"     Count: {count:6d} | Total: {total_time*1000:8.2f}ms ({pct:5.1f}%) | "
            f"Avg: {avg_time*1000:6.2f}ms | Min: {min_time*1000:6.2f}ms | "
            f"Max: {max_time*1000:6.2f}ms | {ops_per_sec:6.1f} ops/sec",
            file=sys.stderr,
        )


class BinPacketStream:
    """Read length-prefixed packets from a single append-only .bin file per track.
    Format: 4-byte big-endian length, then that many bytes (23-byte header + payload).
    """

    def __init__(self, bin_path: Path):
        self.bin_path = Path(bin_path)
        self._file = None
        self._buffer = bytearray()

    def open(self):
        """Open the bin file for reading (from current end for live tail, or from start for catch-up)."""
        if self._file is not None:
            return True
        if not self.bin_path.exists():
            return False
        try:
            self._file = open(self.bin_path, "rb")
            self._buffer.clear()
            return True
        except Exception:
            return False

    def close(self):
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        self._buffer.clear()

    def read_packet(self):
        """Read one length-prefixed packet. Returns (packet_bytes) or None if no complete packet / EOF."""
        if self._file is None:
            if not self.open():
                return None
        try:
            # Need at least 4 bytes for length
            while len(self._buffer) < 4:
                chunk = self._file.read(4096)
                if not chunk:
                    return None
                self._buffer.extend(chunk)
            length = struct.unpack(">I", bytes(self._buffer[:4]))[0]
            if length < HEADER_SIZE or length > 1024 * 1024:
                return None
            need = 4 + length
            while len(self._buffer) < need:
                chunk = self._file.read(4096)
                if not chunk:
                    return None
                self._buffer.extend(chunk)
            packet_bytes = bytes(self._buffer[4:need])
            del self._buffer[:need]
            return packet_bytes
        except Exception:
            return None


class LiveCSVReader:
    """Read growing CSV file sequentially from beginning - READ ONLY (file is written by C++ code)"""

    def __init__(self, csv_file):
        self.csv_file = Path(csv_file)
        self.file_handle = None
        self.line_number = 0  # Track which line we're on (0-indexed)
        self.current_position = 0  # Track file position to allow "staying" on same line

    def __enter__(self):
        if self.csv_file.exists():
            self.file_handle = open(self.csv_file, "r")
            self.file_handle.seek(0)
            self.line_number = 0
            self.current_position = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file_handle:
            self.file_handle.close()

    def read_next_line(self):
        """Read next line sequentially - one line at a time"""
        if not self.file_handle:
            if self.csv_file.exists():
                self.file_handle = open(self.csv_file, "r")
                self.file_handle.seek(0)
                self.line_number = 0
                self.current_position = 0
            else:
                return None

        self.current_position = self.file_handle.tell()
        line = self.file_handle.readline()

        if line:
            self.line_number += 1
            return line.strip()
        else:
            self.file_handle.seek(0, os.SEEK_END)
            file_size = self.file_handle.tell()

            if file_size > self.current_position:
                self.file_handle.seek(self.current_position)
                line = self.file_handle.readline()
                if line:
                    self.line_number += 1
                    return line.strip()

            self.file_handle.seek(self.current_position)
            return None

    def stay_on_current_line(self):
        if self.file_handle:
            self.file_handle.seek(self.current_position)


class LivePacketProcessor:
    """Processes packets in real-time and creates HLS segments"""

    def __init__(self, packet_buffer_dir, output_dir, segment_duration=4.0, clear_dirs=False):
        self.packet_buffer_dir = Path(packet_buffer_dir)
        self.segment_duration = segment_duration

        # Store base HLS directory for dynamic track switching
        # If output_dir ends with a number (like /path/to/ts_segments/1331), extract base
        output_path = Path(output_dir)
        try:
            # Check if last component is numeric (track_id_index)
            last_component = output_path.name
            if last_component.isdigit():
                self.hls_base_dir = output_path.parent
                self.use_dynamic_output = True
            else:
                self.hls_base_dir = None
                self.use_dynamic_output = False
        except Exception:
            self.hls_base_dir = None
            self.use_dynamic_output = False

        # Set initial output_dir (will be updated when track changes if use_dynamic_output)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.frame_buffers = {}
        self.expected_frame_num = None
        self.frame_wait_start = None
        self._frame_read_start_time = None

        self.csv_reader = None
        self.csv_entries_buffer = {}
        self.bin_stream = None  # Single append-only .bin per track (length-prefixed)

        # Track which track_id_index we are currently processing
        self.current_track_id_index = None

        # Track last time we saw any new packet / frame activity
        self.last_activity_ts = time.time()

        # Track if we've successfully processed at least one frame from current track
        # (only switch tracks if we've processed frames AND had 5s inactivity)
        self.frames_processed_count = 0

        self.lock = threading.Lock()
        self.profiler = TimeProfiler()

        try:
            self.codec = av.CodecContext.create("h264", "r")
            print("✅ PyAV H.264 decoder initialized", file=sys.stderr)
        except Exception as e:
            print(f"❌ Failed to initialize PyAV decoder: {e}", file=sys.stderr)
            raise

        self.ffmpeg_process = None
        self._init_ffmpeg()

        self.playlist_path = self.output_dir / "playlist.m3u8"
        self.last_segment_num = -1
        self._init_playlist()

        print(f"📁 Watching: {self.packet_buffer_dir}", file=sys.stderr)
        print(f"📁 Output: {self.output_dir}", file=sys.stderr)
        print(
            f"⏱️  FFmpeg will create TS segments every {self.segment_duration} seconds\n",
            file=sys.stderr,
        )

    def _init_ffmpeg(self):
        """Initialize ffmpeg subprocess for creating TS segments"""
        try:
            output_path = str(self.output_dir.absolute() / "seg_%05d.ts")
            print(f"🎬 Initializing ffmpeg with output path: {output_path}", file=sys.stderr)

            segment_filename = "seg_%05d.ts"

            self.ffmpeg_process = subprocess.Popen(
                [
                    "ffmpeg",
                    "-nostats",
                    "-loglevel",
                    "warning",
                    "-fflags",
                    "+genpts",
                    "-analyzeduration",
                    "10000000",
                    "-probesize",
                    "10000000",
                    "-f",
                    "h264",
                    "-i",
                    "-",
                    "-c",
                    "copy",
                    "-f",
                    "segment",
                    "-segment_time",
                    str(self.segment_duration),
                    "-segment_format",
                    "mpegts",
                    "-reset_timestamps",
                    "1",
                    "-write_header",
                    "1",
                    segment_filename,
                ],
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
                cwd=str(self.output_dir.absolute()),
            )

            def read_stderr():
                if self.ffmpeg_process and self.ffmpeg_process.stderr:
                    for line in iter(self.ffmpeg_process.stderr.readline, b""):
                        if line:
                            print(
                                f"FFmpeg: {line.decode('utf-8', errors='ignore').strip()}",
                                file=sys.stderr,
                            )

            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stderr_thread.start()

            print("✅ FFmpeg subprocess initialized for TS segment creation", file=sys.stderr)
        except Exception as e:
            print(f"❌ Failed to initialize ffmpeg subprocess: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc()
            raise

    def push_h264(self, nal_bytes):
        """Push H.264 NAL units to ffmpeg subprocess"""
        if not self.ffmpeg_process:
            print("⚠️  FFmpeg process not initialized!", file=sys.stderr)
            return

        if not self.ffmpeg_process.stdin:
            print("⚠️  FFmpeg stdin is None!", file=sys.stderr)
            return

        if self.ffmpeg_process.poll() is not None:
            print(
                f"⚠️  FFmpeg process has terminated (return code: {self.ffmpeg_process.returncode})",
                file=sys.stderr,
            )
            self._init_ffmpeg()
            if not self.ffmpeg_process or not self.ffmpeg_process.stdin:
                print("❌ Failed to restart ffmpeg", file=sys.stderr)
                return

        try:
            self.ffmpeg_process.stdin.write(nal_bytes)
            self.ffmpeg_process.stdin.flush()
        except (BrokenPipeError, OSError) as e:
            print(f"⚠️  Error writing to ffmpeg: {e}", file=sys.stderr)
            self._init_ffmpeg()
            if self.ffmpeg_process and self.ffmpeg_process.stdin:
                try:
                    self.ffmpeg_process.stdin.write(nal_bytes)
                    self.ffmpeg_process.stdin.flush()
                except Exception as e2:
                    print(f"❌ Failed to write after restart: {e2}", file=sys.stderr)

    def _init_playlist(self):
        """Initialize empty M3U8 playlist file"""
        with open(self.playlist_path, "w") as f:
            f.write("#EXTM3U\n")
            f.write("#EXT-X-VERSION:3\n")
            f.write("#EXT-X-TARGETDURATION:7\n")
            f.write("#EXT-X-MEDIA-SEQUENCE:0\n")
            f.write("\n")

    def append_segment_to_playlist(self, segment_num):
        """Append a new segment to the m3u8 playlist"""
        segment_name = f"seg_{segment_num:05d}.ts"
        segment_path = self.output_dir / segment_name

        if not segment_path.exists():
            return False

        with open(self.playlist_path, "a") as f:
            duration = float(self.segment_duration)
            f.write(f"#EXTINF:{duration:.3f},\n")
            f.write(f"{segment_name}\n")
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        self.last_segment_num = segment_num
        return True

    def check_and_update_playlist(self):
        """Check for new segments and append them to playlist"""
        next_segment_num = self.last_segment_num + 1
        segment_name = f"seg_{next_segment_num:05d}.ts"
        segment_path = self.output_dir / segment_name

        if segment_path.exists():
            self.append_segment_to_playlist(next_segment_num)
            while True:
                next_segment_num += 1
                if not self.append_segment_to_playlist(next_segment_num):
                    break

    def cleanup(self):
        """Clean up ffmpeg subprocess"""
        if self.ffmpeg_process:
            try:
                if self.ffmpeg_process.stdin:
                    self.ffmpeg_process.stdin.close()
                self.ffmpeg_process.wait(timeout=5)
                print("✅ FFmpeg subprocess closed", file=sys.stderr)
            except subprocess.TimeoutExpired:
                print("⚠️  FFmpeg subprocess did not terminate, killing...", file=sys.stderr)
                self.ffmpeg_process.kill()
                self.ffmpeg_process.wait()
            except Exception as e:
                print(f"⚠️  Error closing ffmpeg subprocess: {e}", file=sys.stderr)
        self.ffmpeg_process = None

    def _switch_output_to_track(self, track_id):
        """Switch HLS output to ts_segments/<track_id>. Used when receiving a new connection in socket mode."""
        base = self.hls_base_dir if self.hls_base_dir is not None else _SCRIPT_DIR / "ts_segments"
        self.output_dir = Path(base) / str(track_id)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if self.ffmpeg_process:
            self.cleanup()
        self._init_ffmpeg()
        self.playlist_path = self.output_dir / "playlist.m3u8"
        self.last_segment_num = -1
        self._init_playlist()
        self.current_track_id_index = track_id
        print(f"📁 Output switched to track {track_id}: {self.output_dir}", file=sys.stderr)

    def run_from_socket(self, conn, track_id):
        """Process packets from a single socket connection (C++ pushes directly). Blocks until connection closes."""
        self._switch_output_to_track(track_id)
        self.expected_frame_num = 1
        self.frame_buffers = {}
        self.frames_processed_count = 0
        print(f"📡 Processing stream from socket for track_id={track_id}...", file=sys.stderr)
        try:
            while True:
                packet_bytes = _read_packet_from_socket(conn)
                if packet_bytes is None:
                    break
                packet_info, error = self._parse_packet_bytes(packet_bytes)
                if not packet_info:
                    continue
                expected_frame = self.expected_frame_num
                buffer = self.frame_buffers.get(expected_frame)
                # Resync: if we have an incomplete frame and receive packet_index==0, sender dropped the rest
                if buffer and len(buffer["packets"]) < buffer["total_packets"] and packet_info["packet_index"] == 0:
                    print(
                        f"[SOCKET] Resync: skipping incomplete frame {expected_frame} "
                        f"({len(buffer['packets'])}/{buffer['total_packets']} packets), advancing to next frame",
                        file=sys.stderr,
                    )
                    if expected_frame in self.frame_buffers:
                        del self.frame_buffers[expected_frame]
                    self.expected_frame_num += 1
                    expected_frame = self.expected_frame_num
                self.process_new_packet_from_data(expected_frame, packet_info)
                buffer = self.frame_buffers.get(expected_frame)
                if not buffer or len(buffer["packets"]) < buffer["total_packets"]:
                    continue
                try:
                    self.reconstruct_and_process_frame(expected_frame, buffer)
                except Exception as e:
                    print(f"❌ Error reconstructing frame {expected_frame}: {e}", file=sys.stderr)
                    import traceback
                    traceback.print_exc()
                if expected_frame in self.frame_buffers:
                    del self.frame_buffers[expected_frame]
                self.expected_frame_num += 1
                self.frames_processed_count += 1
                if self.frames_processed_count % 50 == 0 and self.frames_processed_count > 0:
                    rcvbuf = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
                    try:
                        buf_bytes = struct.unpack("I", fcntl.ioctl(conn.fileno(), termios.FIONREAD, struct.pack("I", 0)))[0]
                        print(
                            f"[SOCKET] Frame {self.frames_processed_count} processed, recv buffer: {rcvbuf} bytes, queued: {buf_bytes} bytes",
                            file=sys.stderr,
                        )
                    except (OSError, AttributeError):
                        print(
                            f"[SOCKET] Frame {self.frames_processed_count} processed, recv buffer: {rcvbuf} bytes, queued: N/A",
                            file=sys.stderr,
                        )
        finally:
            conn.close()
        print(f"✅ Socket connection closed for track_id={track_id}, processed {self.frames_processed_count} frames", file=sys.stderr)

    def parse_packet_file(self, packet_file):
        """Parse packet file"""
        with self.profiler.time_operation("read_packet_file"):
            try:
                with open(packet_file, "rb") as f:
                    data = f.read()
            except Exception as e:
                return None, f"Error reading file: {e}"
        return self._parse_packet_bytes(data)

    def _parse_packet_bytes(self, data):
        """Parse packet from bytes (23-byte header + payload). Used by file and bin stream."""
        with self.profiler.time_operation("parse_packet_file"):
            try:
                if len(data) < HEADER_SIZE:
                    return None, "Packet too small"

                (
                    sensor_timestamp,
                    wallclock_timestamp,
                    frame_size,
                    keyframe_flag,
                    packet_index,
                    total_packets,
                ) = struct.unpack(">QQIBBB", data[:HEADER_SIZE])

                payload = data[HEADER_SIZE:]

                return {
                    "sensor_timestamp": sensor_timestamp,
                    "wallclock_timestamp": wallclock_timestamp,
                    "frame_size": frame_size,
                    "is_keyframe": (keyframe_flag == 1),
                    "packet_index": packet_index,
                    "total_packets": total_packets,
                    "payload": payload,
                }, None

            except Exception as e:
                return None, f"Error parsing: {e}"

    def process_new_packet_from_data(self, frame_num, packet_info):
        """Process a packet from in-memory data (from bin stream). Updates frame_buffers."""
        with self.lock:
            if frame_num not in self.frame_buffers:
                self.frame_buffers[frame_num] = {
                    "packets": {},
                    "total_packets": packet_info["total_packets"],
                    "sensor_ts": packet_info["sensor_timestamp"],
                    "is_keyframe": packet_info["is_keyframe"],
                }
            buffer = self.frame_buffers[frame_num]
            buffer["packets"][packet_info["packet_index"]] = packet_info
            return True

    def process_new_packet_without_deletion(self, packet_file):
        """Process a packet file without deleting it (for sequential processing)"""
        with self.lock:
            frame_info = self.parse_frame_info_from_filename(packet_file)
            if not frame_info:
                print(
                    f"❌ Failed to parse frame info from filename: {packet_file}",
                    file=sys.stderr,
                )
                return False

            frame_num, packet_index, total_packets = frame_info

            if not packet_file.exists():
                print(f"❌ Packet file does not exist: {packet_file}", file=sys.stderr)
                return False

            packet_info, error = self.parse_packet_file(packet_file)
            if not packet_info:
                print(
                    f"❌ Failed to parse packet file {packet_file}: {error}",
                    file=sys.stderr,
                )
                return False

            if frame_num not in self.frame_buffers:
                self.frame_buffers[frame_num] = {
                    "packets": {},
                    "total_packets": packet_info["total_packets"],
                    "sensor_ts": packet_info["sensor_timestamp"],
                    "is_keyframe": packet_info["is_keyframe"],
                }

            buffer = self.frame_buffers[frame_num]
            buffer["packets"][packet_info["packet_index"]] = packet_info

            return True

    def reconstruct_and_process_frame(self, frame_num, buffer):
        """Reconstruct H264 frame and push to ffmpeg"""
        with self.profiler.time_operation("reconstruct_frame"):
            packets = sorted(buffer["packets"].items())
            h264_data = bytearray()
            for idx, packet_info in packets:
                h264_data.extend(packet_info["payload"])

        h264_bytes = bytes(h264_data)
        self.push_h264(h264_bytes)
        if frame_num % 50 == 0:
            print(
                f"📤 Pushed frame {frame_num} to ffmpeg (keyframe={buffer['is_keyframe']}), {len(h264_bytes)} bytes",
                file=sys.stderr,
            )

    def parse_frame_info_from_filename(self, filename):
        """Parse frame number, packet index, and total packets from filename"""
        try:
            stem = filename.stem
            parts = stem.split("_")
            if len(parts) == 3:
                frame_num = int(parts[0])
                packet_index = int(parts[1])
                total_packets = int(parts[2])
                return (frame_num, packet_index, total_packets)
        except (ValueError, IndexError):
            pass
        return None

    def _get_current_track_id_index(self):
        """Get current track_id_index from sink track_video_index.json (counter)."""
        try:
            import json

            track_index_file = _SCRIPT_DIR / "variable_files" / "track_video_index.json"
            if track_index_file.exists():
                with open(track_index_file, "r") as f:
                    data = json.load(f)
                    return data.get("counter", 0)
        except Exception as e:
            print(
                f"⚠️  Warning: Could not read track_video_index.json, using 0: {e}",
                file=sys.stderr,
            )
        return 0

    def reset_for_new_track(self):
        """
        Reset internal state so we can start processing a new track_id_index
        (called when we detect that track_video_index.json's counter changed).
        """
        # Close any existing CSV reader
        if self.csv_reader:
            try:
                self.csv_reader.__exit__(None, None, None)
            except Exception:
                pass
        self.csv_reader = None

        # Clear per-frame state
        self.csv_entries_buffer.clear()
        self.frame_buffers.clear()
        self.expected_frame_num = None
        self.frame_wait_start = None
        self._frame_read_start_time = None
        if hasattr(self, "_frame_total_start_time"):
            self._frame_total_start_time = None

        # Close bin stream for old track
        if self.bin_stream:
            self.bin_stream.close()
            self.bin_stream = None

        # Reset frame processing count for new track
        self.frames_processed_count = 0

        # If using dynamic output (default mode), update output_dir for new track
        if self.use_dynamic_output:
            new_track_id = self._get_current_track_id_index()
            new_output_dir = self.hls_base_dir / str(new_track_id)
            
            # Close old ffmpeg process if running
            if self.ffmpeg_process:
                try:
                    if self.ffmpeg_process.stdin:
                        self.ffmpeg_process.stdin.close()
                    self.ffmpeg_process.terminate()
                    self.ffmpeg_process.wait(timeout=2)
                except Exception as e:
                    print(f"⚠️  Warning: Error closing old ffmpeg: {e}", file=sys.stderr)
                    if self.ffmpeg_process:
                        try:
                            self.ffmpeg_process.kill()
                        except Exception:
                            pass
                self.ffmpeg_process = None
            
            # Update output directory
            self.output_dir = new_output_dir
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # Reinitialize ffmpeg with new output directory
            self._init_ffmpeg()
            
            # Reinitialize playlist in new directory
            self.playlist_path = self.output_dir / "playlist.m3u8"
            self.last_segment_num = -1
            self._init_playlist()
            
            print(
                f"📁 Switched HLS output to: {self.output_dir} (track_id_index={new_track_id})",
                file=sys.stderr,
            )

        # Re-initialize using latest track_id_index
        self.initialize_frame_counter()
        # Reset inactivity timer now that we're on a new track
        self.last_activity_ts = time.time()

    def initialize_csv_reader(self):
        """Initialize CSV reader for current track_id_index (legacy; used when single .bin not present)"""
        track_id_index = self._get_current_track_id_index()
        csv_file = self.packet_buffer_dir / f"{track_id_index}.txt"
        if csv_file.exists():
            self.csv_reader = LiveCSVReader(csv_file)
            self.csv_reader.__enter__()
            print(
                f"📄 Reading CSV file sequentially from beginning: {csv_file}",
                file=sys.stderr,
            )
        else:
            print(f"⚠️  CSV file not found: {csv_file}, will wait for it...", file=sys.stderr)

    def initialize_bin_stream(self):
        """Open single append-only .bin file for current track (length-prefixed packets)."""
        track_id_index = self._get_current_track_id_index()
        bin_path = self.packet_buffer_dir / f"{track_id_index}.bin"
        if self.bin_stream is not None:
            if self.bin_stream.bin_path == bin_path:
                return
            self.bin_stream.close()
            self.bin_stream = None
        self.bin_stream = BinPacketStream(bin_path)
        if self.bin_stream.open():
            print(
                f"📦 Reading length-prefixed packet stream: {bin_path}",
                file=sys.stderr,
            )
        else:
            print(f"⚠️  Bin file not found yet: {bin_path}, will retry...", file=sys.stderr)

    def initialize_frame_counter(self):
        """Set current track and expected frame. Prefer single .bin file per track."""
        track_id_index = self._get_current_track_id_index()

        # Remember which track we're currently processing
        self.current_track_id_index = track_id_index

        # Prefer single .bin per track (length-prefixed); fallback to CSV + per-packet .bin
        bin_path = self.packet_buffer_dir / f"{track_id_index}.bin"
        if bin_path.exists():
            self.initialize_bin_stream()
            self.expected_frame_num = 1
            print(
                "📊 Using single .bin stream; starting from frame 1",
                file=sys.stderr,
            )
        else:
            self.initialize_csv_reader()
            csv_file = self.packet_buffer_dir / f"{track_id_index}.txt"
            min_frame = None
            if csv_file.exists():
                try:
                    with open(csv_file, "r") as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith("==="):
                                parts = line.split(",")
                                if len(parts) == 3:
                                    try:
                                        frame_num, _, _ = map(int, parts)
                                        if min_frame is None or frame_num < min_frame:
                                            min_frame = frame_num
                                    except ValueError:
                                        pass
                except Exception as e:
                    print(f"⚠️  Warning: Could not read CSV file: {e}", file=sys.stderr)
            if min_frame is not None:
                self.expected_frame_num = min_frame
                print(
                    f"📊 Starting sequential processing from frame {min_frame} (from CSV)",
                    file=sys.stderr,
                )
            else:
                self.expected_frame_num = 1
                print(
                    "📊 No frames in CSV, starting from frame 1 (will wait for new frames)",
                    file=sys.stderr,
                )
        # Reset inactivity timer when we (re)start a track
        self.last_activity_ts = time.time()

    def process_sequential_packets_from_bin(self):
        """Process packets from single length-prefixed .bin file per track."""
        if self.expected_frame_num is None:
            return
        track_id_index = self._get_current_track_id_index()
        bin_path = self.packet_buffer_dir / f"{track_id_index}.bin"
        if not bin_path.exists():
            time.sleep(0.1)
            return
        if self.bin_stream is None:
            self.initialize_bin_stream()
        if self.bin_stream is None or not self.bin_stream.open():
            time.sleep(0.1)
            return
        packet_bytes = self.bin_stream.read_packet()
        if packet_bytes is None:
            time.sleep(0.05)
            return
        packet_info, error = self._parse_packet_bytes(packet_bytes)
        if not packet_info:
            return
        self.last_activity_ts = time.time()
        expected_frame = self.expected_frame_num
        self.process_new_packet_from_data(expected_frame, packet_info)
        buffer = self.frame_buffers.get(expected_frame)
        if not buffer or len(buffer["packets"]) < buffer["total_packets"]:
            return
        self.frame_wait_start = None
        frame_start_time = time.perf_counter()
        process_start_time = time.perf_counter()
        try:
            self.reconstruct_and_process_frame(expected_frame, buffer)
        except Exception as e:
            print(
                f"❌ Error reconstructing frame {expected_frame}: {e}",
                file=sys.stderr,
            )
            import traceback
            traceback.print_exc()
            self.expected_frame_num += 1
            if expected_frame in self.frame_buffers:
                del self.frame_buffers[expected_frame]
            return
        del self.frame_buffers[expected_frame]
        self.expected_frame_num += 1
        self.frames_processed_count += 1
        self.last_activity_ts = time.time()
        process_elapsed = (time.perf_counter() - process_start_time) * 1000
        frame_total_time = (time.perf_counter() - frame_start_time) * 1000
        print(
            f"✅ Frame {expected_frame}: {buffer['total_packets']} packets, "
            f"process={process_elapsed:.2f}ms total={frame_total_time:.2f}ms",
            file=sys.stderr,
        )

    def process_sequential_packets_from_csv(self):
        """Process packets sequentially based on CSV"""
        if not self.csv_reader:
            self.initialize_csv_reader()
            if not self.csv_reader:
                return

        if self.expected_frame_num is None:
            return

        expected_frame = self.expected_frame_num

        if expected_frame not in self.csv_entries_buffer:
            if not hasattr(self, "_frame_read_start_time") or self._frame_read_start_time is None:
                self._frame_read_start_time = time.perf_counter()
                if not hasattr(self, "_frame_total_start_time"):
                    self._frame_total_start_time = self._frame_read_start_time

        line = self.csv_reader.read_next_line()

        if not line:
            if expected_frame not in self.csv_entries_buffer:
                if self.frame_wait_start is None:
                    self.frame_wait_start = time.time()
                    print(
                        f"⏳ Waiting for frame {expected_frame} to appear in CSV...",
                        file=sys.stderr,
                    )
                else:
                    wait_time = time.time() - self.frame_wait_start
                    if int(wait_time) % 5 == 0:
                        print(
                            f"⏳ Still waiting for frame {expected_frame} (waited {wait_time:.1f}s)...",
                            file=sys.stderr,
                        )
            else:
                frame_packets = self.csv_entries_buffer[expected_frame]
                total_packets = next(iter(frame_packets.values()))
                found_indices = set(frame_packets.keys())
                expected_indices = set(range(total_packets))
                missing = expected_indices - found_indices
                wait_time = time.time() - self.frame_wait_start if self.frame_wait_start else 0
                if int(wait_time) % 5 == 0:
                    print(
                        f"⏳ Frame {expected_frame}: have {len(found_indices)}/{total_packets} packets "
                        f"(missing: {sorted(missing)}, waited {wait_time:.1f}s)",
                        file=sys.stderr,
                    )
            self.csv_reader.stay_on_current_line()
            time.sleep(0.1)
            return

        if line and not line.startswith("==="):
            parts = line.split(",")
            if len(parts) == 3:
                try:
                    frame_num, packet_index, total_packets = map(int, parts)
                except ValueError:
                    pass
                else:
                    if frame_num == expected_frame:
                        if frame_num not in self.csv_entries_buffer:
                            self.csv_entries_buffer[frame_num] = {}
                        self.csv_entries_buffer[frame_num][packet_index] = total_packets

                        # New packet entry for this frame -> update activity timer
                        self.last_activity_ts = time.time()

                        frame_packets = self.csv_entries_buffer[expected_frame]
                        expected_indices = set(range(total_packets))
                        found_indices = set(frame_packets.keys())

                        if found_indices == expected_indices:
                            self.frame_wait_start = None

                            if (
                                hasattr(self, "_frame_read_start_time")
                                and self._frame_read_start_time is not None
                            ):
                                read_time = (
                                    time.perf_counter() - self._frame_read_start_time
                                ) * 1000
                                print(
                                    f"📋 Frame {frame_num}: Read and buffered {total_packets} packets, "
                                    f"elapsed={read_time:.2f}ms",
                                    file=sys.stderr,
                                )
                                self._frame_read_start_time = None
                        else:
                            return
                    else:
                        if expected_frame in self.csv_entries_buffer:
                            frame_packets = self.csv_entries_buffer[expected_frame]
                            if frame_packets:
                                total_packets_prev = next(iter(frame_packets.values()))
                                expected_indices_prev = set(range(total_packets_prev))
                                found_indices_prev = set(frame_packets.keys())

                                if found_indices_prev != expected_indices_prev:
                                    self.csv_reader.stay_on_current_line()
                                    return

                        if frame_num not in self.csv_entries_buffer:
                            self.csv_entries_buffer[frame_num] = {}

                        self.csv_entries_buffer[frame_num][packet_index] = total_packets

                        # New packet entry for a new frame -> update activity timer
                        self.last_activity_ts = time.time()

                        self.expected_frame_num = frame_num
                        self.frame_wait_start = None
                        self._frame_read_start_time = time.perf_counter()
                        return

        if expected_frame not in self.csv_entries_buffer:
            self.csv_reader.stay_on_current_line()
            return

        frame_packets = self.csv_entries_buffer[expected_frame]
        if not frame_packets:
            self.csv_reader.stay_on_current_line()
            return

        total_packets = next(iter(frame_packets.values()))
        expected_indices = set(range(total_packets))
        found_indices = set(frame_packets.keys())

        if found_indices != expected_indices:
            self.csv_reader.stay_on_current_line()
            return

        track_id_index = self._get_current_track_id_index()
        track_id_index_dir = self.packet_buffer_dir / str(track_id_index)
        packets_by_index = {}

        for packet_index in range(total_packets):
            packet_path = track_id_index_dir / f"{expected_frame}_{packet_index}_{total_packets}.bin"
            if not packet_path.exists():
                time.sleep(0.01)
                self.csv_reader.stay_on_current_line()
                return
            packets_by_index[packet_index] = packet_path

        self.frame_wait_start = None
        if not hasattr(self, "_frame_total_start_time") or self._frame_total_start_time is None:
            self._frame_total_start_time = time.perf_counter()
        frame_start_time = time.perf_counter()

        process_start_time = time.perf_counter()
        for packet_index in range(total_packets):
            packet_file = packets_by_index[packet_index]
            success = self.process_new_packet_without_deletion(packet_file)
            if not success:
                print(
                    f"❌ Frame {expected_frame}: Error processing packet {packet_index}, skipping frame",
                    file=sys.stderr,
                )
                self.expected_frame_num += 1
                self.frame_wait_start = None
                return
        process_end_time = time.perf_counter()
        process_elapsed = (process_end_time - process_start_time) * 1000
        print(
            f"📦 Frame {expected_frame}: Processed {total_packets} packets, elapsed={process_elapsed:.2f}ms",
            file=sys.stderr,
        )

        if expected_frame in self.frame_buffers:
            buffer = self.frame_buffers[expected_frame]
            if len(buffer["packets"]) >= buffer["total_packets"]:
                try:
                    reconstruct_start_time = time.perf_counter()
                    self.reconstruct_and_process_frame(expected_frame, buffer)
                    reconstruct_end_time = time.perf_counter()
                    reconstruct_elapsed = (
                        reconstruct_end_time - reconstruct_start_time
                    ) * 1000
                    print(
                        f"🔧 Frame {expected_frame}: Reconstructed frame, elapsed={reconstruct_elapsed:.2f}ms",
                        file=sys.stderr,
                    )

                    del self.frame_buffers[expected_frame]
                    del self.csv_entries_buffer[expected_frame]
                    self.expected_frame_num += 1

                    frame_end_time = time.perf_counter()
                    if (
                        hasattr(self, "_frame_total_start_time")
                        and self._frame_total_start_time is not None
                    ):
                        frame_total_time = (
                            frame_end_time - self._frame_total_start_time
                        ) * 1000
                        self._frame_total_start_time = None
                    else:
                        frame_total_time = (frame_end_time - frame_start_time) * 1000
                    print(
                        f"✅ Frame {expected_frame}: {total_packets} packets, total={frame_total_time:.2f}ms",
                        file=sys.stderr,
                    )
                    # Completed a frame successfully – update activity timer
                    self.last_activity_ts = time.time()
                    # Increment frame count (we've processed at least one frame now)
                    self.frames_processed_count += 1
                except Exception as e:
                    print(f"❌ Error reconstructing frame {expected_frame}: {e}", file=sys.stderr)
                    import traceback

                    traceback.print_exc()

    def process_sequential_packets(self):
        """Process one unit of work: prefer single .bin stream, else CSV + per-packet .bin."""
        track_id_index = self._get_current_track_id_index()
        bin_path = self.packet_buffer_dir / f"{track_id_index}.bin"
        if bin_path.exists():
            self.process_sequential_packets_from_bin()
        else:
            self.process_sequential_packets_from_csv()


class LiveHLSCreator:
    """Main class for live HLS creation"""

    def __init__(self, packet_buffer_dir, output_dir, segment_duration=4.0, clear_dirs=False):
        self.processor = LivePacketProcessor(
            packet_buffer_dir, output_dir, segment_duration, clear_dirs
        )

    def start(self, process_existing=True):
        """Start processing packets using polling with sequential frame processing"""
        self.processor.initialize_frame_counter()

        if process_existing:
            self.process_existing_packets_sequential()

        print("🎥 Live HLS generation started. Polling for packets...\n", file=sys.stderr)
        print("📺 Playlist: playlist.m3u8", file=sys.stderr)
        print(
            "🗑️  Packet files will be deleted after successful frame processing",
            file=sys.stderr,
        )
        print(
            "⏳ Waiting for packets to arrive (will wait indefinitely)...",
            file=sys.stderr,
        )
        print("⏹️  Press Ctrl+C or send SIGTERM/SIGINT to stop\n", file=sys.stderr)

        shutdown_flag = threading.Event()

        def signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            print(
                f"\n🛑 Received signal {signal_name} ({signum}), stopping gracefully...",
                file=sys.stderr,
            )
            shutdown_flag.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            last_playlist_check = time.time()
            playlist_check_interval = 1.0

            while not shutdown_flag.is_set():
                with self.processor.profiler.time_operation("main_loop_iteration"):
                    self.processor.process_sequential_packets()

                current_time = time.time()
                if current_time - last_playlist_check >= playlist_check_interval:
                    self.processor.check_and_update_playlist()
                    last_playlist_check = current_time

                # Only check for inactivity timeout if we've processed at least one frame.
                # If CSV doesn't exist and we haven't processed any frames, wait indefinitely.
                if self.processor.frames_processed_count > 0:
                    inactivity = current_time - self.processor.last_activity_ts
                    if inactivity >= 5.0:
                        cur_track = self.processor.current_track_id_index
                        print(
                            f"⏸️  No new packets for {inactivity:.1f}s on track_id_index={cur_track} "
                            f"(processed {self.processor.frames_processed_count} frames), "
                            "waiting for next track_id_index...",
                            file=sys.stderr,
                        )

                        # Idle loop: poll track_video_index.json every 1 second until
                        # track_id_index changes, then reset processor for new track.
                        while not shutdown_flag.is_set():
                            latest = self.processor._get_current_track_id_index()
                            if latest != cur_track:
                                print(
                                    f"🔁 Detected new track_id_index={latest} (was {cur_track}), "
                                    "switching packet_buffer processing to new capture",
                                    file=sys.stderr,
                                )
                                self.processor.reset_for_new_track()
                                break
                            time.sleep(1.0)

                        # After switching tracks (or if shutting down), continue main loop
                        # which will now process the new track.

                if shutdown_flag.is_set():
                    break

        except KeyboardInterrupt:
            print("\n🛑 Stopping...", file=sys.stderr)
            shutdown_flag.set()
        finally:
            self.processor.cleanup()

            print(f"\n✅ Stopped.", file=sys.stderr)
            print(f"📁 Output directory: {self.processor.output_dir}", file=sys.stderr)

    def process_existing_packets_sequential(self):
        """Process any existing packet files sequentially (and delete them)"""
        max_iterations = 10000
        iteration = 0

        print(
            f"📦 Processing existing packets sequentially from {self.processor.packet_buffer_dir}...",
            file=sys.stderr,
        )

        while iteration < max_iterations:
            iteration += 1
            prev_expected_frame_num = self.processor.expected_frame_num

            self.processor.process_sequential_packets()

            if self.processor.expected_frame_num == prev_expected_frame_num:
                track_id_index = self.processor._get_current_track_id_index()
                bin_path = self.processor.packet_buffer_dir / f"{track_id_index}.bin"
                if bin_path.exists():
                    # Single .bin: no "remaining" check; main loop will keep reading as more is appended
                    break
                csv_file = self.processor.packet_buffer_dir / f"{track_id_index}.txt"
                has_more = csv_file.exists() and csv_file.stat().st_size > 0
                if not has_more:
                    break
                break

        track_id_index = self.processor._get_current_track_id_index()
        bin_path = self.processor.packet_buffer_dir / f"{track_id_index}.bin"
        if bin_path.exists():
            has_remaining = False  # Bin stream: continuous append, no "remaining" concept
        else:
            csv_file = self.processor.packet_buffer_dir / f"{track_id_index}.txt"
            has_remaining = csv_file.exists() and csv_file.stat().st_size > 0
        if has_remaining:
            print(
                "✅ Processed existing packets sequentially. Some packets remaining (will process in main loop)\n",
                file=sys.stderr,
            )
        else:
            print("✅ Processed and cleaned up all existing packets\n", file=sys.stderr)


def run_socket_server(socket_path, segment_duration=4.0):
    """Listen on Unix socket; for each connection receive track_id and packets, push to HLS. Never returns."""
    sock_path = str(socket_path)
    if os.path.exists(sock_path):
        try:
            os.unlink(sock_path)
        except Exception as e:
            print(f"⚠️  Could not unlink existing socket {sock_path}: {e}", file=sys.stderr)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(sock_path)
    server.listen(1)
    print(f"📡 Listening on {sock_path} for C++ packet stream (local_ts_segments)", file=sys.stderr)
    packet_buffer_dir = str(_SCRIPT_DIR / "packet_buffer_for_ts")
    output_base = str(_SCRIPT_DIR / "ts_segments" / "0")  # so processor has hls_base_dir = ts_segments
    processor = LivePacketProcessor(packet_buffer_dir, output_base, segment_duration)

    # In socket mode, packet processing blocks in processor.run_from_socket(...),
    # so the periodic playlist updater in the polling loop never runs.
    # Keep the playlist updated in the background.
    def _playlist_updater():
        while True:
            try:
                processor.check_and_update_playlist()
            except Exception as e:
                print(f"⚠️  Playlist updater error: {e}", file=sys.stderr)
            time.sleep(1.0)

    threading.Thread(target=_playlist_updater, daemon=True).start()

    while True:
        try:
            conn, _ = server.accept()
        except OSError as e:
            print(f"⚠️  accept() failed: {e}", file=sys.stderr)
            continue
        try:
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
            rcvbuf = conn.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            print(f"[SOCKET] Receive buffer: requested 4MB, got {rcvbuf} bytes ({rcvbuf / (1024*1024):.2f} MB)", file=sys.stderr)
            if rcvbuf < 2 * 1024 * 1024:
                print(
                    "[SOCKET] ⚠️  Buffer capped by kernel. Run: sudo sysctl -w net.core.rmem_max=8388608  (see docs/SOCKET_SETUP.md)",
                    file=sys.stderr,
                )
            buf = b""
            while len(buf) < 4:
                chunk = conn.recv(4 - len(buf))
                if not chunk:
                    conn.close()
                    continue
                buf += chunk
            track_id = struct.unpack(">I", buf)[0]
            print(f"📡 New connection: track_id={track_id}", file=sys.stderr)
            processor.run_from_socket(conn, track_id)
        except Exception as e:
            print(f"❌ Error handling connection: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            try:
                conn.close()
            except Exception:
                pass


def main():
    """Entry point. Requires local_ts_segments=true in config; runs socket server only."""
    try:
        import av  # noqa: F401
    except ImportError:
        print("❌ PyAV not installed. Install with: pip install av", file=sys.stderr)
        sys.exit(1)

    default_segment_duration = 4.0

    if not _is_local_ts_segments():
        print(
            "❌ Local TS segments is disabled in variable_files/udp_streaming_config.json.",
            file=sys.stderr,
        )
        print(
            '   Set "local_ts_segments": true for HLS/TS via socket.',
            file=sys.stderr,
        )
        sys.exit(1)

    socket_path = _get_local_ts_socket_path()
    run_socket_server(socket_path, default_segment_duration)


if __name__ == "__main__":
    main()

