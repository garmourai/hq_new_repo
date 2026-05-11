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
import subprocess
import socket
import fcntl
import termios
from pathlib import Path
import sys
import av  # PyAV - same as inspire.py
import threading
from datetime import datetime
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

        # Track which track_id_index we are currently processing
        self.current_track_id_index = None

        self.frames_processed_count = 0
        self.streamed_packets_dir = _SCRIPT_DIR / "streamed_packets"
        self.streamed_packets_dir.mkdir(parents=True, exist_ok=True)
        self.dropped_frames_csv = self.streamed_packets_dir / "0" / "hls_dropped_frames.csv"
        self.frame_log_csv = self.streamed_packets_dir / "0" / "hls_frame_log.csv"
        self.ffmpeg_warnings_csv = self.streamed_packets_dir / "0" / "hls_ffmpeg_warnings.csv"
        self._last_pushed_frame_num = 0
        self._frame_log_file = None
        self._set_track_log_paths(0)

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

        print(f"📁 Watching: {self.packet_buffer_dir}", file=sys.stderr)
        print(f"📁 Output: {self.output_dir}", file=sys.stderr)
        print(
            f"⏱️  FFmpeg will create TS segments every {self.segment_duration} seconds\n",
            file=sys.stderr,
        )

    def _open_frame_log(self):
        """Ensure frame log file is open (line-buffered append)."""
        if self._frame_log_file is None:
            self._frame_log_file = open(self.frame_log_csv, "a", buffering=1)

    def _set_track_log_paths(self, track_id):
        """Point logs to streamed_packets/<track_id>/ and ensure CSV headers exist."""
        track_dir = self.streamed_packets_dir / str(track_id)
        track_dir.mkdir(parents=True, exist_ok=True)
        self.dropped_frames_csv = track_dir / "hls_dropped_frames.csv"
        self.frame_log_csv = track_dir / "hls_frame_log.csv"
        self.ffmpeg_warnings_csv = track_dir / "hls_ffmpeg_warnings.csv"

        if not self.dropped_frames_csv.exists():
            with open(self.dropped_frames_csv, "w") as f:
                f.write("frame_num,reason,sensor_ts_ns,wall_ts_ns,detail\n")
        if not self.frame_log_csv.exists():
            with open(self.frame_log_csv, "w") as f:
                f.write("frame_num,sensor_ts_ns,wall_ts_ns,is_keyframe,size_bytes,packet_count\n")

        # Rotate frame log handle to the current track file.
        if self._frame_log_file:
            try:
                self._frame_log_file.flush()
                self._frame_log_file.close()
            except Exception:
                pass
            self._frame_log_file = None
        self._open_frame_log()

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
                    "repeat+warning",
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
                    "-segment_list",
                    "playlist.m3u8",
                    "-segment_list_type",
                    "m3u8",
                    "-segment_list_flags",
                    "+live",
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
                            decoded = line.decode('utf-8', errors='ignore').strip()
                            ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                            frame_approx = self._last_pushed_frame_num
                            print(f"FFmpeg: {decoded}", file=sys.stderr)
                            try:
                                with open(self.ffmpeg_warnings_csv, "a") as wf:
                                    safe = decoded.replace('"', '""')
                                    wf.write(f'{ts},{frame_approx},"{safe}"\n')
                            except Exception:
                                pass

            stderr_thread = threading.Thread(target=read_stderr, daemon=True)
            stderr_thread.start()

            print("✅ FFmpeg subprocess initialized for TS segment creation", file=sys.stderr)
        except Exception as e:
            print(f"❌ Failed to initialize ffmpeg subprocess: {e}", file=sys.stderr)
            import traceback

            traceback.print_exc()
            raise

    def push_h264(self, nal_bytes):
        """Push H.264 NAL units to ffmpeg subprocess. Exits process on any FFmpeg failure — no restart."""
        if not self.ffmpeg_process or not self.ffmpeg_process.stdin:
            print("\n[FATAL] FFmpeg process not available. Exiting.", file=sys.stderr)
            sys.exit(1)

        if self.ffmpeg_process.poll() is not None:
            rc = self.ffmpeg_process.returncode
            print(f"\n[FATAL] FFmpeg exited unexpectedly (return code: {rc}).", file=sys.stderr)
            print(f"[FATAL] Likely cause: disk full, output directory issue, or external kill.", file=sys.stderr)
            print(f"[FATAL] All segments written so far are preserved. Exiting cleanly.", file=sys.stderr)
            sys.exit(1)

        try:
            self.ffmpeg_process.stdin.write(nal_bytes)
            self.ffmpeg_process.stdin.flush()
        except BrokenPipeError:
            print(f"\n[FATAL] FFmpeg stdin broken pipe — FFmpeg process died while writing.", file=sys.stderr)
            print(f"[FATAL] Likely cause: disk full, output directory issue, or external kill.", file=sys.stderr)
            print(f"[FATAL] All segments written so far are preserved. Exiting cleanly.", file=sys.stderr)
            sys.exit(1)
        except OSError as e:
            print(f"\n[FATAL] FFmpeg stdin write error: {e}", file=sys.stderr)
            print(f"[FATAL] All segments written so far are preserved. Exiting cleanly.", file=sys.stderr)
            sys.exit(1)

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
        if hasattr(self, '_frame_log_file') and self._frame_log_file:
            try:
                self._frame_log_file.flush()
                self._frame_log_file.close()
            except Exception:
                pass
            self._frame_log_file = None

    def _switch_output_to_track(self, track_id):
        """Switch HLS output to ts_segments/<track_id>. Used when receiving a new connection in socket mode."""
        base = self.hls_base_dir if self.hls_base_dir is not None else _SCRIPT_DIR / "ts_segments"
        self.output_dir = Path(base) / str(track_id)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._set_track_log_paths(track_id)
        if self.ffmpeg_process:
            self.cleanup()
        self._init_ffmpeg()
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
                    got = len(buffer["packets"])
                    total = buffer["total_packets"]
                    print(
                        f"[SOCKET] Resync: skipping incomplete frame {expected_frame} "
                        f"({got}/{total} packets), advancing to next frame",
                        file=sys.stderr,
                    )
                    self._log_dropped_frame(
                        expected_frame, "resync_incomplete",
                        buffer.get("sensor_ts", 0),
                        buffer["packets"][min(buffer["packets"])]["wallclock_timestamp"] if buffer["packets"] else 0,
                        f"got={got} expected={total}",
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
                    sensor_ts = buffer.get("sensor_ts", 0) if buffer else 0
                    wall_ts = buffer["packets"][min(buffer["packets"])]["wallclock_timestamp"] if buffer and buffer["packets"] else 0
                    self._log_dropped_frame(expected_frame, "reconstruction_exception", sensor_ts, wall_ts, str(e))
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

    def _log_dropped_frame(self, frame_num, reason, sensor_ts, wall_ts, detail=""):
        """Append one line to dropped_frames.csv."""
        try:
            with open(self.dropped_frames_csv, "a") as f:
                f.write(f'{frame_num},{reason},{sensor_ts},{wall_ts},"{detail}"\n')
        except Exception as e:
            print(f"⚠️  Could not write dropped_frames.csv: {e}", file=sys.stderr)

    def reconstruct_and_process_frame(self, frame_num, buffer):
        """Reconstruct H264 frame and push to ffmpeg only if complete and size-validated."""
        sensor_ts = buffer.get("sensor_ts", 0)
        wall_ts = buffer["packets"][min(buffer["packets"])]["wallclock_timestamp"]

        # 1. Validate all packet indices 0..total_packets-1 are present
        expected_indices = set(range(buffer["total_packets"]))
        got_indices = set(buffer["packets"].keys())
        if got_indices != expected_indices:
            missing = sorted(expected_indices - got_indices)
            self._log_dropped_frame(frame_num, "missing_packet_indices", sensor_ts, wall_ts,
                                    f"missing={missing} got={sorted(got_indices)}")
            print(f"⚠️  Frame {frame_num} dropped: missing packet indices {missing}", file=sys.stderr)
            return

        with self.profiler.time_operation("reconstruct_frame"):
            packets = sorted(buffer["packets"].items())
            h264_data = bytearray()
            for idx, packet_info in packets:
                h264_data.extend(packet_info["payload"])

        # 2. Validate total payload size matches frame_size from header
        expected_size = buffer["packets"][0]["frame_size"]
        actual_size = len(h264_data)
        if actual_size != expected_size:
            self._log_dropped_frame(frame_num, "size_mismatch", sensor_ts, wall_ts,
                                    f"expected={expected_size} actual={actual_size}")
            print(f"⚠️  Frame {frame_num} dropped: size mismatch expected={expected_size} actual={actual_size}", file=sys.stderr)
            return

        h264_bytes = bytes(h264_data)
        self._last_pushed_frame_num = frame_num
        self.push_h264(h264_bytes)
        if self._frame_log_file is None:
            self._open_frame_log()
        self._frame_log_file.write(f"{frame_num},{sensor_ts},{wall_ts},{int(buffer['is_keyframe'])},{len(h264_bytes)},{buffer['total_packets']}\n")
        if frame_num % 30 == 0:
            self._frame_log_file.flush()
        if frame_num % 50 == 0:
            print(
                f"📤 Pushed frame {frame_num} to ffmpeg (keyframe={buffer['is_keyframe']}), {len(h264_bytes)} bytes",
                file=sys.stderr,
            )



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

