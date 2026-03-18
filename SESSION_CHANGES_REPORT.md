# Session changes report: single flag `local_ts_segments`, socket-only local TS

Use this report to apply the same changes to other branches.  
**Goal:** One config flag `local_ts_segments`; local HLS/TS via Unix socket only (no .bin/.txt files).

---

## 1. Config JSON

**File:** `variable_files/udp_streaming_config.json`

**Replace** any `local_packet_buffer_enabled` / `local_ts_via_socket` with a single flag:

```json
{"udp_streaming_enabled":true,"local_ts_segments":true,"local_ts_socket_path":"/tmp/rpicam_hls.sock","udp_destination":"udp://192.168.1.100:5002"}
```

- **Remove:** `local_packet_buffer_enabled`, `local_ts_via_socket`
- **Add/keep:** `local_ts_segments` (boolean), `local_ts_socket_path`, `udp_destination`, `udp_streaming_enabled`

---

## 2. C++ net_output.cpp

**File:** `rpicam-apps/output/net_output.cpp`

### 2.1 Static variables and setters (top of file, after includes)

**Remove:**  
- `frame_log_stream`, `frame_log_mutex`, `packet_append_stream`, `packet_append_mutex`  
- `get_frame_log_file_path()`  
- `g_local_packet_buffer_enabled`, `g_local_ts_via_socket`  
- `set_local_packet_buffer_enabled`, `set_local_ts_via_socket`  
- `global_frame_counter`, `frame_counter_mutex`, `current_track_id_index`, `track_id_mutex` (and any code that only wrote to them)  
- `PACKET_BUFFER_DIR` and `create_directories(PACKET_BUFFER_DIR)` in constructor  

**Keep/use this pattern:**

```cpp
// Static variables for packet / local TS and CSV
namespace {
    const std::string STREAMED_PACKETS_DIR = "/home/pi/source_code/streamed_packets";
}

// Global NetOutput* so reset_frame_counter can call openCSVLogsForTrack
static NetOutput* g_net_output = nullptr;

// Single flag: when true, push packets to Python via Unix socket for local HLS/TS segments. No .bin/.txt files.
static std::atomic<bool> g_udp_send_enabled(true);
static std::atomic<bool> g_local_ts_segments(false);
static std::string g_local_ts_socket_path = "/tmp/rpicam_hls.sock";
static int g_local_ts_socket = -1;
static std::mutex g_local_ts_socket_mutex;

void set_udp_send_enabled(bool enabled) { g_udp_send_enabled = enabled; }
void set_local_ts_segments(bool enabled) { g_local_ts_segments = enabled; }
void set_local_ts_socket_path(const std::string& path) { g_local_ts_socket_path = path; }
```

### 2.2 reset_frame_counter()

**Behavior:**  
- No file creation (.txt/.bin).  
- When `g_local_ts_segments` is true: close existing socket, create AF_UNIX socket, connect to `g_local_ts_socket_path`, send 4-byte big-endian `track_id_index`, store fd in `g_local_ts_socket`.  
- Always call `g_net_output->openCSVLogsForTrack(track_id_index)` when `g_net_output` is non-null.

**Segment to use:**

```cpp
// Function to reset frame counter (exposed for rpicam_source.cpp)
// Called when capture starts: open CSV logs for track and connect to local TS socket if enabled.
void reset_frame_counter(int track_id_index) {
    // Open CSV logs for this track (streamed_packets/<track_id>/sender_packets.csv, sender_failed_packets.csv)
    if (g_net_output) {
        g_net_output->openCSVLogsForTrack(track_id_index);
    }
    
    // When local_ts_segments is true: connect to Python via Unix socket and send track_id (no .bin/.txt files)
    if (g_local_ts_segments.load()) {
        std::lock_guard<std::mutex> sock_lock(g_local_ts_socket_mutex);
        if (g_local_ts_socket >= 0) {
            close(g_local_ts_socket);
            g_local_ts_socket = -1;
        }
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd >= 0) {
            struct sockaddr_un addr = {};
            addr.sun_family = AF_UNIX;
            size_t path_len = std::min(g_local_ts_socket_path.size(), sizeof(addr.sun_path) - 1);
            std::memcpy(addr.sun_path, g_local_ts_socket_path.c_str(), path_len);
            addr.sun_path[path_len] = '\0';
            if (connect(fd, reinterpret_cast<const struct sockaddr*>(&addr), sizeof(addr)) == 0) {
                g_local_ts_socket = fd;
                uint32_t tid_be = htonl(static_cast<uint32_t>(track_id_index));
                ssize_t n = send(fd, &tid_be, 4, MSG_NOSIGNAL);
                if (n != 4) {
                    std::cerr << "[LOCAL_TS] WARNING: Failed to send track_id on socket" << std::endl;
                    close(g_local_ts_socket);
                    g_local_ts_socket = -1;
                } else {
                    std::cerr << "[LOCAL_TS] Connected to " << g_local_ts_socket_path << ", track_id=" << track_id_index << std::endl;
                }
            } else {
                std::cerr << "[LOCAL_TS] Connect to " << g_local_ts_socket_path << " failed (is packet_buffer_to_hls.py running?): " << strerror(errno) << std::endl;
                close(fd);
            }
        } else {
            std::cerr << "[LOCAL_TS] socket(AF_UNIX) failed: " << strerror(errno) << std::endl;
        }
    }
}
```

### 2.3 close_frame_log()

**Behavior:** Only close the local TS socket. No file flush/close.

```cpp
// Function to close local TS socket (exposed for rpicam_source.cpp). Called when capture stops.
void close_frame_log() {
    std::lock_guard<std::mutex> sock_lock(g_local_ts_socket_mutex);
    if (g_local_ts_socket >= 0) {
        close(g_local_ts_socket);
        g_local_ts_socket = -1;
    }
}
```

### 2.4 Destructor ~NetOutput()

**Remove:** Any block that closes `frame_log_stream` or `packet_append_stream`.  
**Keep:** Closing CSV logs and `fd_`; call `close_frame_log()` from rpicam_source on stop, not necessarily in destructor.

### 2.5 sendPacket() – local TS path

**Remove:** All writes to frame log (.txt) and packet append (.bin).  
**Keep:** Only socket send when `g_local_ts_segments` is true:

```cpp
			// When local_ts_segments is true: send packet to Python via Unix socket only (no .bin/.txt)
			if (g_local_ts_segments.load()) {
				uint32_t len = static_cast<uint32_t>(packet_size);
				uint32_t len_be = htonl(len);
				std::lock_guard<std::mutex> sock_lock(g_local_ts_socket_mutex);
				if (g_local_ts_socket >= 0) {
					char buf[4];
					memcpy(buf, &len_be, 4);
					ssize_t n = send(g_local_ts_socket, buf, 4, MSG_NOSIGNAL);
					if (n == 4)
						n = send(g_local_ts_socket, packet, packet_size, MSG_NOSIGNAL);
					if (n != (ssize_t)packet_size) {
						close(g_local_ts_socket);
						g_local_ts_socket = -1;
					}
				}
			}
```

---

## 3. C++ rpicam_source.cpp

**File:** `rpicam-apps/apps/rpicam_source.cpp`

### 3.1 Externs

**Replace** old setters with:

```cpp
extern void reset_frame_counter(int track_id_index);
extern void close_frame_log();
extern void set_udp_send_enabled(bool);
extern void set_local_ts_segments(bool);
extern void set_local_ts_socket_path(const std::string& path);
```

(Remove `set_local_packet_buffer_enabled`, `set_local_ts_via_socket`.)

### 3.2 UdpStreamingConfig and read_udp_streaming_config()

**Struct:** Use a single flag and socket path:

```cpp
// Streaming config: read from variable_files/udp_streaming_config.json
// - udp_streaming_enabled: send packets over UDP to udp_destination
// - local_ts_segments: push packets to Python via Unix socket for local HLS/TS (packet_buffer_to_hls.py). No .bin/.txt files.
struct UdpStreamingConfig {
    bool udp_streaming_enabled = false;
    bool local_ts_segments = false;
    std::string destination = "udp://192.168.1.100:5002";
    std::string local_ts_socket_path = "/tmp/rpicam_hls.sock";
};
static UdpStreamingConfig read_udp_streaming_config() {
    UdpStreamingConfig cfg;
    std::string path = "/home/pi/source_code/variable_files/udp_streaming_config.json";
    try {
        std::ifstream f(path);
        if (!f.is_open())
            return cfg;
        json j = json::parse(f);
        cfg.udp_streaming_enabled = j.value("udp_streaming_enabled", false);
        cfg.local_ts_segments = j.value("local_ts_segments", false);
        cfg.destination = j.value("udp_destination", cfg.destination);
        cfg.local_ts_socket_path = j.value("local_ts_socket_path", cfg.local_ts_socket_path);
    } catch (const std::exception& e) {
        std::cerr << "Streaming config read failed: " << e.what() << std::endl;
    }
    return cfg;
}
```

### 3.3 NetOutput creation and capture start

- `use_net_output = udp_cfg.udp_streaming_enabled || udp_cfg.local_ts_segments`
- Call `set_local_ts_segments(udp_cfg.local_ts_segments)` and `set_local_ts_socket_path(udp_cfg.local_ts_socket_path)` (no `set_local_packet_buffer_enabled` / `set_local_ts_via_socket`).
- When `udp_cfg.local_ts_segments`: call `reset_frame_counter(track_id_index)` and log that packets are pushed via socket.

```cpp
        // Streaming: create NetOutput when UDP and/or local TS segments (socket) is enabled
        UdpStreamingConfig udp_cfg = read_udp_streaming_config();
        std::unique_ptr<Output> net_output;
        bool use_net_output = udp_cfg.udp_streaming_enabled || udp_cfg.local_ts_segments;
        if (use_net_output) {
            set_udp_send_enabled(udp_cfg.udp_streaming_enabled);
            set_local_ts_segments(udp_cfg.local_ts_segments);
            set_local_ts_socket_path(udp_cfg.local_ts_socket_path);
            VideoOptions net_options = *options;
            net_options.output = udp_cfg.destination;
            net_options.metadata.clear();
            net_output = std::unique_ptr<Output>(Output::Create(&net_options));
            if (udp_cfg.local_ts_segments) {
                reset_frame_counter(track_id_index);
                std::cerr << "[LOCAL_TS] Pushing packets via socket to packet_buffer_to_hls.py" << std::endl;
            }
            if (udp_cfg.udp_streaming_enabled)
                std::cerr << "[UDP] Streaming enabled to " << udp_cfg.destination << std::endl;
            else if (!udp_cfg.local_ts_segments)
                std::cerr << "[STREAMING] Both UDP and local TS disabled (file output only)" << std::endl;
        } else {
            std::cerr << "[STREAMING] NetOutput disabled (file output only)" << std::endl;
        }
```

### 3.4 Capture stop

When capture stops (timeout/frameout/stop_capture), call `close_frame_log()` only if `udp_cfg.local_ts_segments`:

```cpp
                if (udp_cfg.local_ts_segments)
                    close_frame_log();
```

---

## 4. Python packet_buffer_to_hls.py

**File:** `packet_buffer_to_hls.py` (project root or same repo path)

### 4.1 Docstring (top)

Describe socket-only and single flag:

```python
"""
Live Packet Buffer to HLS Converter (Streaming-Friendly, No FFmpeg wrapper outside)

Path-independent: all paths are relative to the directory containing this script.
- variable_files/udp_streaming_config.json – local_ts_segments must be true; C++ pushes packets via Unix socket (no .bin/.txt files).
- variable_files/track_video_index.json – track_id_index (counter)
- ts_segments/<track_id>/ – HLS output (m3u8 + .ts)
"""
```

### 4.2 Config helpers

**Replace** `_is_local_packet_buffer_enabled()` and `_is_local_ts_via_socket()` with one helper:

```python
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
```

### 4.3 main()

- If `local_ts_segments` is false: print that local TS is disabled and exit.  
- If true: run only the socket server (no file-based or CLI-based .bin/.txt path).

```python
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
```

- **Remove:** CLI parsing for `<packet_buffer_dir> <output_dir> [segment_duration]` and any code path that starts `LiveHLSCreator` or reads from .bin/.txt.  
- **Remove:** Any duplicate second script block at the end of the file (old .bin-based docstring and `main()`).

### 4.4 Socket server log message

Use a single flag in the message, e.g.:

```python
print(f"📡 Listening on {sock_path} for C++ packet stream (local_ts_segments)", file=sys.stderr)
```

---

## 5. Shell script (optional)

**File:** `prepare_camera_source_check.sh` (or equivalent)

Update the HLS session comment so it reflects socket-only:

```bash
# Create and start tmux session for HLS (socket -> ts_segments via packet_buffer_to_hls.py)
```

---

## Checklist for another branch

- [ ] Config: `udp_streaming_config.json` has `local_ts_segments` only (no `local_packet_buffer_enabled` / `local_ts_via_socket`).
- [ ] net_output.cpp: single flag `g_local_ts_segments`, setters `set_local_ts_segments` / `set_local_ts_socket_path`; no .bin/.txt streams or frame log; `reset_frame_counter` only opens CSV and socket; `close_frame_log` only closes socket; `sendPacket` sends to socket only when `g_local_ts_segments`.
- [ ] rpicam_source.cpp: `UdpStreamingConfig` has `local_ts_segments`; read from JSON; externs and setter calls updated; `reset_frame_counter` and `close_frame_log` used only when `local_ts_segments`.
- [ ] packet_buffer_to_hls.py: `_is_local_ts_segments()` only; `main()` exits if disabled, else runs `run_socket_server` only; no duplicate script block at end.
- [ ] Build: `ninja` in rpicam-apps build dir succeeds.
- [ ] Run: Start `packet_buffer_to_hls.py` then rpicam-source with `local_ts_segments: true`; HLS appears under `ts_segments/<track_id>/`.
