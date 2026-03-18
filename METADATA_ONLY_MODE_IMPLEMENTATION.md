# Metadata-Only Mode & Upload Fixes – Implementation Guide

This document describes all uncommitted changes that implement:
1. **Metadata-only mode**: JSON files created and uploaded without video files
2. **Retry fix**: JSON-only files in staging are correctly uploaded (including retries)
3. **Log throttling**: Reduced logging noise when the uploader is idle

Use this guide to replicate the changes in another repository.

---

## Overview of Changes

| Component | Purpose |
|-----------|---------|
| `udp_streaming_config.json` | New flags: `file_output_enabled`, `create_metadata_file` |
| `rpicam-apps/apps/rpicam_source.cpp` | Conditional video/metadata output; metadata-only move logic |
| `upload_files.py` | Metadata-only processing; retry fix; log throttling |

---

## 1. Config: `variable_files/udp_streaming_config.json`

Add two new keys:

```json
{
  "file_output_enabled": false,
  "create_metadata_file": true,
  ...existing keys...
}
```

### Flag semantics

| Flag | Default | Description |
|------|---------|-------------|
| `file_output_enabled` | `true` | When `true`: create both `.h264` and `.json`. When `false`: no video file. |
| `create_metadata_file` | `true` | When `true` and `file_output_enabled` is `false`: still create and move `.json` (metadata-only mode). |

**Metadata-only mode:** `file_output_enabled: false` + `create_metadata_file: true` → JSON created and uploaded, no video.

---

## 2. C++: `rpicam-apps/apps/rpicam_source.cpp`

### 2.1 Extend `UdpStreamingConfig` struct

Add to the struct (after `enable_csv_logging`):

```cpp
bool file_output_enabled = true;
bool create_metadata_file = true;
```

Update the struct comment:

```cpp
// - file_output_enabled: when false, skip writing .h264 file; when true, write both .h264 and .json
// - create_metadata_file: when true and file_output_enabled is false, still create .json only (metadata-only mode)
```

### 2.2 Read config in `read_udp_streaming_config()`

In the JSON parsing block, add:

```cpp
cfg.file_output_enabled = j.value("file_output_enabled", true);
cfg.create_metadata_file = j.value("create_metadata_file", true);
```

### 2.3 File output setup (where `options->output` and `options->metadata` are set)

Replace the unconditional assignment with:

```cpp
UdpStreamingConfig udp_cfg = read_udp_streaming_config();

// File output: video when file_output_enabled; metadata when file_output_enabled OR create_metadata_file
if (udp_cfg.file_output_enabled) {
    options->output = "/home/pi/source_code/temporary_videos/" + vid_filename + ".h264";
    options->metadata = "/home/pi/source_code/temporary_metadata/" + vid_filename + ".json";
} else if (udp_cfg.create_metadata_file) {
    options->output.clear();  // No video; base Output writes metadata only
    options->metadata = "/home/pi/source_code/temporary_metadata/" + vid_filename + ".json";
} else {
    options->output.clear();
    options->metadata.clear();
}
```

Ensure `read_udp_streaming_config()` is called before this block (and only once per capture setup).

### 2.4 Move logic (after capture stops)

Replace the unconditional move block with:

```cpp
// Move recorded files: both when file_output_enabled; metadata only when create_metadata_file
if (udp_cfg.file_output_enabled || udp_cfg.create_metadata_file) {
    std::string counter_str = "_" + std::to_string(counter);
    std::string old_metadata_destination = "/home/pi/source_code/temporary_metadata/" + video_filename + ".json";
    std::string new_metadata_destination = "/home/pi/source_code/ready_to_upload_source_content/" + video_filename + counter_str + ".json";

    try {
        if (udp_cfg.file_output_enabled) {
            std::string old_video_destination = "/home/pi/source_code/temporary_videos/" + video_filename + ".h264";
            std::string new_video_destination = "/home/pi/source_code/ready_to_upload_source_content/" + video_filename + counter_str + ".h264";
            fs::rename(old_video_destination, new_video_destination);
            fs::rename(old_metadata_destination, new_metadata_destination);
            std::cout << "Files moved successfully: " << new_video_destination << ", " << new_metadata_destination << std::endl;
        } else {
            fs::rename(old_metadata_destination, new_metadata_destination);
            std::cout << "Metadata moved successfully: " << new_metadata_destination << std::endl;
        }
        counter++;
    } catch (const std::exception& e) {
        std::cerr << "Error moving files: " << e.what() << std::endl;
        message = e.what();
    }
}
```

Note: `udp_cfg` must be available in this scope (e.g. read earlier in the same block or passed down).

---

## 3. Python: `upload_files.py`

### 3.1 `process_video()` – support metadata-only

Changes:

- Compute `metadata_only` when `metadata["files"].get("video")` is `None` or `metadata.get("metadata_only")` is true.
- For metadata-only: move and upload only the JSON.
- For normal mode: keep existing video+JSON behavior.

Core logic:

```python
def process_video(metadata):
    x = metadata["x"]
    metadata_only = metadata.get("metadata_only", False) or metadata["files"].get("video") is None
    video_filename = metadata["files"].get("video")
    json_filename = metadata["files"]["json"]

    # ... staging_dir setup ...

    if metadata_only:
        files_ready = os.path.exists(staging_json)
    else:
        staging_video = os.path.join(staging_dir, video_filename)
        files_ready = os.path.exists(staging_video) and os.path.exists(staging_json)

    if not files_ready:
        if metadata_only:
            if not move_files(BASE_DIR, staging_dir, json_filename):
                return False
        else:
            if not (move_files(BASE_DIR, staging_dir, video_filename) and
                    move_files(BASE_DIR, staging_dir, json_filename)):
                return False

    if metadata_only:
        if not upload_file(staging_json, json_s3_key):
            return False
    else:
        if not (upload_file(staging_video, video_s3_key) and
                upload_file(staging_json, json_s3_key)):
            return False
    # ... rest (logs, offsets, cleanup) unchanged ...
```

### 3.2 `process_videos()` – standalone JSON processing

After the video loop, add:

```python
# Set of x values that have a matching .h264
video_x_set = {extract_x(f) for f in video_files if extract_x(f) is not None}

# ... existing video loop ...

# Process standalone JSON files (metadata-only captures: no matching .h264)
for json_file in json_files:
    if json_file.startswith("offset_") or json_file in ("check_mismatch.json", "check_restart.json"):
        continue
    x = extract_x(json_file)
    if x is None:
        continue
    if x in video_x_set:
        continue
    metadata = {
        "x": x,
        "files": {"video": None, "json": json_file},
        "metadata_only": True,
        "size": {"video": 0, "json": os.path.getsize(...)},
        "state": 0,
        "retries": 0
    }
    if process_video(metadata):
        # Remove json from BASE_DIR, add to state, save
    else:
        # Increment retries, maybe mark failed
```

### 3.3 `retry_uploads_dir()` – JSON-only retry and cleanup

Replace the JSON-only handling with:

- Only skip when `state_entry` exists and `state_entry.get('state') == 2`.
- Otherwise: upload JSON, update state, remove file from staging on success.

```python
state_entry = next((f for f in state.values() if isinstance(f, dict) and f.get('files', {}).get('json') == json_file), None)
if state_entry is not None and state_entry.get('state') == 2:
    continue  # Already uploaded successfully

if upload_file(json_path, s3_key):
    # Update state_entry, add to processed
    try:
        os.remove(json_path)
    except OSError:
        pass
else:
    if state_entry is not None:
        state_entry['retries'] = state_entry.get('retries', 0) + 1
```

### 3.4 Log throttling

- Add `verbose=True` to `retry_uploads_dir(state, verbose=True)` and `process_videos(state, verbose=True)`.
- When `verbose=False`, use `logging.debug` instead of `logging.info` for scanning/discovery logs.
- In the main loop: set `quiet = (loop_count % 50 != 0)` and pass `verbose=not quiet` to both functions.
- Only log "Step 1", "Step 2", "No new videos found...", "State saved" when `not quiet`.

---

## 4. Path and repo adaptations

If paths differ in the target repo, update:

- `rpicam_source.cpp`: `/home/pi/source_code/` → your project root.
- `upload_files.py`: `BASE_DIR`, `UPLOAD_BASE_DIR`, `INFO_YAML_PATH`, etc., if they differ.

---

## 5. Testing checklist

- [ ] Metadata-only mode: `file_output_enabled: false`, `create_metadata_file: true` → JSON only.
- [ ] Normal mode: `file_output_enabled: true` → both video and JSON.
- [ ] No output: both false → no video, no metadata.
- [ ] Standalone JSON in `ready_to_upload_source_content` is picked up and uploaded.
- [ ] Retry: JSON in staging without video is uploaded and removed from staging.
- [ ] Idle runs produce less log noise (updates every ~100 seconds when quiet).

---

## 6. Config example

```json
{
  "udp_streaming_enabled": true,
  "local_ts_segments": true,
  "enable_print_every_frame": false,
  "enable_csv_logging": false,
  "file_output_enabled": false,
  "create_metadata_file": true,
  "local_ts_socket_path": "/tmp/rpicam_hls.sock",
  "udp_destination": "udp://192.168.1.100:5002"
}
```
