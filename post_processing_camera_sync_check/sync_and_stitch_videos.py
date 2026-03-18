import json
import cv2
import numpy as np
import os
import statistics
import subprocess

def convert_h264_to_mp4(h264_path):
    mp4_path = h264_path.replace(".h264", ".mp4")
    print(f"Converting {h264_path} to {mp4_path}")
    command = ["ffmpeg", "-y", "-i", h264_path, "-c:v", "copy", mp4_path]
    subprocess.run(command, check=True)
    return mp4_path

def load_metadata(json_path):
    with open(json_path, "r") as file:
        return json.load(file)
def find_video_with_prefix(directory, prefix):
    """
    Searches for an MP4 file in 'directory' whose filename contains 'prefix'.
    Returns the full path to the file if found; otherwise, returns None.
    """
    if not os.path.isdir(directory):
        return None

    for filename in os.listdir(directory):
        if filename.lower().endswith('.h264') and prefix in filename:
            return os.path.join(directory, filename)
    return None

def check_frame_gaps(metadata_path, output_txt):
    with open(metadata_path, "r") as file:
        metadata = json.load(file)
    
    gaps = []
    for i in range(1, len(metadata)):
        prev_time = metadata[i - 1]["FrameWallClock"]
        curr_time = metadata[i]["FrameWallClock"]
        gap = curr_time - prev_time
        
        if gap > 35000:
            gaps.append(f"Frame {i-1} to {i}: gap = {gap}\n")
    
    with open(output_txt, "w") as file:
        file.writelines(gaps)
    
    print(f"Saved gaps to {output_txt}")


def match_frames(sink_metadata, source_metadata, time_diff):
    matching_pairs = []
    
    source_sensor_times = [frame["SensorTimestamp"] for frame in source_metadata]
    
    source_index = 0
    for sink_index, sink_frame in enumerate(sink_metadata):
        print("=======================================================================================")
        print("source idx: ", source_index, "sink index: ", sink_index)
        adjusted_sink_time = sink_frame["SensorTimestamp"] + time_diff

        # print("abs(adjusted_sink_time - source_sensor_times[source_index + 1]): ", abs(adjusted_sink_time - source_sensor_times[source_index + 1]))
        # print("abs(adjusted_sink_time - source_sensor_times[source_index]): ", abs(adjusted_sink_time - source_sensor_times[source_index]))
        
        while source_index < len(source_sensor_times) - 1 and abs(adjusted_sink_time - source_sensor_times[source_index + 1]) < abs(adjusted_sink_time - source_sensor_times[source_index]):
            source_index += 1
            # print("source idx: ", source_index, "sink idx: ", sink_index)
            # print("abs(adjusted_sink_time - source_sensor_times[source_index + 1]): ", abs(adjusted_sink_time - source_sensor_times[source_index + 1]))
            # print("abs(adjusted_sink_time - source_sensor_times[source_index]): ", abs(adjusted_sink_time - source_sensor_times[source_index]))

            if source_index >= len(source_sensor_times): 
                break
        
        if abs(adjusted_sink_time - source_sensor_times[source_index]) < 33000000:
            print((sink_index, source_index))
            extra_val = adjusted_sink_time - source_sensor_times[source_index]
            time_diff = time_diff - extra_val
            matching_pairs.append((sink_index, source_index))

        # if sink_index >= 995 or sink_index < 10:
        #     breakpoint()
    
    return matching_pairs

def find_first_matching_frame(sink_metadata, source_metadata, time_diff):
    matching_pairs = []
    sensor_timestamp_diff = None
    source_times = [frame["FrameWallClock"] for frame in source_metadata]
    
    source_sensor_times = [frame["SensorTimestamp"] for frame in source_metadata]
    sink_sensor_times = [frame["SensorTimestamp"] for frame in sink_metadata]
    
    source_index = 0
    for sink_index, sink_frame in enumerate(sink_metadata):
        print("source idx: ", source_index)
        adjusted_sink_time = sink_frame["FrameWallClock"] + time_diff
        
        while source_index < len(source_times) - 1 and abs(adjusted_sink_time - source_times[source_index + 1]) < abs(adjusted_sink_time - source_times[source_index]):
            print("source idx: ", source_index)
            
            source_index += 1
            if source_index >= len(source_times): 
                break
        
        if abs(adjusted_sink_time - source_times[source_index]) < 33000:
            matching_pairs.append((sink_index, source_index))

        if len(matching_pairs) == 1:
            sensor_timestamp_diff = source_sensor_times[source_index] - sink_sensor_times[sink_index]
            break
    
    
    return sensor_timestamp_diff


def stitch_videos(sink_video_path, source_video_path, matching_pairs, output_path, capture_folder_name, first_frames_images, first_frames_without_offset_images, last_frames_images, last_frames_without_offset_images, restart_flag):
    cap_sink = cv2.VideoCapture(sink_video_path)
    cap_source = cv2.VideoCapture(source_video_path)
    
    frame_width = int(cap_sink.get(cv2.CAP_PROP_FRAME_WIDTH)) + int(cap_source.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = max(int(cap_sink.get(cv2.CAP_PROP_FRAME_HEIGHT)), int(cap_source.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, cap_sink.get(cv2.CAP_PROP_FPS), (frame_width, frame_height))
    
    output_folder = os.path.dirname(output_path)

    # pairs_to_process = matching_pairs
    pairs_to_process = []
    if matching_pairs:
        pairs_to_process.extend(matching_pairs[:25])  # First 25
        pairs_to_process.extend(matching_pairs[-25:]) 

    original_pair = [0,0]
    cap_sink.set(cv2.CAP_PROP_POS_FRAMES, original_pair[0])
    cap_source.set(cv2.CAP_PROP_POS_FRAMES, original_pair[1])

    ret_sink, frame_sink = cap_sink.read()
    ret_source, frame_source = cap_source.read()

    stitched_frame = np.hstack((frame_sink, frame_source))
    fname = 'first_stitched_frame_without_offset.png'
    if restart_flag:
        fname = 'restart_' + fname
    cv2.imwrite(os.path.join(output_folder, fname), stitched_frame)
    cv2.imwrite(os.path.join(first_frames_without_offset_images, f'{capture_folder_name}_{fname}'), stitched_frame)

    min_value = min(matching_pairs[-1][0], matching_pairs[-1][1])
    last_original_pair = [min_value, min_value]
    cap_sink.set(cv2.CAP_PROP_POS_FRAMES, last_original_pair[0])
    cap_source.set(cv2.CAP_PROP_POS_FRAMES, last_original_pair[1])

    ret_sink, frame_sink = cap_sink.read()
    ret_source, frame_source = cap_source.read()

    stitched_frame = np.hstack((frame_sink, frame_source))
    fname = 'last_stitched_frame_without_offset.png'
    # <---- RESTART FLAG CHECK: Prepend "restart_" if needed.
    if restart_flag:
        fname = 'restart_' + fname
    cv2.imwrite(os.path.join(output_folder, fname), stitched_frame)
    cv2.imwrite(os.path.join(last_frames_without_offset_images, f'{capture_folder_name}_{fname}'), stitched_frame)

    first_saved = False
    last_saved = False


    for idx, (sink_idx, source_idx) in enumerate(pairs_to_process):
        print(f"stitching {sink_idx} and {source_idx}")
        cap_sink.set(cv2.CAP_PROP_POS_FRAMES, sink_idx)
        cap_source.set(cv2.CAP_PROP_POS_FRAMES, source_idx)
        
        ret_sink, frame_sink = cap_sink.read()
        ret_source, frame_source = cap_source.read()

        if not ret_sink or not ret_source:
            break

        stitched_frame = np.hstack((frame_sink, frame_source))
        out.write(stitched_frame)

        # Save first and last stitched frame as image in the same folder as the video
        if idx == 0 and not first_saved:
            
            first_saved = True
        if idx == len(pairs_to_process) - 1 and not last_saved:
            cv2.imwrite(os.path.join(output_folder, 'last_stitched_frame.png'), stitched_frame)
            cv2.imwrite(os.path.join(last_frames_images, f'{capture_folder_name}_last_stitched_frame.png'), stitched_frame)
            last_saved = True

    cap_sink.release()
    cap_source.release()
    out.release()

def main():

    base_dir = "./3d_vision_testing"
    output_dir = "./synced_videos_one_source_one_sink"
    output_dir_only_images = "./synced_one_source_one_sink_images"
    first_frames_images = os.path.join(output_dir_only_images, "first_frames")
    last_frames_images = os.path.join(output_dir_only_images, "last_frames")
    first_frames_without_offset_images = os.path.join(output_dir_only_images, "first_frames_without_offset")
    last_frames_without_offset_images = os.path.join(output_dir_only_images, "last_frames_without_offset")


    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_only_images, exist_ok=True)
    os.makedirs(first_frames_images, exist_ok=True)
    os.makedirs(last_frames_images, exist_ok=True)
    os.makedirs(first_frames_without_offset_images, exist_ok=True)
    os.makedirs(last_frames_without_offset_images, exist_ok=True)
    
    date_folders = [os.path.join(base_dir, folder) for folder in os.listdir(base_dir)
                    if os.path.isdir(os.path.join(base_dir, folder))]
    
    if not date_folders:
        print("No date folders found in", date_folders)

    prev_offset = -1
    
    for date_folder in sorted(date_folders):
        print(f"\nProcessing time folder: {date_folder}")

        capture_folders = [os.path.join(date_folder, folder) for folder in os.listdir(date_folder)
                            if os.path.isdir(os.path.join(date_folder, folder))]
        for capture_folder in sorted(capture_folders):
            print(f"\nProcessing capture folder: {capture_folder}")
            capture_folder_name = os.path.basename(capture_folder)
            
            if int(capture_folder_name) < 2776:
                continue
            source_dir = os.path.join(capture_folder, "source")
            sink_dir   = os.path.join(capture_folder, "sink_1")

            if not os.path.isdir(sink_dir):
                continue
            if not os.path.isdir(source_dir):
                continue
            # Check for restart flag in check_restart.json (assumed to be in the capture folder)
            restart_flag = False
            check_restart_path = os.path.join(capture_folder, "check_restart.json")
            if os.path.exists(check_restart_path):
                with open(check_restart_path, "r") as f:
                    restart_data = json.load(f)
                    if restart_data.get("restart") == 1:
                        restart_flag = True
            # json_files = [f for f in os.listdir(date_folder) if f.endswith('.json') and "offset" in f]
            # breakpoint()
            offset_file = next((f for f in os.listdir(capture_folder) if f.endswith('_sink_1.json') and "offset" in f), None)
            if not offset_file:
                print(f"No JSON file with 'offset' and '_sink_1' found in {capture_folder}")
                continue


            # source_video = [f for f in os.listdir(source_dir) if f.lower().endswith('.h264')]
            source_video = next((f for f in os.listdir(source_dir) if f.lower().endswith('.h264')), None)
            if not source_video:
                print("  No source video found in", source_dir)
                continue
            source_video_path = os.path.join(source_dir, source_video)
            source_json = next((f for f in os.listdir(source_dir) if f.lower().endswith('.json')), None)
            if not source_json:
                print("  No source video found in", source_dir)
                continue
            source_json_path = os.path.join(source_dir, source_json)


            # sink_video = [f for f in os.listdir(sink_dir) if f.lower().endswith('.h264')]
            sink_video = next((f for f in os.listdir(sink_dir) if f.lower().endswith('.h264')), None)
            if not sink_video:
                print("  No source video found in", sink_dir)
                continue
            sink_video_path = os.path.join(sink_dir, sink_video)

            sink_json = next((f for f in os.listdir(sink_dir) if f.lower().endswith('.json')), None)
            if not source_json:
                print("  No source video found in", sink_dir)
                continue
            sink_json_path = os.path.join(sink_dir, sink_json)

            prefix = source_video[:17]
            safe_prefix = prefix.replace(":", "-")
            new_output_folder = os.path.join(output_dir, f"{capture_folder_name}_{prefix}_{safe_prefix}")
            # new_output_folder_images_only = os.path.join(output_dir_only_images, f"{capture_folder_name}_{prefix}_{safe_prefix}")
            os.makedirs(new_output_folder, exist_ok=True)
            # os.makedirs(new_output_folder_images_only, exist_ok=True)
            offset_output_file = os.path.join(new_output_folder, f"offset_info.json")

            offset_file_path = os.path.join(capture_folder, offset_file)

            with open(offset_file_path, "r") as f:
                offset_data = json.load(f)

            with open(offset_output_file, "a") as f:
                values = [v for v in offset_data if v != -1]  # Filter out -1 values
                    
                if not values:  # Skip if all values were -1
                    values = [-1]
                
                median_value = statistics.median(values)
                offset_value = median_value * 1_000_000

                # Save to file
                f.write(f"Offset Values: {values} -> Median: {median_value}\n")
                if prev_offset != -1 and median_value == -1:
                    offset_value = prev_offset
                prev_offset = offset_value

            copy_source_json_path = os.path.join(new_output_folder, "source.json")
            copy_sink_json_path = os.path.join(new_output_folder, "sink.json")
            first_sync_info_path = os.path.join(new_output_folder, "first_sync_frame_info.txt")
            sink_metadata_stats_path = os.path.join(new_output_folder, "sink_metadata_stats.txt")
            source_metadata_stats_path = os.path.join(new_output_folder, "source_metadata_stats.txt")


            output_file = os.path.join(new_output_folder, f"stitched_{safe_prefix}.mp4")


            with open(source_json_path, "r") as f:
                source_data = json.load(f)

            with open(sink_json_path, "r") as f:
                sink_data = json.load(f)

            # Save the loaded JSON data to the new files
            with open(copy_source_json_path, "w") as f:
                json.dump(source_data, f, indent=4)

            with open(copy_sink_json_path, "w") as f:
                json.dump(sink_data, f, indent=4)

            print(f"  Stitching videos for prefix '{prefix}':")
            print("output folder: ", new_output_folder)
            print("    Source Video:", source_video)
            print("    Source JSON: ", source_json)
            print("    Sink Video:  ", sink_video)
            print("    Sink JSON:  ", sink_json)
            print("    Offset: ", offset_value)
            print("    Output: ", output_file)

            if os.path.exists(output_file):
                print(f"  Output already exists, skipping.")
                continue
            # breakpoint()

            source_video_mp4 = convert_h264_to_mp4(source_video_path)
            sink_video_mp4 = convert_h264_to_mp4(sink_video_path)


            sink_metadata = load_metadata(sink_json_path)
            source_metadata = load_metadata(source_json_path)

            check_frame_gaps(sink_json_path, sink_metadata_stats_path)
            check_frame_gaps(source_json_path, source_metadata_stats_path)
            
            sensor_timestamp_diff = find_first_matching_frame(sink_metadata, source_metadata, offset_value)
            if sensor_timestamp_diff == None:
                print("not able to sync start frames")
                # breakpoint()
                continue
            matching_frames = match_frames(sink_metadata, source_metadata, sensor_timestamp_diff)
            
            with open(first_sync_info_path, "w") as f:
                json.dump(matching_frames, f)

            stitch_videos(sink_video_mp4, source_video_mp4, matching_frames, output_file, capture_folder_name, first_frames_images, first_frames_without_offset_images, last_frames_images, last_frames_without_offset_images,restart_flag)


if __name__ == "__main__":
    main()
