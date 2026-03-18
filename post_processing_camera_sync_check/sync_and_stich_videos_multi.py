import json
import cv2
import numpy as np
import os
import statistics
import subprocess

def find_last_n_common_source(matching_frames1_without_offset, matching_frames2_without_offset, n=25):
   
    i = len(matching_frames1_without_offset) - 1
    j = len(matching_frames2_without_offset) - 1

    print(i , " " , j)
    last_n_tuples = []
    while i >= 0 and j >= 0:
        if len(last_n_tuples) == 25:
            break

        y1, x1 = matching_frames1_without_offset[i]
        y2, x2 = matching_frames2_without_offset[j]

        if x1 == x2:
            last_n_tuples.append((x1, y1, y2))
            i -= 1
            j -= 1
        elif x1 > x2:
            i -= 1
        else:
            j -= 1

    return last_n_tuples

def find_first_n_common_source(matching_frames1_without_offset, matching_frames2_without_offset, n=25):
    i = 0
    j = 0

    first_n_tuples = []

    while i < len(matching_frames1_without_offset) and j < len(matching_frames2_without_offset):
        if len(first_n_tuples) == 25:
            break

        y1, x1 = matching_frames1_without_offset[i]
        y2, x2 = matching_frames2_without_offset[j]
        
        if x1 == x2:
            first_n_tuples.append((x1, y1, y2))
            i += 1
            j += 1
        elif x1 < x2:
            i += 1
        else:
            j += 1

    return first_n_tuples

def create_video_from_pairs(matching_frames1_without_offset, sink1_video_mp4, source_video_mp4, output_file_pair1):
    # Keep first 25 and last 25 pairs
    selected_pairs = matching_frames1_without_offset[:25] + matching_frames1_without_offset[-25:]

    # Open video captures
    sink_cap = cv2.VideoCapture(sink1_video_mp4)
    source_cap = cv2.VideoCapture(source_video_mp4)

    # Get video properties (assume both videos have same resolution & FPS)
    width = int(sink_cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(sink_cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = sink_cap.get(cv2.CAP_PROP_FPS)

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_file_pair1, fourcc, fps, (width * 2, height))

    for sink_frame_num, source_frame_num in selected_pairs:
        # Read sink frame
        sink_cap.set(cv2.CAP_PROP_POS_FRAMES, sink_frame_num)
        ret_sink, frame_sink = sink_cap.read()

        # Read source frame
        source_cap.set(cv2.CAP_PROP_POS_FRAMES, source_frame_num)
        ret_source, frame_source = source_cap.read()

        if ret_sink and ret_source:
            # Concatenate side by side
            combined_frame = cv2.hconcat([frame_sink, frame_source])
            out.write(combined_frame)

    # Release everything
    sink_cap.release()
    source_cap.release()
    out.release()


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

def match_frames(sink_metadata, source_metadata, time_diff, bypass = False):
    matching_pairs = []
    time_diff = None
    
    source_sensor_times = [frame["SensorTimestamp"] for frame in source_metadata]
    
    source_index = 0
    for sink_index, sink_frame in enumerate(sink_metadata):
        print("=======================================================================================")
        print("source idx: ", source_index, "sink index: ", sink_index)
        if sink_index == len(sink_metadata) - 1 or source_index == len(source_sensor_times) - 1:
            break
        if time_diff == None:
            time_diff = source_sensor_times[source_index] - sink_frame["SensorTimestamp"]
        adjusted_sink_time = sink_frame["SensorTimestamp"] + time_diff

        while source_index < len(source_sensor_times) - 1 and abs(adjusted_sink_time - source_sensor_times[source_index + 1]) < abs(adjusted_sink_time - source_sensor_times[source_index]):
            source_index += 1
            if source_index >= len(source_sensor_times): 
                break
        
        if abs(adjusted_sink_time - source_sensor_times[source_index]) < 33000000:
            print((sink_index, source_index))
            extra_val = adjusted_sink_time - source_sensor_times[source_index]
            time_diff = time_diff - extra_val
            matching_pairs.append((sink_index, source_index))
    
    return matching_pairs

def stitch_three_videos(sink1_video_path, sink2_video_path, source_video_path,
                       matching_frames1_without_offset, matching_frames2_without_offset, output_path, 
                       capture_folder_name, first_frames_without_offset_images, 
                       last_frames_without_offset_images, restart_flag):
    cap_sink1 = cv2.VideoCapture(sink1_video_path)
    cap_sink2 = cv2.VideoCapture(sink2_video_path)
    cap_source = cv2.VideoCapture(source_video_path)
    
    # Calculate total width and height for the stitched video
    frame_width = int(cap_sink1.get(cv2.CAP_PROP_FRAME_WIDTH)) + \
              int(cap_sink2.get(cv2.CAP_PROP_FRAME_WIDTH)) + \
              int(cap_source.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = max(int(cap_sink1.get(cv2.CAP_PROP_FRAME_HEIGHT)), 
                      int(cap_sink2.get(cv2.CAP_PROP_FRAME_HEIGHT)),
                      int(cap_source.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(output_path, fourcc, cap_sink1.get(cv2.CAP_PROP_FPS), (frame_width, frame_height))
    
    output_folder = os.path.dirname(output_path)

    last_n_frames_tuples = find_last_n_common_source(matching_frames1_without_offset, matching_frames2_without_offset)
    first_n_frames_tuples = find_first_n_common_source(matching_frames1_without_offset, matching_frames2_without_offset)
    last_frames_without_offset = last_n_frames_tuples[0]

    # Save first and last frames without offset
    original_pair = [0, 0, 0]
    cap_sink1.set(cv2.CAP_PROP_POS_FRAMES, original_pair[0])
    cap_sink2.set(cv2.CAP_PROP_POS_FRAMES, original_pair[1])
    cap_source.set(cv2.CAP_PROP_POS_FRAMES, original_pair[2])

    ret_sink1, frame_sink1 = cap_sink1.read()
    ret_sink2, frame_sink2 = cap_sink2.read()
    ret_source, frame_source = cap_source.read()

    stitched_frame = np.hstack((frame_sink1, frame_sink2, frame_source))
    fname = 'first_stitched_frame_without_offset.png'
    if restart_flag:
        fname = 'restart_' + fname
    cv2.imwrite(os.path.join(output_folder, fname), stitched_frame)
    cv2.imwrite(os.path.join(first_frames_without_offset_images, f'{capture_folder_name}_{fname}'), stitched_frame) ##first frame

    cap_sink1.set(cv2.CAP_PROP_POS_FRAMES, last_frames_without_offset[1])
    cap_sink2.set(cv2.CAP_PROP_POS_FRAMES, last_frames_without_offset[2])
    cap_source.set(cv2.CAP_PROP_POS_FRAMES, last_frames_without_offset[0])

    ret_sink1, frame_sink1 = cap_sink1.read()
    ret_sink2, frame_sink2 = cap_sink2.read()
    ret_source, frame_source = cap_source.read()

    stitched_frame = np.hstack((frame_sink1, frame_sink2, frame_source))
    fname = 'last_stitched_frame_without_offset.png'
    if restart_flag:
        fname = 'restart_' + fname
    cv2.imwrite(os.path.join(output_folder, fname), stitched_frame)
    cv2.imwrite(os.path.join(last_frames_without_offset_images, f'{capture_folder_name}_{fname}'), stitched_frame) #last frame

    last_n_frames_tuples.reverse()

    for idx in range(len(first_n_frames_tuples)):

        source_idx, sink1_idx, sink2_idx = first_n_frames_tuples[idx]
        
        cap_sink1.set(cv2.CAP_PROP_POS_FRAMES, sink1_idx)
        cap_sink2.set(cv2.CAP_PROP_POS_FRAMES, sink2_idx)
        cap_source.set(cv2.CAP_PROP_POS_FRAMES, source_idx)

        ret_sink1, frame_sink1 = cap_sink1.read()
        ret_sink2, frame_sink2 = cap_sink2.read()
        ret_source, frame_source = cap_source.read()

        if not ret_sink1 or not ret_sink2 or not ret_source:
            break

        stitched_frame = np.hstack((frame_sink1, frame_sink2, frame_source))
        out.write(stitched_frame)

    for idx in range(len(last_n_frames_tuples)):

        source_idx, sink1_idx, sink2_idx = last_n_frames_tuples[idx]
        
        cap_sink1.set(cv2.CAP_PROP_POS_FRAMES, sink1_idx)
        cap_sink2.set(cv2.CAP_PROP_POS_FRAMES, sink2_idx)
        cap_source.set(cv2.CAP_PROP_POS_FRAMES, source_idx)

        ret_sink1, frame_sink1 = cap_sink1.read()
        ret_sink2, frame_sink2 = cap_sink2.read()
        ret_source, frame_source = cap_source.read()

        if not ret_sink1 or not ret_sink2 or not ret_source:
            break

        stitched_frame = np.hstack((frame_sink1, frame_sink2, frame_source))
        out.write(stitched_frame)

    cap_sink1.release()
    cap_sink2.release()
    cap_source.release()
    out.release()

def main():
    base_dir = "./3d_vision_testing"
    output_dir = "./synced_videos_one_source_two_sinks"
    output_dir_only_images = "./synced_one_source_two_sinks_images"
    first_frames_without_offset_images = os.path.join(output_dir_only_images, "first_frames_without_offset")
    last_frames_without_offset_images = os.path.join(output_dir_only_images, "last_frames_without_offset")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(output_dir_only_images, exist_ok=True)
    os.makedirs(first_frames_without_offset_images, exist_ok=True)
    os.makedirs(last_frames_without_offset_images, exist_ok=True)
    
    date_folders = [os.path.join(base_dir, folder) for folder in os.listdir(base_dir)
                    if os.path.isdir(os.path.join(base_dir, folder))]
    if not date_folders:
        print("No date folders found in", date_folders)

    for date_folder in sorted(date_folders):
        print(f"\nProcessing time folder: {date_folder}")

        capture_folders = [os.path.join(date_folder, folder) for folder in os.listdir(date_folder)
                            if os.path.isdir(os.path.join(date_folder, folder))]
        for capture_folder in sorted(capture_folders):
            print(f"\nProcessing capture folder: {capture_folder}")
            capture_folder_name = os.path.basename(capture_folder)
            
            if int(capture_folder_name) < 4297:
                continue
            source_dir = os.path.join(capture_folder, "source")
            sink1_dir = os.path.join(capture_folder, "sink1")
            sink2_dir = os.path.join(capture_folder, "sink2")
            
            if not os.path.isdir(sink1_dir) or not os.path.isdir(sink2_dir):
                continue
            if not os.path.isdir(source_dir):
                continue
            
            # Check for restart flag
            restart_flag = False
            check_restart_path = os.path.join(capture_folder,"source", "check_restart.json")
            if os.path.exists(check_restart_path):
                with open(check_restart_path, "r") as f:
                    restart_data = json.load(f)
                    if restart_data.get("restart") == 1:
                        restart_flag = True

            # Find source video
            source_video = next((f for f in os.listdir(source_dir) if f.lower().endswith('.h264')), None)
            if not source_video:
                print("  No source video found in", source_dir)
                continue
            source_video_path = os.path.join(source_dir, source_video)
            source_json = next(
                (f for f in os.listdir(source_dir) if f.lower().endswith('.json') and f[0].isdigit()), 
                None
            )
            if not source_json:
                print("  No source metadata found in", source_dir)
                continue
            source_json_path = os.path.join(source_dir, source_json)

            # Find sink1 video
            sink1_video = next((f for f in os.listdir(sink1_dir) if f.lower().endswith('.h264')), None)
            if not sink1_video:
                print("  No sink1 video found in", sink1_dir)
                continue
            sink1_video_path = os.path.join(sink1_dir, sink1_video)
            sink1_json = next((f for f in os.listdir(sink1_dir) if f.lower().endswith('.json')), None)
            if not sink1_json:
                print("  No sink1 metadata found in", sink1_dir)
                continue
            sink1_json_path = os.path.join(sink1_dir, sink1_json)

            # Find sink2 video
            sink2_video = next((f for f in os.listdir(sink2_dir) if f.lower().endswith('.h264')), None)
            if not sink2_video:
                print("  No sink2 video found in", sink2_dir)
                continue
            sink2_video_path = os.path.join(sink2_dir, sink2_video)
            sink2_json = next((f for f in os.listdir(sink2_dir) if f.lower().endswith('.json')), None)
            if not sink2_json:
                print("  No sink2 metadata found in", sink2_dir)
                continue
            sink2_json_path = os.path.join(sink2_dir, sink2_json)

            prefix = source_video[:17]
            safe_prefix = prefix.replace(":", "-")
            new_output_folder = os.path.join(output_dir, f"{capture_folder_name}_{prefix}_{safe_prefix}")
            os.makedirs(new_output_folder, exist_ok=True)
            
            # Copy metadata files
            copy_source_json_path = os.path.join(new_output_folder, "source.json")
            copy_sink1_json_path = os.path.join(new_output_folder, "sink1.json")
            copy_sink2_json_path = os.path.join(new_output_folder, "sink2.json")
            
            first_sync_info_path1 = os.path.join(new_output_folder, "first_sync_frame_info_sink1.txt")
            first_sync_info_path2 = os.path.join(new_output_folder, "first_sync_frame_info_sink2.txt")
            
            sink1_metadata_stats_path = os.path.join(new_output_folder, "sink1_metadata_stats.txt")
            sink2_metadata_stats_path = os.path.join(new_output_folder, "sink2_metadata_stats.txt")
            source_metadata_stats_path = os.path.join(new_output_folder, "source_metadata_stats.txt")

            output_file = os.path.join(new_output_folder, f"stitched_{safe_prefix}.mp4")
            output_file_pair1 = os.path.join(new_output_folder, f"pair_stitched_{safe_prefix}.mp4")
            output_file_pair2 = os.path.join(new_output_folder, f"pair2_stitched_{safe_prefix}.mp4")

            # Load metadata
            with open(source_json_path, "r") as f:
                source_data = json.load(f)
            with open(sink1_json_path, "r") as f:
                sink1_data = json.load(f)
            with open(sink2_json_path, "r") as f:
                sink2_data = json.load(f)

            # Save the loaded JSON data to the new files
            with open(copy_source_json_path, "w") as f:
                json.dump(source_data, f, indent=4)
            with open(copy_sink1_json_path, "w") as f:
                json.dump(sink1_data, f, indent=4)
            with open(copy_sink2_json_path, "w") as f:
                json.dump(sink2_data, f, indent=4)

            print(f"  Stitching videos for prefix '{prefix}':")
            print("output folder: ", new_output_folder)
            print("    Source Video:", source_video)
            print("    Sink1 Video: ", sink1_video)
            print("    Sink2 Video: ", sink2_video)
            print("    Output: ", output_file)

            if os.path.exists(output_file):
                print(f"  Output already exists, skipping.")
                continue

            # Convert videos to mp4
            source_video_mp4 = convert_h264_to_mp4(source_video_path)
            sink1_video_mp4 = convert_h264_to_mp4(sink1_video_path)
            sink2_video_mp4 = convert_h264_to_mp4(sink2_video_path)

            # Load metadata
            sink1_metadata = load_metadata(sink1_json_path)
            sink2_metadata = load_metadata(sink2_json_path)
            source_metadata = load_metadata(source_json_path)

            frame_info = {}
            frame_info["source"] = len(source_metadata)
            frame_info["sink1"] = len(sink1_metadata)
            frame_info["sink2"] = len(sink2_metadata)

            frame_info_path = os.path.join(new_output_folder, "frame_info.json")
            with open(frame_info_path, "w") as f:
                json.dump(frame_info, f, indent=4)

            # Check frame gaps
            check_frame_gaps(sink1_json_path, sink1_metadata_stats_path)
            check_frame_gaps(sink2_json_path, sink2_metadata_stats_path)
            check_frame_gaps(source_json_path, source_metadata_stats_path)
            
            matching_frames1_without_offset = match_frames(sink1_metadata, source_metadata, 0, bypass=True)
            
            matching_frames2_without_offset = match_frames(sink2_metadata, source_metadata, 0, bypass=True)

            create_video_from_pairs(matching_frames1_without_offset, sink1_video_mp4, source_video_mp4, output_file_pair1)
            create_video_from_pairs(matching_frames2_without_offset, sink2_video_mp4, source_video_mp4, output_file_pair2)

            # print("here: ", len(matching_frames2_without_offset))
            # breakpoint()

            # Save sync info
            with open(first_sync_info_path1, "w") as f:
                json.dump(matching_frames1_without_offset, f)
            with open(first_sync_info_path2, "w") as f:
                json.dump(matching_frames2_without_offset, f)
            # Stitch all three videos together
            stitch_three_videos(sink1_video_mp4, sink2_video_mp4, source_video_mp4, 
                                matching_frames1_without_offset, matching_frames2_without_offset, output_file, 
                                capture_folder_name, first_frames_without_offset_images, 
                                last_frames_without_offset_images, restart_flag)

if __name__ == "__main__":
    main()