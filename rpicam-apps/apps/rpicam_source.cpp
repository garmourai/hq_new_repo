/* SPDX-License-Identifier: BSD-2-Clause */
/*
 * Copyright (C) 2020, Raspberry Pi (Trading) Ltd.
 *
 * rpicam_source.cpp - libcamera3d vision source camera app.
 */

 #include <chrono>
 #include <thread>
 #include <poll.h>
 #include <signal.h>
 #include <sys/signalfd.h>
 #include <sys/stat.h>
 #include <iomanip>
 #include <nlohmann/json.hpp>
 #include <unistd.h>
 #include <cstring>
 #include <iostream>
 #include <fstream>
 #include <mutex>
 #include <atomic>
 #include <filesystem>
 
 #include "core/rpicam_encoder.hpp"
 #include "output/output.hpp"
 #include "utils/httplib.h"
 
 using json = nlohmann::json;
 using namespace std::placeholders;
 using namespace httplib;
 
 // State management
 enum class CameraState { CONFIGURE, IDLE, CAPTURE, SAVE };
 std::atomic<CameraState> current_state(CameraState::CONFIGURE);
 std::mutex state_mutex;
 std::mutex filename_mutex;
 std::atomic<bool> start_capture(false);
 std::atomic<bool> stop_capture(false);
 std::atomic<bool> stop_capture_complete(false);
 std::string video_filename;
 
 // Add HTTP server-related variables
 const int HTTP_PORT = 8080;
 std::unique_ptr<Server> server;
 std::thread server_thread;
 
namespace fs = std::filesystem;

// Forward declarations for frame counter functions from net_output.cpp
extern void reset_frame_counter(int track_id_index);
extern void close_frame_log();
extern void set_udp_send_enabled(bool);
extern void set_local_ts_segments(bool);
extern void set_local_ts_socket_path(const std::string& path);
extern void set_enable_csv_logging(bool);

// Helper function to read track_id counter from track_video_index.json
static int read_track_id_counter() {
    int counter = 0;
    std::string counter_file = "/home/pi/source_code/variable_files/track_video_index.json";
    try {
        std::ifstream infile(counter_file);
        if (infile.is_open()) {
            json video_index = json::parse(infile);
            counter = video_index["counter"];
            infile.close();
        } else {
            std::cerr << "Warning: Could not read counter file, using 0." << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Warning: Could not read counter file, using 0: " << e.what() << std::endl;
    }
    return counter;
}

// Streaming config: read from variable_files/udp_streaming_config.json
// - udp_streaming_enabled: send packets over UDP to udp_destination
// - local_ts_segments: push packets to Python via Unix socket for local HLS/TS (packet_buffer_to_hls.py). No .bin/.txt files.
// - enable_print_every_frame: when true, print Count/time every frame; when false (default), print only every 1000 frames
// - enable_csv_logging: when true, log packets to streamed_packets/<track_id>/sender_packets.csv (debug)
// - file_output_enabled: when false, skip writing .h264 file; when true, write both .h264 and .json
// - create_metadata_file: when true and file_output_enabled is false, still create .json only (metadata-only mode)
struct UdpStreamingConfig {
    bool udp_streaming_enabled = false;
    bool local_ts_segments = false;
    bool enable_print_every_frame = false;
    bool enable_csv_logging = false;
    bool file_output_enabled = true;
    bool create_metadata_file = true;
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
        cfg.enable_print_every_frame = j.value("enable_print_every_frame", false);
        cfg.enable_csv_logging = j.value("enable_csv_logging", false);
        cfg.file_output_enabled = j.value("file_output_enabled", true);
        cfg.create_metadata_file = j.value("create_metadata_file", true);
        cfg.destination = j.value("udp_destination", cfg.destination);
        cfg.local_ts_socket_path = j.value("local_ts_socket_path", cfg.local_ts_socket_path);
    } catch (const std::exception& e) {
        std::cerr << "Streaming config read failed: " << e.what() << std::endl;
    }
    return cfg;
}

static int get_colourspace_flags(std::string const &codec)
 {
     if (codec == "mjpeg" || codec == "yuv420")
         return RPiCamEncoder::FLAG_VIDEO_JPEG_COLOURSPACE;
     else
         return RPiCamEncoder::FLAG_VIDEO_NONE;
 }
 
 std::string generate_filename(std::chrono::high_resolution_clock::time_point time_point) {
      auto now_time_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
     auto now_ms = std::chrono::duration_cast<std::chrono::microseconds>(time_point.time_since_epoch()) % 1000000;
 
     std::stringstream filename;
     filename << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d_%H:%M:%S")
              << ":" << std::setfill('0') << std::setw(6) << now_ms.count();
 
     return filename.str();
 }
 
 std::string get_current_time()
 {
     auto now = std::chrono::system_clock::now();
     auto time_t_now = std::chrono::system_clock::to_time_t(now);
     auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
 
     std::tm local_time = *std::localtime(&time_t_now);
     std::stringstream formatted_time_stream;
     formatted_time_stream << std::put_time(&local_time, "%H:%M:%S") << ":" << std::to_string(ms.count());
     return formatted_time_stream.str();
 }
 
 void start_http_server() {
     server = std::make_unique<Server>();
     
     // Start/Stop endpoints
     server->Post("/start", [](const Request& req, Response& res) {
         if(current_state == CameraState::IDLE) {
             start_capture = true;
             res.set_content(R"({"status": "Capture started"})", "application/json");
         } else {
             LOG(1, "Rejected /start - Invalid state");
             res.status = 400;
             res.set_content(R"({"error": "Already capturing or busy"})", "application/json");
         }
     });
 
     server->Post("/stop", [](const Request& req, Response& res) {
         if(current_state == CameraState::CAPTURE) {
             stop_capture = true;
             while(!stop_capture_complete) {
                 std::this_thread::sleep_for(std::chrono::milliseconds(100));
             }
             stop_capture_complete = false;
             res.set_content(R"({"status": "Capture stopped"})", "application/json");
         } else {
             res.status = 400;
             res.set_content(R"({"error": "Not currently capturing"})", "application/json");
         }
     });
 
     server->Post("/video_name", [](const Request& req, Response& res) {
         if(current_state != CameraState::CONFIGURE) {
             std::lock_guard<std::mutex> lock(filename_mutex);
             json response = {
                 {"status", "Getting video name"},
                 {"filename", video_filename}
             };
             res.set_content(response.dump(), "application/json");
         } else {
             res.status = 400;
             res.set_content(R"({"error": "New video is being configured"})", "application/json");
         }
     });
 
     // Status endpoint
     server->Get("/status", [](const Request& req, Response& res) {
         json status;
         std::lock_guard<std::mutex> lock(state_mutex);
         switch(current_state.load()) {
             case CameraState::CONFIGURE: status["state"] = "CONFIGURE"; break;
             case CameraState::IDLE:      status["state"] = "IDLE"; break;
             case CameraState::CAPTURE:   status["state"] = "CAPTURE"; break;
             case CameraState::SAVE:      status["state"] = "SAVE"; break;
         }
         res.set_content(status.dump(), "application/json");
     });
 
     server_thread = std::thread([](){
         LOG(1, "Starting HTTP server on port " << HTTP_PORT);
         server->listen("0.0.0.0", HTTP_PORT);
     });
     server_thread.detach();
 }
 
 // The main even loop for the application.
 
 static void event_loop(RPiCamEncoder &app)
 {
     VideoOptions *options = app.GetOptions();
 
     {
         std::lock_guard<std::mutex> lock(state_mutex);
         current_state = CameraState::CONFIGURE;
     }
 
    // CRITICAL: Configure encoder to generate keyframes with SPS/PPS
    // Set intra period (I-frame interval) - every 60 frames = keyframe every ~2 seconds at 30fps
    if (options->intra == 0) {
        options->intra = 60;  // Force keyframes every 60 frames
        std::cerr << "[CONFIG] Setting intra period to 60 frames for keyframe generation" << std::endl;
    }
     // Enable inline headers to ensure SPS/PPS are included with every keyframe
     if (!options->inline_headers) {
         options->inline_headers = true;
         std::cerr << "[CONFIG] Enabling inline headers (SPS/PPS with every keyframe)" << std::endl;
     }
 
     app.OpenCamera();
     // libcamera::ControlList controls;
     // controls.set(libcamera::controls::AeEnable, false);         // Disable auto exposure
     // controls.set(libcamera::controls::ExposureTime, 3000);     // Set exposure time (microseconds)
     // controls.set(libcamera::controls::AnalogueGain, 1.0f);      // Set analogue gain (1.0 = no gain)
 
     // app.SetControls(controls); 
     app.ConfigureVideo(get_colourspace_flags(options->codec));
 
     {
         std::lock_guard<std::mutex> lock(state_mutex);
         current_state = CameraState::IDLE;
     }
 
     start_http_server();
 
     while(true)
     {
        int track_id_index = 0;
        while(true) {
            if(start_capture) {
                start_capture = false;
                // Read current capture ID (counter) from track_video_index.json
                track_id_index = read_track_id_counter();
                break;
            }
            // std::cout << "source code waiting here" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
         
         auto block_start_time = std::chrono::high_resolution_clock::now();
     
         {
             std::lock_guard<std::mutex> lock(state_mutex);
             current_state = CameraState::CONFIGURE;
         }
         
        auto start_time = std::chrono::high_resolution_clock::now();
        std::string vid_filename = generate_filename(start_time);

        {
            std::lock_guard<std::mutex> lock(filename_mutex);
            video_filename = vid_filename;
        }

        // Streaming + file output config
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
        options->flush = false;  // Avoid per-frame flush to reduce I/O pauses; file is flushed on close
        options->inline_headers = true;  // Ensure SPS/PPS headers are included for playback
        std::unique_ptr<Output> file_output = std::unique_ptr<Output>(Output::Create(options));
        
        // Track if we've written at least one keyframe to file (needed for playback)
        std::atomic<bool> file_keyframe_written(false);
        
        // Streaming: create NetOutput when UDP and/or local TS segments (socket) is enabled
        std::unique_ptr<Output> net_output;
        bool use_net_output = udp_cfg.udp_streaming_enabled || udp_cfg.local_ts_segments;
        if (use_net_output) {
            set_udp_send_enabled(udp_cfg.udp_streaming_enabled);
            set_local_ts_segments(udp_cfg.local_ts_segments);
            set_local_ts_socket_path(udp_cfg.local_ts_socket_path);
            set_enable_csv_logging(udp_cfg.enable_csv_logging);
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
        
        // Output callback: file always; network when NetOutput exists (feeds packets; send/write gated inside net_output)
        auto combinedCallback = [fo = file_output.get(), no = net_output.get(), &file_keyframe_written](
            void *mem, size_t size, int64_t timestamp_us, bool keyframe) {
            try {
                fo->OutputReady(mem, size, timestamp_us, keyframe);
                if (keyframe) {
                    file_keyframe_written = true;
                }
            } catch (const std::exception& e) {
                LOG_ERROR("CRITICAL: File output error: " << e.what());
            }
            if (no) {
                try {
                    no->OutputReady(mem, size, timestamp_us, keyframe);
                } catch (const std::exception& e) {
                    LOG_ERROR("Network output error (file already saved): " << e.what());
                    if (keyframe) {
                        LOG_ERROR("CRITICAL: Keyframe failed to send to UDP - receiver won't get SPS/PPS!");
                    }
                }
            }
        };
        
        // Metadata callback: file always; network when NetOutput exists
        auto combinedMetadataCallback = [fo = file_output.get(), no = net_output.get()](
            libcamera::ControlList &metadata) {
            fo->MetadataReady(metadata);
            if (no)
                no->MetadataReady(metadata);
        };
        
        app.SetEncodeOutputReadyCallback(combinedCallback);
        app.SetMetadataReadyCallback(combinedMetadataCallback);
        app.StartEncoder();
         app.StartCamera();
 
         auto now = std::chrono::system_clock::now();
         auto time_t_now = std::chrono::system_clock::to_time_t(now);
         auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
 
         std::tm local_time = *std::localtime(&time_t_now);
 
         std::cout << "Time right before capture :" << std::put_time(&local_time, "%H:%M:%S")
                 << ":" << std::setfill('0') << std::setw(3) << ms.count()
                 << std::endl;
 
         {
             std::lock_guard<std::mutex> lock(state_mutex);
             current_state = CameraState::CAPTURE;
         }
 
         auto block_end_time = std::chrono::high_resolution_clock::now();
         auto execution_duration = std::chrono::duration_cast<std::chrono::milliseconds>(block_end_time - block_start_time).count();
     
         std::cout << "Time taken for the code block: " << execution_duration << " ms" << std::endl;
         
         LOG(1, "started video capture...");
         for (unsigned int count = 0; ; count++)
         {
             // auto start_time_check = std::chrono::high_resolution_clock::now();
 
             // Print every frame if enable_print_every_frame, else only every 1000 frames
             if (udp_cfg.enable_print_every_frame || (count % 1000 == 0)) {
                 std::cout << "Count : " << count << std::endl;
 
                 auto now1 = std::chrono::system_clock::now();
                 auto time_t_now = std::chrono::system_clock::to_time_t(now1);
                 auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now1.time_since_epoch()) % 1000;
 
                 std::tm local_time = *std::localtime(&time_t_now);
 
                 std::cout << std::put_time(&local_time, "%H:%M:%S")
                         << ":" << std::setfill('0') << std::setw(3) << ms.count()
                         << std::endl;
             }
 
             auto block_start_time_var2 = std::chrono::high_resolution_clock::now();
             
             
             RPiCamEncoder::Msg msg = app.Wait();
 
             auto block_end_time_var2 = std::chrono::high_resolution_clock::now();
             auto execution_duration_var2 = std::chrono::duration_cast<std::chrono::milliseconds>(block_end_time_var2 - block_start_time_var2).count();
 
             if (udp_cfg.enable_print_every_frame || (count % 1000 == 0))
                 std::cout << "Time taken by app wait: " << execution_duration_var2 << " ms" << std::endl;
 
 
             // auto end_time_check = std::chrono::high_resolution_clock::now();
 
             // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time_check - start_time_check).count();
 
             // std::cout << duration << std::endl;
             // std::cout << count << std::endl;
 
             // LOG(1, "No of frames captured: " << count );
             if (msg.type == RPiCamApp::MsgType::Timeout)
             {   
                 
                 LOG_ERROR("ERROR: Device timeout detected, attempting a restart!!!");
                 app.StopCamera();
                 app.StartCamera();
                 continue;
             }
             if (msg.type == RPiCamEncoder::MsgType::Quit)
                 return;
             else if (msg.type != RPiCamEncoder::MsgType::RequestComplete)
                 throw std::runtime_error("unrecognised message!");
 
             auto now = std::chrono::high_resolution_clock::now();
             bool timeout = !options->frames && options->timeout &&
                         ((now - start_time) > options->timeout.value);
             bool frameout = options->frames && count >= options->frames;
            if (timeout || frameout || stop_capture)
            {
                stop_capture = false;
                if (udp_cfg.local_ts_segments)
                    close_frame_log();
                
                std::string counter_file="/home/pi/source_code/variable_files/track_video_index.json";
                 try {
                     
                     int counter = 0;
                     int num_frames = count;
                     std::string message = "Success";
                     json video_index;
 
                     std::ifstream infile(counter_file);
                     if (infile.is_open()) {
                         video_index = json::parse(infile);
                         counter = video_index["counter"];
                         infile.close();
                     } else {
                         std::cerr << "Error: Could not open counter file, starting from 0." << std::endl;
                     }
 
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
 
                     video_index["counter"] = counter;
                     video_index["num_frames"] = num_frames;
                     video_index["message"] = message;
 
                     std::ofstream outfile(counter_file);
                     if (outfile.is_open()) {
                         outfile << std::setw(4) << video_index << std::endl;
                         outfile.close();
                     } else {
                         std::cerr << "Error: Could not update counter file." << std::endl;
                     }
                 
                     std::cout << "Files moved successfully.\n";
                 } catch (const fs::filesystem_error& e) {
                     std::cerr << "Error moving files: " << e.what() << std::endl;
                 }
                 LOG(1, "No of frames captured: " << count + 1);
                 LOG(1, "Saved recording to " << vid_filename + ".h264" << std::endl);
            
                 if (timeout)
                     LOG(1, "Halting: reached timeout of " << options->timeout.get<std::chrono::milliseconds>()
                                                         << " milliseconds.");
                 
                 {
                     std::lock_guard<std::mutex> lock(state_mutex);
                     current_state = CameraState::SAVE;
                 }
 
                 app.StopCamera(); // stop complains if encoder very slow to close
                 stop_capture_complete = true;
                 std::cout << "Time right after STOP capture :" << get_current_time() << std::endl;
                 // Wait a bit for encoder to finish writing buffered data
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                
                app.StopEncoder();

                // Wait a bit more to ensure all data is flushed, especially if we haven't seen a keyframe yet (only when file output enabled)
                if (udp_cfg.file_output_enabled && !file_keyframe_written.load()) {
                    LOG_ERROR("WARNING: No keyframe written to file - file may not be playable!");
                    // Wait a bit longer in case keyframe is still being written
                    std::this_thread::sleep_for(std::chrono::milliseconds(300));
                }
                
                // Explicitly flush and close file output before it goes out of scope
                file_output.reset();  // This will call destructor and closeFile()
                
                if (file_keyframe_written.load()) {
                    LOG(1, "File closed successfully with at least one keyframe - should be playable");
                } else {
                    LOG_ERROR("WARNING: File closed without keyframe - may not be playable!");
                }
 
                 {
                     std::lock_guard<std::mutex> lock(state_mutex);
                     current_state = CameraState::IDLE;
                 }
 
                 break;
             }
 
             CompletedRequestPtr &completed_request = std::get<CompletedRequestPtr>(msg.payload);
             app.EncodeBuffer(completed_request, app.VideoStream());
             app.ShowPreview(completed_request, app.VideoStream());
         }
     }
     return;
 }
 
 int main(int argc, char *argv[])
 {
     try
     {
         RPiCamEncoder app;
         VideoOptions *options = app.GetOptions();
         if (options->Parse(argc, argv))
         {
             if (options->verbose >= 2)
                 options->Print();
 
             event_loop(app);
         }
     }
     catch (std::exception const &e)
     {
         LOG_ERROR("ERROR: *** " << e.what() << " ***");
         return -1;
     }
     return 0;
 }