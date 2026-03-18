/* SPDX-License-Identifier: BSD-2-Clause */
/*
 * Copyright (C) 2020, Raspberry Pi (Trading) Ltd.
 *
 * net_output.cpp - send output over network.
 */

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <cerrno>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <thread>
#include <filesystem>
#include <atomic>
#include <mutex>

#include <libcamera/controls.h>

#include "net_output.hpp"

// Static variables for packet / local TS and CSV
namespace {
    const std::string STREAMED_PACKETS_DIR = "/home/pi/source_code/streamed_packets";
}

// Global NetOutput* so reset_frame_counter can call openCSVLogsForTrack
static NetOutput* g_net_output = nullptr;

// CSV logging for packet debug (streamed_packets/<track_id>/sender_packets.csv). Controlled via set_enable_csv_logging (from udp_streaming_config.json).
static std::atomic<bool> g_enable_csv_logging(false);

// Terminal/stderr output. Set to false for quiet operation (reduces I/O, useful when logging to file).
static constexpr bool ENABLE_TERMINAL_LOGGING = true;

// Single flag: when true, push packets to Python via Unix socket for local HLS/TS segments. No .bin/.txt files.
static std::atomic<bool> g_udp_send_enabled(true);
static std::atomic<bool> g_local_ts_segments(false);
static std::string g_local_ts_socket_path = "/tmp/rpicam_hls.sock";
static int g_local_ts_socket = -1;
static std::mutex g_local_ts_socket_mutex;

void set_udp_send_enabled(bool enabled) { g_udp_send_enabled = enabled; }
void set_local_ts_segments(bool enabled) { g_local_ts_segments = enabled; }
void set_local_ts_socket_path(const std::string& path) { g_local_ts_socket_path = path; }
void set_enable_csv_logging(bool enabled) { g_enable_csv_logging = enabled; }

// Function to reset frame counter (exposed for rpicam_source.cpp)
// Called when capture starts: open CSV logs for track and connect to local TS socket if enabled.
void reset_frame_counter(int track_id_index) {
    // Open CSV logs for this track (streamed_packets/<track_id>/sender_packets.csv, sender_failed_packets.csv)
    if (g_enable_csv_logging.load() && g_net_output) {
        g_net_output->openCSVLogsForTrack(track_id_index);
    }
    // Open local_ts dropped packets CSV when local_ts is used (always, so we can track drops)
    if (g_local_ts_segments.load() && g_net_output) {
        g_net_output->openLocalTsDroppedCSVForTrack(track_id_index);
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
                // Increase send buffer to reduce blocking (match order of magnitude used for UDP)
                int sendbuf = 4 * 1024 * 1024; // 4 MB
                if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sendbuf, sizeof(sendbuf)) < 0) {
                    if (ENABLE_TERMINAL_LOGGING)
                        std::cerr << "[LOCAL_TS] WARNING: setsockopt(SO_SNDBUF) failed: " << strerror(errno) << std::endl;
                }
                // Non-blocking so full buffer never stalls the capture pipeline
                int flags = fcntl(fd, F_GETFL, 0);
                if (flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0) {
                    // ok
                } else {
                    if (ENABLE_TERMINAL_LOGGING)
                        std::cerr << "[LOCAL_TS] WARNING: fcntl(O_NONBLOCK) failed: " << strerror(errno) << std::endl;
                }
                uint32_t tid_be = htonl(static_cast<uint32_t>(track_id_index));
                ssize_t n = send(fd, &tid_be, 4, MSG_NOSIGNAL);
                if (n != 4) {
                    if (ENABLE_TERMINAL_LOGGING)
                        std::cerr << "[LOCAL_TS] WARNING: Failed to send track_id on socket" << std::endl;
                    close(g_local_ts_socket);
                    g_local_ts_socket = -1;
                } else {
                    if (ENABLE_TERMINAL_LOGGING)
                        std::cerr << "[LOCAL_TS] Connected to " << g_local_ts_socket_path << ", track_id=" << track_id_index << std::endl;
                }
            } else {
                if (ENABLE_TERMINAL_LOGGING)
                    std::cerr << "[LOCAL_TS] Connect to " << g_local_ts_socket_path << " failed (is packet_buffer_to_hls.py running?): " << strerror(errno) << std::endl;
                close(fd);
            }
        } else {
            if (ENABLE_TERMINAL_LOGGING)
                std::cerr << "[LOCAL_TS] socket(AF_UNIX) failed: " << strerror(errno) << std::endl;
        }
    }
}

// Function to close local TS socket (exposed for rpicam_source.cpp). Called when capture stops.
void close_frame_log() {
    std::lock_guard<std::mutex> sock_lock(g_local_ts_socket_mutex);
    if (g_local_ts_socket >= 0) {
        if (ENABLE_TERMINAL_LOGGING)
            std::cerr << "[SOCKET_DEBUG] local_ts (packet_buffer_to_hls): Closing socket (capture stopped)" << std::endl;
        close(g_local_ts_socket);
        g_local_ts_socket = -1;
    }
}

NetOutput::NetOutput(VideoOptions const *options) : Output(options)
{
	char protocol[4];
	int start, end, a, b, c, d, port;
	if (sscanf(options->output.c_str(), "%3s://%n%d.%d.%d.%d%n:%d", protocol, &start, &a, &b, &c, &d, &end, &port) != 6)
		throw std::runtime_error("bad network address " + options->output);
	std::string address = options->output.substr(start, end - start);

	if (strcmp(protocol, "udp") == 0)
	{
		saddr_ = {};
		saddr_.sin_family = AF_INET;
		saddr_.sin_port = htons(port);
		if (inet_aton(address.c_str(), &saddr_.sin_addr) == 0)
			throw std::runtime_error("inet_aton failed for " + address);

		fd_ = socket(AF_INET, SOCK_DGRAM, 0);
		if (fd_ < 0)
			throw std::runtime_error("unable to open udp socket");

		// Increase UDP send buffer size to handle burst of packets
		int send_buffer_size = 16 * 1024 * 1024; // 16 MB
		if (setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &send_buffer_size, sizeof(send_buffer_size)) < 0) {
			if (ENABLE_TERMINAL_LOGGING)
				std::cerr << "[SEND] WARNING: Failed to set SO_SNDBUF, errno=" << errno << std::endl;
		} else {
			// Verify the buffer was actually set (kernel may cap it)
			int actual_buffer_size = 0;
			socklen_t len = sizeof(actual_buffer_size);
			if (getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &actual_buffer_size, &len) == 0) {
				if (actual_buffer_size < send_buffer_size) {
					std::cerr << "[SEND] WARNING: Requested " << (send_buffer_size / (1024 * 1024))
					          << " MB but got " << (actual_buffer_size / (1024 * 1024))
					          << " MB (kernel limit)" << std::endl;
					} else if (ENABLE_TERMINAL_LOGGING) {
						std::cerr << "[SEND] UDP send buffer set to " << (send_buffer_size / (1024 * 1024)) << " MB" << std::endl;
				}
			} else {
				std::cerr << "[SEND] UDP send buffer set to " << (send_buffer_size / (1024 * 1024)) << " MB (verification failed)" << std::endl;
			}
		}

		// Non-blocking UDP so full send buffer doesn't stall the capture pipeline
		int flags = fcntl(fd_, F_GETFL, 0);
		if (flags >= 0 && fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == 0)
			std::cerr << "[SEND] UDP socket set to non-blocking" << std::endl;
		else
			std::cerr << "[SEND] WARNING: Could not set UDP socket to non-blocking" << std::endl;

		saddr_ptr_ = (const sockaddr *)&saddr_; // sendto needs these for udp
		sockaddr_in_size_ = sizeof(sockaddr_in);
	}
	else if (strcmp(protocol, "tcp") == 0)
	{
		// WARNING: I've not actually tried this yet...
		if (options->listen)
		{
			// We are the server.
			int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
			if (listen_fd < 0)
				throw std::runtime_error("unable to open listen socket");

			sockaddr_in server_saddr = {};
			server_saddr.sin_family = AF_INET;
			server_saddr.sin_addr.s_addr = INADDR_ANY;
			server_saddr.sin_port = htons(port);

			int enable = 1;
			if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0)
				throw std::runtime_error("failed to setsockopt listen socket");

			if (bind(listen_fd, (struct sockaddr *)&server_saddr, sizeof(server_saddr)) < 0)
				throw std::runtime_error("failed to bind listen socket");
			listen(listen_fd, 1);

			LOG(2, "Waiting for client to connect...");
			fd_ = accept(listen_fd, (struct sockaddr *)&saddr_, &sockaddr_in_size_);
			if (fd_ < 0)
				throw std::runtime_error("accept socket failed");
			LOG(2, "Client connection accepted");

			close(listen_fd);
		}
		else
		{
			// We are a client.
			saddr_ = {};
			saddr_.sin_family = AF_INET;
			saddr_.sin_port = htons(port);
			if (inet_aton(address.c_str(), &saddr_.sin_addr) == 0)
				throw std::runtime_error("inet_aton failed for " + address);

			fd_ = socket(AF_INET, SOCK_STREAM, 0);
			if (fd_ < 0)
				throw std::runtime_error("unable to open client socket");

			LOG(2, "Connecting to server...");
			if (connect(fd_, (struct sockaddr *)&saddr_, sizeof(sockaddr_in)) < 0)
				throw std::runtime_error("connect to server failed");
			LOG(2, "Connected");
		}

		saddr_ptr_ = NULL; // sendto doesn't want these for tcp
		sockaddr_in_size_ = 0;
	}
	else
		throw std::runtime_error("unrecognised network protocol " + options->output);
	
	// CSV logs are opened per-track when capture starts (via reset_frame_counter -> openCSVLogsForTrack)
	g_net_output = this;
	
	// Create directory for streamed_packets CSV logs
	std::filesystem::create_directories(STREAMED_PACKETS_DIR);
}

NetOutput::~NetOutput()
{
	g_net_output = nullptr;
	if (csv_log_.is_open()) {
		csv_log_.close();
		std::cerr << "[SEND] CSV log closed. Total packets: " << packet_number_ 
		          << ", Total frames: " << frame_number_ << std::endl;
	}
	if (failed_csv_log_.is_open()) {
		failed_csv_log_.close();
		std::cerr << "[SEND] Failed packets CSV log closed. Total failed packets: " << failed_packet_number_
		          << ", Total failed frames: " << failed_frame_number_ << std::endl;
	}
	if (local_ts_dropped_csv_.is_open()) {
		local_ts_dropped_csv_.close();
		if (ENABLE_TERMINAL_LOGGING && local_ts_dropped_count_ > 0)
			std::cerr << "[SEND] Local TS dropped packets CSV closed. Total dropped: " << local_ts_dropped_count_ << std::endl;
	}

	close(fd_);
}

// Maximum payload size per UDP packet (10KB limit)
constexpr size_t MAX_UDP_PAYLOAD = 20480; // 20KB per packet
// Header: sensor_ts (8) + wallclock_ts (8) + frame_size (4) + keyframe (1) + packet_index (1) + total_packets (1) = 23 bytes
constexpr size_t HEADER_SIZE = 23;
constexpr size_t MAX_UDP_SIZE = MAX_UDP_PAYLOAD + HEADER_SIZE; // Total packet size including header

void NetOutput::openCSVLogsForTrack(int track_id)
{
	// Close existing CSV logs if open (from previous track)
	if (csv_log_.is_open()) {
		csv_log_.close();
	}
	if (failed_csv_log_.is_open()) {
		failed_csv_log_.close();
	}
	packet_number_ = 0;
	frame_number_ = 0;
	failed_packet_number_ = 0;
	failed_frame_number_ = 0;

	std::string track_dir = STREAMED_PACKETS_DIR + "/" + std::to_string(track_id);
	std::filesystem::create_directories(track_dir);

	std::string sender_csv = track_dir + "/sender_packets.csv";
	std::string failed_csv = track_dir + "/sender_failed_packets.csv";

	csv_log_.open(sender_csv, std::ios::out);
	if (csv_log_.is_open()) {
		csv_log_ << "Packet_Number,Frame_Number,Frame_Type,Is_First_Packet,"
		         << "Timestamp_NS,Wallclock_Timestamp_NS,Frame_Size,Expected_Packets,Sent_Packet_Size,"
		         << "Frame_Data_Size,Total_Sent_So_Far,Destination_IP,Send_Time_NS,"
		         << "Packet_Data_Preview_First_32_Bytes_Hex,Packet_Data_Size_With_Header,"
		         << "Packet_Data_Size_Without_Header,Packet_Index\n";
		csv_log_.flush();
		std::cerr << "[SEND] CSV logging initialized: " << sender_csv << std::endl;
	} else {
		std::cerr << "[SEND] WARNING: Failed to open CSV log file " << sender_csv << std::endl;
	}

	failed_csv_log_.open(failed_csv, std::ios::out);
	if (failed_csv_log_.is_open()) {
		failed_csv_log_ << "Failed_Packet_Number,Failed_Frame_Number,Frame_Type,Is_First_Packet,"
		                << "Timestamp_NS,Frame_Size,Expected_Packets,Attempted_Packet_Size,"
		                << "Frame_Data_Size,Total_Attempted_So_Far,Destination_IP,Failure_Time_NS,"
		                << "Packet_Data_Preview_First_32_Bytes_Hex,Packet_Data_Size_With_Header,"
		                << "Packet_Data_Size_Without_Header,Packet_Index,Bytes_Sent,Retry_Count,"
		                << "Errno_Value,Error_Reason\n";
		failed_csv_log_.flush();
		std::cerr << "[SEND] Failed packets CSV logging initialized: " << failed_csv << std::endl;
	} else {
		std::cerr << "[SEND] WARNING: Failed to open failed packets CSV log file " << failed_csv << std::endl;
	}
}

void NetOutput::logPacketToCSV(const uint8_t* packet, size_t packet_size, bool is_first_packet,
                               size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
                               bool is_keyframe, size_t frame_data_in_packet, size_t total_sent_so_far,
                               int packet_index, int total_packets)
{
	if (!csv_log_.is_open()) return;
	
	packet_number_++;
	if (is_first_packet) {
		frame_number_++;
	}
	
	auto now = std::chrono::system_clock::now();
	auto time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
		now.time_since_epoch()).count();
	
	// Get first 32 bytes as hex
	std::ostringstream hex_preview;
	size_t preview_size = std::min(32UL, packet_size);
	for (size_t i = 0; i < preview_size; i++) {
		hex_preview << std::hex << std::setfill('0') << std::setw(2) 
		           << static_cast<int>(packet[i]);
	}
	
	// Get destination IP
	std::string dest_ip = "N/A";
	if (saddr_ptr_) {
		char ip_str[INET_ADDRSTRLEN];
		struct sockaddr_in* sin = (struct sockaddr_in*)saddr_ptr_;
		if (inet_ntop(AF_INET, &sin->sin_addr, ip_str, INET_ADDRSTRLEN)) {
			dest_ip = ip_str;
		}
	}
	
	// Calculate expected packets (using 10KB payload limit)
	constexpr size_t MAX_UDP_PAYLOAD_CSV = 20480; // 20KB
	size_t max_payload = MAX_UDP_PAYLOAD_CSV;
	int expected_packets = (frame_size + max_payload - 1) / max_payload;
	
	csv_log_ << packet_number_ << ","
	         << frame_number_ << ","
	         << (is_keyframe ? "KEYFRAME" : "P-Frame") << ","
	         << (is_first_packet ? "1" : "0") << ","
	         << sensor_timestamp << ","
	         << wallclock_timestamp_ns << ","
	         << frame_size << ","
	         << expected_packets << ","
	         << packet_size << ","
	         << frame_data_in_packet << ","
	         << total_sent_so_far << ","
	         << dest_ip << ","
	         << time_ns << ","
	         << hex_preview.str() << ","
	         << packet_size << ","
	         << (is_first_packet ? (packet_size - HEADER_SIZE) : packet_size) << ","
	         << packet_index << "\n";
	// Flush every 60 frames (not every frame) to reduce I/O stalls that can contribute to socket blocking
	if (is_first_packet && (frame_number_ % 60 == 0))
		csv_log_.flush();
}

void NetOutput::logFailedPacketToCSV(const uint8_t* packet, size_t packet_size, bool is_first_packet,
                                     size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
                                     bool is_keyframe, size_t frame_data_in_packet, size_t total_attempted_so_far,
                                     int packet_index, int total_packets, ssize_t bytes_sent,
                                     int retry_count, int errno_value, const std::string& error_reason)
{
	if (!failed_csv_log_.is_open()) return;
	
	failed_packet_number_++;
	if (is_first_packet) {
		failed_frame_number_++;
	}
	
	auto now = std::chrono::system_clock::now();
	auto time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
		now.time_since_epoch()).count();
	
	// Get first 32 bytes as hex
	std::ostringstream hex_preview;
	size_t preview_size = std::min(32UL, packet_size);
	for (size_t i = 0; i < preview_size; i++) {
		hex_preview << std::hex << std::setfill('0') << std::setw(2) 
		           << static_cast<int>(packet[i]);
	}
	
	// Get destination IP
	std::string dest_ip = "N/A";
	if (saddr_ptr_) {
		char ip_str[INET_ADDRSTRLEN];
		struct sockaddr_in* sin = (struct sockaddr_in*)saddr_ptr_;
		if (inet_ntop(AF_INET, &sin->sin_addr, ip_str, INET_ADDRSTRLEN)) {
			dest_ip = ip_str;
		}
	}
	
	// Calculate expected packets (using 10KB payload limit)
	constexpr size_t MAX_UDP_PAYLOAD_CSV2 = 20480; // 20KB
	size_t max_payload = MAX_UDP_PAYLOAD_CSV2;
	int expected_packets = (frame_size + max_payload - 1) / max_payload;
	
	failed_csv_log_ << failed_packet_number_ << ","
	                << failed_frame_number_ << ","
	                << (is_keyframe ? "KEYFRAME" : "P-Frame") << ","
	                << (is_first_packet ? "1" : "0") << ","
	                << sensor_timestamp << ","
	                << wallclock_timestamp_ns << ","
	                << frame_size << ","
	                << expected_packets << ","
	                << packet_size << ","
	                << frame_data_in_packet << ","
	                << total_attempted_so_far << ","
	                << dest_ip << ","
	                << time_ns << ","
	                << hex_preview.str() << ","
	                << packet_size << ","
	                << (is_first_packet ? (packet_size - HEADER_SIZE) : packet_size) << ","
	                << packet_index << ","
	                << bytes_sent << ","
	                << retry_count << ","
	                << errno_value << ","
	                << "\"" << error_reason << "\"\n";
	failed_csv_log_.flush();
}

void NetOutput::openLocalTsDroppedCSVForTrack(int track_id)
{
	if (local_ts_dropped_csv_.is_open()) {
		local_ts_dropped_csv_.close();
	}
	local_ts_dropped_count_ = 0;

	std::string track_dir = STREAMED_PACKETS_DIR + "/" + std::to_string(track_id);
	std::filesystem::create_directories(track_dir);

	std::string dropped_csv = track_dir + "/sender_local_ts_dropped.csv";
	local_ts_dropped_csv_.open(dropped_csv, std::ios::out);
	if (local_ts_dropped_csv_.is_open()) {
		local_ts_dropped_csv_ << "Drop_Number,Frame_Number,Packet_Index,Total_Packets,Frame_Size,"
		                      << "Sensor_Timestamp_NS,Wallclock_Timestamp_NS,Is_Keyframe,Packet_Size,"
		                      << "Errno,Error_Message,Drop_Time_NS\n";
		local_ts_dropped_csv_.flush();
		if (ENABLE_TERMINAL_LOGGING)
			std::cerr << "[SEND] Local TS dropped packets CSV initialized: " << dropped_csv << std::endl;
	}
}

void NetOutput::logLocalTsDroppedPacketToCSV(unsigned int frame_number, int packet_index, int total_packets,
                                             size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
                                             bool is_keyframe, size_t packet_size, int errno_value,
                                             const std::string& error_message)
{
	if (!local_ts_dropped_csv_.is_open()) return;

	local_ts_dropped_count_++;
	auto now = std::chrono::system_clock::now();
	uint64_t drop_time_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
		now.time_since_epoch()).count());

	local_ts_dropped_csv_ << local_ts_dropped_count_ << ","
	                      << frame_number << ","
	                      << packet_index << ","
	                      << total_packets << ","
	                      << frame_size << ","
	                      << sensor_timestamp << ","
	                      << wallclock_timestamp_ns << ","
	                      << (is_keyframe ? "KEYFRAME" : "P-Frame") << ","
	                      << packet_size << ","
	                      << errno_value << ","
	                      << "\"" << error_message << "\","
	                      << drop_time_ns << "\n";
	local_ts_dropped_csv_.flush();
}

// Helper function to convert 64-bit integer to network byte order (big endian)
static uint64_t htonll(uint64_t value)
{
	// Check if we're on a big-endian system
	union { uint32_t i; uint8_t c[4]; } test = { 0x01020304 };
	if (test.c[0] == 0x01) {
		// Big endian - no conversion needed
		return value;
	} else {
		// Little endian - swap bytes
		uint64_t result = 0;
		uint8_t *src = (uint8_t *)&value;
		uint8_t *dst = (uint8_t *)&result;
		for (int i = 0; i < 8; i++) {
			dst[i] = src[7 - i];
		}
		return result;
	}
}

void NetOutput::outputBuffer(void *mem, size_t size, int64_t timestamp_us, uint32_t flags)
{
	// Call with no metadata (for backward compatibility)
	outputBufferWithMetadata(mem, size, timestamp_us, flags, nullptr);
}

void NetOutput::outputBufferWithMetadata(void *mem, size_t size, int64_t timestamp_us, 
                                         uint32_t flags, libcamera::ControlList *metadata)
{
	// Extract SensorTimestamp from metadata - prefer metadata, but use fallback if missing
	// CRITICAL: We must send keyframes even if metadata is missing, otherwise receiver won't get SPS/PPS
	int64_t sensor_timestamp_ns = 0;

	if (metadata) {
		auto ts = metadata->get(libcamera::controls::SensorTimestamp);
		if (ts) {
			sensor_timestamp_ns = *ts;
		} else {
			// Use timestamp_us as fallback (convert microseconds to nanoseconds)
			sensor_timestamp_ns = timestamp_us * 1000;
		}
	} else {
		// Use timestamp_us as fallback (convert microseconds to nanoseconds)
		sensor_timestamp_ns = timestamp_us * 1000;
	}

	// Send packet with header (using fallback timestamp if metadata unavailable)
	sendPacket(mem, size, timestamp_us, flags, sensor_timestamp_ns);
}

void NetOutput::sendPacket(void *mem, size_t size, int64_t timestamp_us, uint32_t flags, 
                           int64_t sensor_timestamp_ns)
{
	bool is_keyframe = (flags & FLAG_KEYFRAME) != 0;
	frames_sent_++;

	// Status every 100 frames
	if (frames_sent_ % 100 == 0 && ENABLE_TERMINAL_LOGGING) {
		std::cerr << "[STATUS] Frame " << frames_sent_ << ": ";
		if (g_local_ts_segments.load())
			std::cerr << "socket=" << (g_local_ts_socket >= 0 ? "OK" : "FAIL") << " ";
		if (g_udp_send_enabled.load())
			std::cerr << "UDP=OK ";
		std::cerr << std::endl;
	}
	
	LOG(2, "NetOutput: output buffer " << mem << " size " << size << " sensor_ts " << sensor_timestamp_ns);
	
	// Calculate total packets needed for this frame (10KB payload limit)
	size_t max_payload = saddr_ptr_ ? MAX_UDP_PAYLOAD : size;
	int total_packets = saddr_ptr_ ? ((size + max_payload - 1) / max_payload) : 1;
	
	// Build packet header:
	// - SensorTimestamp (8 bytes, int64_t, network byte order)
	// - Wallclock timestamp (8 bytes, uint64_t ns since epoch, network byte order)
	// - Frame size (4 bytes, uint32_t, network byte order)
	// - Keyframe flag (1 byte: 1 = keyframe, 0 = not)
	// - Packet index (1 byte: 0-based index of this packet)
	// - Total packets (1 byte: total number of packets for this frame)
	uint8_t header[HEADER_SIZE];
	
	// SensorTimestamp (convert to network byte order - big endian)
	uint64_t sensor_ts_network = htonll((uint64_t)sensor_timestamp_ns);
	memcpy(header, &sensor_ts_network, 8);
	
	// Wallclock timestamp (nanoseconds since epoch, when we start sending this frame)
	uint64_t wallclock_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	uint64_t wallclock_network = htonll(wallclock_ns);
	memcpy(header + 8, &wallclock_network, 8);
	
	// Frame size
	uint32_t size_network = htonl(size);
	memcpy(header + 16, &size_network, 4);
	
	// Keyframe flag
	header[20] = is_keyframe ? 1 : 0;
	
	// Packet index and total packets will be set per packet
	
	// For UDP: split into packets if needed (accounting for header)
	// For TCP: send as one packet
	if (saddr_ptr_) {
		// UDP mode: send header + data, split if needed
		uint8_t *data = (uint8_t *)mem;
		size_t remaining = size;
		int packet_index = 0;  // 0-based index
		size_t total_sent = 0;
		bool local_ts_frame_dropped = false;  // when true, skip rest of frame for local_ts

		while (remaining > 0 && !local_ts_frame_dropped) {
			size_t payload_size = std::min(remaining, max_payload);
			
			// Build packet: header (with packet_index) + payload
			uint8_t packet[MAX_UDP_SIZE];
			uint8_t *ptr = packet;
			size_t packet_size = 0;
			
			// Copy base header (sensor_ts + wallclock_ts + frame_size + keyframe = 21 bytes)
			memcpy(ptr, header, 21);
			ptr += 21;
			
			// Set packet_index (0-based)
			*ptr++ = (uint8_t)packet_index;
			
			// Set total_packets
			*ptr++ = (uint8_t)total_packets;
			packet_size += HEADER_SIZE;
			
			// Add payload
			memcpy(ptr, data, payload_size);
			ptr += payload_size;
			packet_size += payload_size;
			
			total_sent += payload_size;
			
			// When local_ts_segments is true: send packet to Python via Unix socket only (no .bin/.txt)
			if (g_local_ts_segments.load()) {
				uint32_t len = static_cast<uint32_t>(packet_size);
				uint32_t len_be = htonl(len);
				std::lock_guard<std::mutex> sock_lock(g_local_ts_socket_mutex);
				if (g_local_ts_socket >= 0) {
					const int max_retries = 10;
					const int retry_us = 200;
					auto try_send = [max_retries, retry_us](int fd, const void* data, size_t len) -> ssize_t {
						int last_errno_val = 0;
						for (int r = 0; r < max_retries; r++) {
							ssize_t n = send(fd, data, len, MSG_NOSIGNAL);
							if (n >= 0) return n;
							last_errno_val = errno;
							if (errno != EAGAIN && errno != EWOULDBLOCK) return -1;
							if (ENABLE_TERMINAL_LOGGING)
								std::cerr << "[SOCKET] Retry " << (r + 1) << "/" << max_retries
								          << ": send failed (EAGAIN), waiting " << retry_us << " us" << std::endl;
							std::this_thread::sleep_for(std::chrono::microseconds(retry_us));
						}
						errno = last_errno_val;
						return -1;
					};
					char buf[4];
					memcpy(buf, &len_be, 4);
					ssize_t n = try_send(g_local_ts_socket, buf, 4);
					if (n == 4)
						n = try_send(g_local_ts_socket, packet, packet_size);
					if (n != (ssize_t)packet_size) {
						int err = errno;
						if (ENABLE_TERMINAL_LOGGING)
							std::cerr << "[SOCKET] Packet " << packet_index << "/" << total_packets
							          << " dropped (after " << max_retries << " retries): skipping rest of frame. "
							          << "errno=" << err << " (" << strerror(err) << ")" << std::endl;
						std::string err_msg = std::string(strerror(err)) + " (EAGAIN after " + std::to_string(max_retries) + " retries)";
						logLocalTsDroppedPacketToCSV(frames_sent_, packet_index, total_packets,
						                            size, sensor_timestamp_ns, wallclock_ns,
						                            is_keyframe, packet_size, err, err_msg);
						local_ts_frame_dropped = true;
					}
				}
			}

			// Send over UDP (only when UDP streaming enabled; skip if we dropped frame for local_ts)
			if (!local_ts_frame_dropped && g_udp_send_enabled.load()) {
			// Send packet with retry
			ssize_t sent = 0;
			int retry_count = 0;
			const int MAX_RETRIES = 10;
			const int RETRY_DELAY_US = 500; // 0.5ms delay between retries
			int last_errno = 0;
			std::string error_reason;
			
			while (sent < (ssize_t)packet_size && retry_count < MAX_RETRIES) {
				ssize_t result = sendto(fd_, packet + sent, packet_size - sent, 0, saddr_ptr_, sockaddr_in_size_);
				
				if (result < 0) {
					last_errno = errno;
					if (errno == EAGAIN || errno == EWOULDBLOCK) {
						// Buffer full, wait briefly and retry
						retry_count++;
						if (retry_count < MAX_RETRIES) {
							std::this_thread::sleep_for(std::chrono::microseconds(RETRY_DELAY_US));
							continue;
						} else {
							error_reason = "Buffer full after " + std::to_string(MAX_RETRIES) + " retries";
							std::cerr << "[SEND] ERROR: sendto failed after " << MAX_RETRIES
							          << " retries (buffer full), errno=" << errno << std::endl;
							// Log failed packet before throwing
							logFailedPacketToCSV(packet, packet_size, (packet_index == 0), size,
							                     sensor_timestamp_ns, wallclock_ns, is_keyframe, payload_size, total_sent,
							                     packet_index, total_packets, sent, retry_count, last_errno, error_reason);
							throw std::runtime_error("failed to send data on socket after retries");
						}
					} else {
						// Other error - fail immediately
						error_reason = "sendto() failed with errno " + std::to_string(errno);
						std::cerr << "[SEND] ERROR: sendto failed, errno=" << errno << std::endl;
						// Log failed packet before throwing
						logFailedPacketToCSV(packet, packet_size, (packet_index == 0), size,
						                     sensor_timestamp_ns, wallclock_ns, is_keyframe, payload_size, total_sent,
						                     packet_index, total_packets, sent, retry_count, last_errno, error_reason);
						throw std::runtime_error("failed to send data on socket");
					}
				}
				sent += result;
			}
			
			if (sent != (ssize_t)packet_size) {
				error_reason = "Partial send: sent " + std::to_string(sent) + " of " + std::to_string(packet_size) + " bytes";
				std::cerr << "[SEND] ERROR: Partial send after retries - sent=" << sent << ", expected=" << packet_size << std::endl;
				// Log failed packet (partial send is considered a failure)
				logFailedPacketToCSV(packet, packet_size, (packet_index == 0), size,
				                     sensor_timestamp_ns, wallclock_ns, is_keyframe, payload_size, total_sent,
				                     packet_index, total_packets, sent, retry_count, last_errno, error_reason);
				// Continue to next packet instead of logging to success CSV
				data += payload_size;
				remaining -= payload_size;
				packet_index++;
				if (remaining > 0) {
					// 2 ms between packets for I-frame, 0.5 ms for P-frame
					std::this_thread::sleep_for(std::chrono::microseconds(is_keyframe ? 2000 : 500));
				}
				continue;
			}
			
			// Log to CSV ONLY after successful send (ensures CSV only contains packets that were actually sent)
			logPacketToCSV(packet, packet_size, (packet_index == 0), size,
			              sensor_timestamp_ns, wallclock_ns, is_keyframe, payload_size, total_sent, packet_index, total_packets);
			}  // end if (g_udp_send_enabled)
			
			data += payload_size;
			remaining -= payload_size;
			packet_index++;
			
			// Inter-packet delay: 2 ms (I-frame) / 0.5 ms (P-frame)
			if (remaining > 0) {
				std::this_thread::sleep_for(std::chrono::microseconds(is_keyframe ? 2000 : 500));
			}
		}
	} else {
		// TCP mode: send header + all data in one go
		uint8_t *packet = new uint8_t[HEADER_SIZE + size];
		memcpy(packet, header, 21);  // Copy base header (sensor_ts + wallclock_ts + frame_size + keyframe)
		packet[21] = 0;  // packet_index = 0 (only one packet for TCP)
		packet[22] = 1;  // total_packets = 1
		memcpy(packet + HEADER_SIZE, mem, size);
		
		ssize_t sent = send(fd_, packet, HEADER_SIZE + size, 0);
		delete[] packet;
		
		if (sent < 0)
			throw std::runtime_error("failed to send data on socket");
	}
}
