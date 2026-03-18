/* SPDX-License-Identifier: BSD-2-Clause */
/*
 * Copyright (C) 2020, Raspberry Pi (Trading) Ltd.
 *
 * net_output.hpp - send output over network.
 */

#pragma once

#include <netinet/in.h>
#include <fstream>
#include <string>

#include "output.hpp"

class NetOutput : public Output
{
public:
	NetOutput(VideoOptions const *options);
	~NetOutput();
	void openCSVLogsForTrack(int track_id);
	void openLocalTsDroppedCSVForTrack(int track_id);

protected:
	void outputBuffer(void *mem, size_t size, int64_t timestamp_us, uint32_t flags) override;
	void outputBufferWithMetadata(void *mem, size_t size, int64_t timestamp_us, 
	                              uint32_t flags, libcamera::ControlList *metadata) override;

private:
	void sendPacket(void *mem, size_t size, int64_t timestamp_us, uint32_t flags, 
	                int64_t sensor_timestamp_ns);
	void logPacketToCSV(const uint8_t* packet, size_t packet_size, bool is_first_packet,
	                   size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
	                   bool is_keyframe, size_t frame_data_in_packet, size_t total_sent_so_far,
	                   int packet_index, int total_packets);
	void logFailedPacketToCSV(const uint8_t* packet, size_t packet_size, bool is_first_packet,
	                          size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
	                          bool is_keyframe, size_t frame_data_in_packet, size_t total_attempted_so_far,
	                          int packet_index, int total_packets, ssize_t bytes_sent,
	                          int retry_count, int errno_value, const std::string& error_reason);
	void logLocalTsDroppedPacketToCSV(unsigned int frame_number, int packet_index, int total_packets,
	                                  size_t frame_size, int64_t sensor_timestamp, uint64_t wallclock_timestamp_ns,
	                                  bool is_keyframe, size_t packet_size, int errno_value,
	                                  const std::string& error_message);
	int fd_;
	sockaddr_in saddr_;
	const sockaddr *saddr_ptr_;
	socklen_t sockaddr_in_size_;
	
	// CSV logging members
	std::ofstream csv_log_;
	std::ofstream failed_csv_log_;
	std::ofstream local_ts_dropped_csv_;
	int local_ts_dropped_count_ = 0;
	int packet_number_ = 0;
	int frame_number_ = 0;
	unsigned int frames_sent_ = 0;  // for status print every 100 frames
	int failed_packet_number_ = 0;
	int failed_frame_number_ = 0;
};
