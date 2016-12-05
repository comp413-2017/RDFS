#include <functional>

#include <asio.hpp>
#include <boost/lockfree/spsc_queue.hpp>

#include <datatransfer.pb.h>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>

#include "native_filesystem.h"
#include "socket_reads.h"
#include "socket_writes.h"
#include "rpcserver.h"
#include "native_filesystem.h"
#include "zk_dn_client.h"

#pragma once

/*
 * Operation types
 */
#define WRITE_BLOCK 80
#define READ_BLOCK 81
#define READ_METADATA 82
#define REPLACE_BLOCK 83
#define COPY_BLOCK 84
#define BLOCK_CHECKSUM 85
#define TRANSFER_BLOCK 86
#define REQUEST_SHORT_CIRCUIT_FDS 87
#define RELEASE_SHORT_CIRCUIT_FDS 88
#define REQUEST_SHORT_CIRCUIT_SHM 89
#define BLOCK_GROUP_CHECKSUM 90
#define CUSTOM 127

using asio::ip::tcp;
using namespace hadoop::hdfs;

const size_t BYTES_PER_CKSUM = 512;

class TransferServer {
	public:
		TransferServer(int port, std::shared_ptr<nativefs::NativeFS> &fs, std::shared_ptr<zkclient::ZkClientDn> &dn, int max_xmits = 10);

		TransferServer(const TransferServer& other) {}
		
		void serve(asio::io_service& io_service);
		bool sendStats();

	private:
		int max_xmits;
		int port;
		std::atomic<std::uint32_t> xmits{0};
		std::shared_ptr<nativefs::NativeFS> fs;
		std::shared_ptr<zkclient::ZkClientDn> dn;

		mutable std::mutex m;
		std::condition_variable cv;

		bool receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type);
		void handle_connection(tcp::socket sock);
		void processWriteRequest(tcp::socket& sock);
		void processReadRequest(tcp::socket& sock);
		void buildBlockOpResponse(std::string& response_string);
		void ackPackets(tcp::socket& sock, boost::lockfree::spsc_queue<PacketHeaderProto>& ackQueue);

		bool writeFinalPacket(tcp::socket& sock, uint64_t, uint64_t);
		template <typename BufType>
		bool writePacket(tcp::socket& sock, PacketHeaderProto p_head, std::vector<std::uint32_t> cksums, const BufType& data);
		void synchronize(std::function<void(TransferServer&, tcp::socket&)> f, tcp::socket& sock);
		std::uint32_t crc(const std::string& my_string, std::uint16_t off, std::uint16_t len);
};

// Templated method to be generic across any asio buffer type.
template <typename BufType>
bool TransferServer::writePacket(tcp::socket& sock, PacketHeaderProto p_head, std::vector<std::uint32_t> cksums, const BufType& data) {
	std::string p_head_str;
	p_head.SerializeToString(&p_head_str);
	const uint16_t header_len = p_head_str.length();
	const uint32_t cksum_len = sizeof(std::uint32_t) * cksums.size();
	// Add 4 to account for the size of uint32_t.
	// Also add in |cksums| u4s; these are a part of the payload and thus the payload length
	const uint32_t payload_len = 4 + asio::buffer_size(data) + cksum_len;
	// Write payload length, header length, header, payload.

	// Short circuit write calls
	bool write_ok = true;
	write_ok &= rpcserver::write_int32(sock, payload_len);
	write_ok &= rpcserver::write_int16(sock, header_len);
	write_ok &= rpcserver::write_proto(sock, p_head_str);

	for (auto cksum : cksums) {
		write_ok &= rpcserver::write_int32(sock, cksum);
	}

	return write_ok && asio::buffer_size(data) == sock.write_some(data);
}
