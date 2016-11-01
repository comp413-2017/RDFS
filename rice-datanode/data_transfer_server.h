#include <functional>

#include <asio.hpp>

#include <datatransfer.pb.h>

#include "native-filesystem.h"
#include "socket_reads.h"
#include "socket_writes.h"
#include "rpcserver.h"
#include "native-filesystem.h"
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

class TransferServer {
	public:
		TransferServer(int port, nativefs::NativeFS& fs, zkclient::ZkClientDn& dn);
		void serve(asio::io_service& io_service);

	private:
		int port;
		nativefs::NativeFS fs;
		zkclient::ZkClientDn dn;

		bool receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type);
		void handle_connection(tcp::socket sock);
		void processWriteRequest(tcp::socket& sock);
		void processReadRequest(tcp::socket& sock);
		void buildBlockOpResponse(std::string& response_string);

		template <typename BufType>
		bool writePacket(tcp::socket& sock, PacketHeaderProto p_head, const BufType& payload);
};

// Templated method to be generic across any asio buffer type.
template <typename BufType>
bool TransferServer::writePacket(tcp::socket& sock, PacketHeaderProto p_head, const BufType& payload) {
	std::string p_head_str;
	p_head.SerializeToString(&p_head_str);
	const uint16_t header_len = p_head_str.length();
	// Add 4 to account for the size of uint32_t.
	const uint32_t payload_len = 4 + asio::buffer_size(payload);
	// Write payload length, header length, header, payload.
	return (rpcserver::write_int32(sock, payload_len) &&
			rpcserver::write_int16(sock, header_len) &&
			rpcserver::write_proto(sock, p_head_str) &&
			payload_len - 4 == sock.write_some(payload));
}
