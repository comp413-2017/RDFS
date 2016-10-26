#include <unordered_map>
#include <functional>

#include <asio.hpp>

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

class TransferServer {
	public:
		TransferServer(int port);
		void serve(asio::io_service& io_service);

	private:
		int port;
		bool receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type);
		void handle_connection(tcp::socket sock);
		void processReadRequest(tcp::socket& sock);
		void buildReadResponse(std::string& response_string);
};

