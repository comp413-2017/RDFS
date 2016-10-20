#include <unordered_map>
#include <functional>

#include <asio.hpp>

#pragma once

using asio::ip::tcp;

class TransferServer {
	public:
		TransferServer(int port);
		void serve(asio::io_service& io_service);

	private:
		int port;
		bool receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type);
		void handle_connection(tcp::socket sock);
};

