#include <iostream>
#include <asio.hpp>
#include <thread>

#include <Datatransfer.pb.h>

#include <easylogging++.h>

#include "socket_reads.h"
#include "rpcserver.h"
#include "data_transfer_server.h"

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return
#define ERROR_AND_FALSE(msg) LOG(ERROR) << msg; return false

using asio::ip::tcp;

TransferServer::TransferServer(int p) : port{p} {}

bool TransferServer::receive_header(tcp::socket& sock, uint16_t* version, unsigned char* type) {
	return (rpcserver::read_int16(sock, version) && rpcserver::read_byte(sock, type));
}

void TransferServer::handle_connection(tcp::socket sock) {
	asio::error_code error;
	uint16_t version;
	unsigned char type;
	if (receive_header(sock, &version, &type)) {
		LOG(INFO) << "Got header version=" << version << ", type=" << (int) type;
	} else {
		ERROR_AND_RETURN("Failed to receive header.");
	}
}

void TransferServer::serve(asio::io_service& io_service) {
	LOG(INFO) << "Transfer Server listens on :" << this->port;
	tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

	for (;;) {
		tcp::socket sock(io_service);
		a.accept(sock);
		std::thread(&TransferServer::handle_connection, this, std::move(sock)).detach();
	}
}
