#include <unordered_map>
#include <functional>

#include <asio.hpp>

#pragma once

using asio::ip::tcp;

class RPCServer {
	public:
		/**
		 * Construct an RPC server that will listen on given port.
		 */
		RPCServer(int port);
		/**
		 * Begin the listen-loop of the RPC server. This method does not return.
		 */
		void serve(asio::io_service& io_service);
		/**
		 * Register a handler function for the given key, such that when a
		 * request for method of name key arrives, the registered handler is
		 * called.
		 * @param key is the method name specified in the request header.
		 * @param handler takes as input a string the bytes of the method's
		 *        input protobuf, and should return a string of bytes of the
		 *        method's output protobuf (so the handler should parse and
		 *        serialize protobufs)
		 */
		void register_handler(std::string key, std::function<std::string(std::string)> handler);

	private:
		/**
		 * Port the server will listen on when serve is called.
		 */
		int port;
		/**
		 * Table where we store the registered handler functions.
		 */
		std::unordered_map<std::string, std::function<std::string(std::string)>> dispatch_table;
		/**
		 * Receive a hadoop RPC handshake and write data to provided pointers.
		 * True on successful read, false otherwise.
		 */
		bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol);
		/**
		 * Receive the connection prelude: RpcRequestHeaderProto and IpcConnectionContextProto.
		 * True if successful, false otherwise.
		 */
		bool receive_prelude(tcp::socket& sock);
		/**
		 * Handle a single RPC connection: it only returns when the client disconnects.
		 */
		void handle_rpc(tcp::socket sock);

		// **RPCSserver**
		static const std::string CLASS_NAME;
};
