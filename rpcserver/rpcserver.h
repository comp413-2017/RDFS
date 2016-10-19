#include <unordered_map>
#include <functional>

#include <asio.hpp>

#pragma once

using asio::ip::tcp;

class RPCServer {
    public:
        RPCServer(int port);
        void serve(asio::io_service& io_service);
        void register_handler(std::string key, std::function<std::string(std::string)> handler);

    private:
        int port;
        std::unordered_map<std::string, std::function<std::string(std::string)>> dispatch_table;
        bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol);
        bool receive_prelude(tcp::socket& sock);
        void handle_rpc(tcp::socket sock);
};
