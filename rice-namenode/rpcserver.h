#include <asio.hpp>

using asio::ip::tcp;

class RPCServer {
    public:
        RPCServer(int port);
        void serve(asio::io_service& io_service);

    private:
        int port;
        bool receive_handshake(tcp::socket& sock, short* version, short* service, short* auth_protocol);
        bool receive_prelude(tcp::socket& sock);
        void handle_rpc(tcp::socket sock);
};
