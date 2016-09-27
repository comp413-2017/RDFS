#include <cstdlib>
#include <iostream>
#include <asio.hpp>
#include <rpcserver.h>

#include <ClientNameNodeProtocol.pb.h>

std::string getFileInfoDemo(std::string request) {
    hadoop::hdfs::GetFileInfoResponseProto response_pb;
    std::string out_str;
    // Stub just says there's no info.
    response_pb.SerializeToString(&out_str);
    std::cout << "GetFileInfo has " << out_str.size() << " bytes" << std::endl;
    return out_str;
}

int main(int argc, char* argv[]) {
    asio::io_service io_service;
    short port = 5351;
    if (argc == 2) {
        port = std::atoi(argv[1]);
    }
    RPCServer server(port);
    server.register_handler("getFileInfo", getFileInfoDemo);
    server.serve(io_service);
}
