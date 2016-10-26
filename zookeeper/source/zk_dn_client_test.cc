
#include "../include/zk_client_dn.h"
#include "../include/zk_client_common.h"
#include <iostream>

int main(int argc, char* argv[]) {

    if (argc < 1) {
        std::cout << "Please provide a node id" << std::endl;
        std::cout << "Usage: zk_dn_client_test id" << std::endl;
        return 1;
    }

    std::string id = argv[1];

    {
        int error_code;

        zkclient::ZkClientDn client(id, "localhost:2181");
        client.registerDataNode();
        ZKWrapper zk("localhost:2181", error_code);
        assert(error_code == 0); // Stands for ZOK

        // std::cout << zk.get_children("/health", 0)[0] << std::endl;
        while(true){

        }
    }
    return 0;
}
