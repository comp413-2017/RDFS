
#include "../include/zk_nn_client.h"
#include <iostream>

int main(int argc, char* argv[]) {
    zkclient::ZkNnClient nncli("localhost:2181");
    nncli.register_watches();
    std::cout << "Registered watches." << std::endl;
    while(true){

    }
}
