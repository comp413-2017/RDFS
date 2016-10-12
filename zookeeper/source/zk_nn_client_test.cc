
#include "../include/zk_nn_client.h"
#include <iostream>

int main(int argc, char* argv[]) {
    zkclient::ZkNnClient* nncli = new zkclient::ZkNnClient();
    nncli.register_watches();
    std::cout << "Registered watches." << std::endl;
}
