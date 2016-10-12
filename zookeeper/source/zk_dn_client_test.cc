
#include "../include/zk_client_dn.h"
#include "../include/zk_client_common.h"
#include <iostream>

int main(int argc, char* argv[]) {
    std::cout << "hi" << std::endl;
    zkclient::ZkClientDn client("localhost:8108");
    // zkclient::ZkClientCommon co();
}
