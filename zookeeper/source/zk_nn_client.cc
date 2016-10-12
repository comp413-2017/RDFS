#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include <iostream>


namespace zkclient{

ZkNnClient::ZkNnClient(std::string zkIpAndAddress) : ZkClientCommon(zkIpAndAddress) {

}

void ZkNnClient::register_watches() {

    // Place a watch on the health subtree
    std::vector <std::string> children = zk->get_children("/health", 1); // TODO: use a constant for the path
    for (int i = 0; i < children.size(); i++) {
        std::cout << "Attaching child to " << children[i] << ", " << std::endl;
        std::vector <std::string> ephem = zk->get_children("/health/" + children[i], 1);
        /*
        if (ephem.size() > 0) {
            std::cout << "Found ephem " << ephem[0] << std::endl;
        } else {
            std::cout << "No ephem found for " << children[i] << std::endl;
        }
         */
    }
}

}

#endif
