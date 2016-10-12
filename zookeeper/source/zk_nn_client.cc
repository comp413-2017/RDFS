#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_client_common.h"
#include "../include/zk_nn_client.h"


class ZkNnClient: public ZkClientCommon {

    ZkNnClient::register_watches() {

        // Place a watch on the health subtree
        zk.get_children("/health", 1); // TODO: use a constant for the path
    }
};

#endif
