#ifndef RDFS_ZKNNCLIENT_H
#define RDFS_ZKNNCLIENT_H

#include "../include/zk_client_common.h"

namespace zkclient {

    class ZkNnClient : public ZkClientCommon {
        public:
            ZkNnClient(std::string zkIpAndAddress);
            void register_watches();

    };

}

#endif //RDFS_ZKNNCLIENT_H

