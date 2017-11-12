// Copyright 2017 Rice University, COMP 413 2017

#ifndef ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_
#define ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_

#include <zkwrapper.h>

#include <string>

#include <boost/shared_ptr.hpp>

namespace zkclient {

    class ZkClientCommon {
    public:
        explicit ZkClientCommon(std::string hostAndIp);
        explicit ZkClientCommon(std::shared_ptr<ZKWrapper> zk);

        void init();
        std::shared_ptr<ZKWrapper> zk;

        // constants used by the clients
        static const char NAMESPACE_PATH[];
        static const char BLOCKS_TREE[];
        static const char HEALTH[];
        static const char HEALTH_BACKSLASH[];
        static const char STATS[];
        static const char HEARTBEAT[];
        static const char WORK_QUEUES[];
        static const char REPLICATE_QUEUES[];
        static const char REPLICATE_QUEUES_NO_BACKSLASH[];
        static const char DELETE_QUEUES[];
        static const char DELETE_QUEUES_NO_BACKSLASH[];
        static const char WAIT_FOR_ACK[];
        static const char WAIT_FOR_ACK_BACKSLASH[];
        static const char REPLICATE_BACKSLASH[];
        static const char BLOCK_LOCATIONS[];
        static const char BLOCKS[];

        // ---------- MJP Header -----------
        static const char CLIENTS[];
        static const char LEASES[];
        // ---------- MJP Footer -----------

    private:
        static const std::string CLASS_NAME;
    };
}  // namespace zkclient

#endif  // ZOOKEEPER_INCLUDE_ZK_CLIENT_COMMON_H_
