#ifndef RDFS_ZKCLIENTCOMMON_H
#define RDFS_ZKCLIENTCOMMON_H

#include <zkwrapper.h>

#include <boost/shared_ptr.hpp>

namespace zkclient {

    class ZkClientCommon {
    public:
        ZkClientCommon(std::string hostAndIp);
        ZkClientCommon(std::shared_ptr <ZKWrapper> zk);

        void init();
        std::shared_ptr <ZKWrapper> zk;

        // constants used by the clients
        static const std::string NAMESPACE_PATH;
        static const std::string HEALTH;
        static const std::string HEALTH_BACKSLASH;
        static const std::string STATS;
        static const std::string HEARTBEAT;
        static const std::string WORK_QUEUES;
        static const std::string REPLICATE_QUEUES;
        static const std::string REPLICATE_QUEUES_NO_BACKSLASH;
        static const std::string DELETE_QUEUES;
        static const std::string DELETE_QUEUES_NO_BACKSLASH;
        static const std::string WAIT_FOR_ACK;
        static const std::string WAIT_FOR_ACK_BACKSLASH;
        static const std::string REPLICATE_BACKSLASH;
		static const std::string BLOCK_LOCATIONS;
        static const std::string BLOCKS;
        static const std::string DELETE_BACKSLASH;
    private:
        static const std::string CLASS_NAME;
    };
}

#endif //RDFS_ZKCLIENTCOMMON_H
