#ifndef RDFS_ZKNNCLIENT_H
#define RDFS_ZKNNCLIENT_H

#include "../include/zk_client_common.h"

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>

namespace zkclient {

	using namespace hadoop::hdfs;
	class ZkNnClient : public ZkClientCommon {
		public:
			ZkNnClient(std::string zkIpAndAddress);
			void register_watches();
			void get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res);
			void create_file(CreateRequestProto& request, CreateResponseProto& response);
			bool file_exists(const std::string& path);
		private:
			std::string ZookeeperPath(const std::string &hadoopPath);
	};

}

#endif //RDFS_ZKNNCLIENT_H

