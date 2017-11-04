#include <google/protobuf/message.h>
#include "hdfs.pb.h"

#include "zk_nn_client.h"
#include "zkwrapper.h"
#include "zk_nn_client.h"
#include "zk_dn_client.h"

#include <iostream>

#pragma once

namespace webRequestTranslator {
  /**
   * Converts RDFS protobuf responses into the appropriate webRDFS response.
   */
  std::string convertToResponse(google::protobuf::Message &res);
}; // namespace