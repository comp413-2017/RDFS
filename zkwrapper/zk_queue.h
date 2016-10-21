//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#ifndef RDFS_ZK_QUEUE_H
#define RDFS_ZK_QUEUE_H

#include <algorithm>
#include <zookeeper.h>
#include "zkwrapper.h"

class ZKQueue {
    public:
        ZKQueue(ZKWrapper &wrapper, std::string path) : zk(wrapper) {
                q_path = path;
                element = path + "/q_item-";
                peek_path = "";
                zk.create(path, ZKWrapper::EMPTY_VECTOR, errorcode);
        }

        std::string peek();

        int pop();

        std::string push(const std::vector<std::uint8_t> &data);

    private:
		int* errorcode;
        std::string q_path;
        std::string peek_path;
        std::string element;
        ZKWrapper zk;
};

#endif //RDFS_ZK_QUEUE_H
