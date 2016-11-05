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
				int error_code;
				if (!zk.create(path, ZKWrapper::EMPTY_VECTOR, error_code)) {
					// TODO: Handle erro
				}
		}

		/**
		 * Returns the name of the first item in the queue
		 */
		std::string peek();

		/**
		 * Removes the first item from the queue.
		 * Returns 0 if successful, otherwise an error code
		 */
		int pop();

		/**
		 * Adds a new item to the end of the queue with the given data.
		 * Returns the path of the newly added element
		 */
		std::string push(const std::vector<std::uint8_t> &data);

	private:
		std::string q_path;
		std::string peek_path;
		std::string element;
		ZKWrapper zk;
		static const std::string CLASS_NAME;
};

#endif //RDFS_ZK_QUEUE_H
