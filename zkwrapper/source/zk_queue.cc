//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"

std::string element = "/q-item-";

bool push(const std::shared_ptr <ZKWrapper> zk, const std::string &q_path, const std::vector<std::uint8_t> &pushed_data, int &error_code) {
	LOG(INFO) << "Pushing to queue: " << q_path;

	// Check that the "queue" znode exists
	bool exists;
	if (!zk->exists(q_path, exists, error_code) || exists == false) {
		LOG(ERROR) << "There is no queue node at: " << q_path;
		return false;
	}

	// Push a new element on to the queue with the given data
	std::string pushed_path;
	if (!zk->create_sequential(q_path + element, pushed_data, pushed_path, false, error_code)) {
		LOG(ERROR) << "There was an error when pushing new element onto the queue.";
		return false;
	}

	// TODO: This section is just for debugging / checking correctness, can remove
	std::vector<std::string> children;
	if (!zk->get_children(q_path, children, error_code)) {
		LOG(ERROR) << "Failed to get children of queue node.";
		return false;
	}
	LOG(INFO) << "Queue front: " << children.back();
	LOG(INFO) << "Queue back: " << children.front();

	return true;
}

bool pop(const std::shared_ptr <ZKWrapper> zk, const std::string &q_path, const std::vector<std::uint8_t> &popped_data, int &error_code) {
	// std::cout << "Popping: " << peek_path << std::endl;

	// std::vector<std::string> children;
	// int error_code;

	// if (!zk.get_children(q_path, children, error_code)) {
		// TODO: Check error_code if desired
		// return 1;
	// }
	// if (children.size() < 1) {
		// std::cerr << "ERROR: Queue is empty, nothing to pop!" << std::endl;
		// return -1;
	// }

	// if (zk.delete_node(peek_path, error_code)) {
		// std::cerr << "The element '" << peek_path << "' was popped." << std::endl;
		// if (!zk.get_children(q_path, children, error_code)) {
			// TODO: Check error_code if desired
			// return 1;
		// }
		// if (children.size() > 0) {
			// peek_path = q_path + "/" + children.back();
		// } else {
			// peek_path = "";
		// }
		// std::cerr << "The first element of the queue is now: " << peek_path << std::endl;
		// return 0;
	// } else {
		// std::cerr << "ERROR: There was an error when popping the first element of the queue. Error code: " << error_code << std::endl;
		// std::cerr << "ERROR: The first element of the queue is still: " << peek_path << std::endl;
		// return 1;
	// }
	return true;
}

bool peek(const std::shared_ptr <ZKWrapper> zk, const std::string &q_path, std::string &peeked_path, int &error_code) {

	std::vector<std::string> children;
	if (!zk->get_children(q_path, children, error_code)) {
		LOG(ERROR) << "Failed to get children of queue node.";
		return false;
	}
	peeked_path = children.back();
	LOG(INFO) << "Queue head: " << peeked_path;

	return true;
}

