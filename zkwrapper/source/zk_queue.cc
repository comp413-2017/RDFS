//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"

std::string element = "/q-item-";

bool push(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path, const std::vector<std::uint8_t> &pushed_data, int &error_code) {
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

bool pop(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path, std::vector<std::uint8_t> &popped_data, int &error_code) {
	std::vector<std::string> children;
	if (!zk->get_children(q_path, children, error_code)) {
		LOG(ERROR) << "Failed to get children of queue node: " << q_path;
		return false;
	}
	if (children.size() < 1) {
		std::cerr << "ERROR: Queue is empty, nothing to pop!" << std::endl;
		// TODO: set a custom error code?
		return false;
	}

	std::string peeked_path = q_path + "/" + children.back();
	LOG(INFO) << "Popping: " << peeked_path;

	if (!zk->delete_node(peeked_path, error_code)) {
		std::cerr << "ERROR: There was an error when popping the first element of the queue. Error code: " << error_code << std::endl;
		std::cerr << "ERROR: The first element of the queue is still: " << peeked_path << std::endl;
		return false;
	}
	LOG(INFO) << "The element '" << peeked_path << "' was popped.";

	return true;
}

bool peek(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path, std::string &peeked_path, int &error_code) {
	std::vector<std::string> children;
	if (!zk->get_children(q_path, children, error_code)) {
		LOG(ERROR) << "Failed to get children of queue node.";
		return false;
	}
	if (children.size() < 1) {
		LOG(ERROR) << "Queue:" << q_path << " is empty.";
		peeked_path = q_path;
		return true;
	}
	peeked_path = children.back();
	LOG(INFO) << "Queue head: " << peeked_path;

	return true;
}

