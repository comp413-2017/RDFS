//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"
#include <easylogging++.h>

const std::string ZKQueue::CLASS_NAME = ": **ZkQueuer** : ";

std::string q_path;
std::string peek_path;
std::string element;

std::string ZKQueue::peek() {
	return peek_path;
}

int ZKQueue::pop() {
	int error_code;
	if (zk.delete_node(peek_path, error_code)) {
		LOG(ERROR) << CLASS_NAME <<  "The element '"<< peek_path << "' was popped.";
		std::vector<std::string> children = std::vector<std::string>();
		// TODO: Don't set children
		if (zk.get_children(q_path, children, error_code)) {
			std::sort(children.begin(), children.end());
			peek_path = children.front();
			LOG(ERROR) << CLASS_NAME <<  "The first element of the queue is now: " << peek_path;
		}
		return error_code;
	} else {
		LOG(ERROR) << CLASS_NAME <<  "There was an error when popping the first element of the queue.";
		LOG(ERROR) << CLASS_NAME <<  "The first element of the queue is still: " << peek_path;
		return error_code;
	}
}

std::string ZKQueue::push(const std::vector<std::uint8_t> &data) {

	int error_code;
	std::string new_path;

	if (!zk.create_sequential(element, data, new_path, false, error_code)){
		LOG(ERROR) << CLASS_NAME <<  "Failed to push item!";
		// TODO: Handle error
	}
	LOG(INFO) << CLASS_NAME << "Added element to queue at: " << new_path;
	std::vector<std::string> children = std::vector<std::string>();
	// TODO: Dont' add children
	if (zk.get_children(q_path, children, error_code)) {
		if (children.size() == 1) {
			// Queue was empty before, so the just inserted element is the head
			peek_path = new_path;
			LOG(INFO) << CLASS_NAME << "Queue was empty, now head is: " << peek_path;
		}
	} else {
		// TODO: Handle exception
	}
	return new_path;
}
