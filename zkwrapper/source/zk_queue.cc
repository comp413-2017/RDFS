//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"
#include "zk_lock.h"
namespace zkqueue {
	std::string element = "/q-item-";

	bool push(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path,
			  const std::vector <std::uint8_t> &pushed_data, std::string &pushed_path, int &error_code) {
		LOG(INFO) << "Pushing to queue: " << q_path;

		// Check that the "queue" znode exists
		bool exists;
		if (!zk->exists(q_path, exists, error_code) || exists == false) {
			LOG(ERROR) << "There is no queue node at: " << q_path;
			return false;
		}

		// Push a new element on to the queue with the given data
		if (!zk->create_sequential(q_path + element, pushed_data, pushed_path, false, error_code)) {
			LOG(ERROR) << "There was an error when pushing new element onto the queue.";
			return false;
		}

		// TODO: This section is just for debugging / checking correctness, can remove
		std::vector <std::string> children;
		if (!zk->get_children(q_path, children, error_code)) {
			LOG(ERROR) << "Failed to get children of queue node.";
			return false;
		}

		return true;
	}

	bool pop(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path, std::vector <std::uint8_t> &popped_data,
			 std::string &popped_path, int &error_code) {
		std::vector <std::string> children;
		if (!zk->get_children(q_path, children, error_code)) {
			LOG(ERROR) << "Failed to get children of queue node: " << q_path;
			return false;
		}

		ZKLock q_lock(*zk, q_path);
		q_lock.lock();

		if (children.size() < 1) {
			LOG(ERROR) << "Queue is empty, nothing to pop!";
			error_code = ZNONODE;
			q_lock.unlock();
			return false;
		}

		if (!peek(zk, q_path, popped_path, error_code)) {
			LOG(ERROR) << "Failed to get head of queue!";
			q_lock.unlock();
			return false;
		}
		LOG(INFO) << "Popping: " << popped_path;

		if (!zk->get(popped_path, popped_data, error_code)) {
			LOG(ERROR) << "Failed to get data from popped node";
			q_lock.unlock();
			return false;
		}

		if (!zk->delete_node(popped_path, error_code)) {
			std::cerr << "ERROR: There was an error when popping the first element of the queue. Error code: "
					  << error_code << std::endl;
			std::cerr << "ERROR: The first element of the queue is still: " << popped_path << std::endl;
			q_lock.unlock();
			return false;
		}
		LOG(INFO) << "The element '" << popped_path << "' was popped.";

		q_lock.unlock();

		return true;
	}

	bool
	peek(const std::shared_ptr <ZKWrapper> &zk, const std::string &q_path, std::string &peeked_path, int &error_code) {
		std::vector <std::string> children;
		if (!zk->get_children(q_path, children, error_code)) {
			LOG(ERROR) << "Failed to get children of queue node.";
			return false;
		}
		if (children.size() < 1) {
			LOG(ERROR) << "Queue:" << q_path << " is empty.";
			peeked_path = q_path;
			return true;
		}

		std::sort(children.begin(), children.end());
		peeked_path = q_path + "/" + children.front();
		LOG(INFO) << "Queue head: " << peeked_path;

		return true;
	}
}