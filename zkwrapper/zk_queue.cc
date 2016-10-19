//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"
#include <zookeeper.h>

std::string q_path;
std::string peek_path;
std::string element;

std::string ZKQueue::peek() {
    return peek_path;
}

int ZKQueue::pop() {
    if (zk.delete_node(peek_path) == 0) {
        std::cerr << "The element '"<< peek_path << "' was popped." << std::endl;
        std::vector<std::string> children = zk.get_children(q_path, 0);
        peek_path = children.front();
        std::cerr << "The first element of the queue is now: " << peek_path << std::endl;
        return 0;
    } else {
        std::cerr << "There was an error when popping the first element of the queue." << std::endl;
        std::cerr << "The first element of the queue is still: " << peek_path << std::endl;
        return 1;
    }
}

std::string ZKQueue::push(const std::string &data, const int num_bytes) {
    zk.create(element, data, num_bytes, ZOO_SEQUENCE);
    std::vector<std::string> children = zk.get_children(q_path, 0);
    std::string last = children.back();
    std::cout << "Added element to queue at: " << last << std::endl;

    if (children.size() == 1) {
        // Queue was empty before, so the just inserted element is the head
        peek_path = children.front();
        std::cout << "Queue was empty, now head is: " << peek_path << std::endl;
    }

    return last;
}
