//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#include "zk_queue.h"

std::string q_path;
std::string peek_path;
std::string element;

std::string ZKQueue::peek() {
    return peek_path;
}

int ZKQueue::pop() {
    int error_code;
    if (zk.delete_node(peek_path, error_code)) {
        std::cerr << "The element '"<< peek_path << "' was popped." << std::endl;
        std::vector<std::string> children = std::vector<std::string>();
        // TODO: Don't set children
        if (zk.get_children(q_path, children, error_code)) {
            std::sort(children.begin(), children.end());
            peek_path = children.front();
            std::cerr << "The first element of the queue is now: " << peek_path << std::endl;
            return 0;
        } else {
            // TODO: Handle error better
            return 1;
        }
    } else {
        std::cerr << "There was an error when popping the first element of the queue." << std::endl;
        std::cerr << "The first element of the queue is still: " << peek_path << std::endl;
        return 1;
    }
}

std::string ZKQueue::push(const std::vector<std::uint8_t> &data) {

    int error_code;
    std::string new_path;

    if (!zk.create_sequential(element, data, new_path, false, error_code)){
        std::cerr << "Failed to push item!" << std::endl;
        // TODO: Handle error
    }
    std::cout << "Added element to queue at: " << new_path << std::endl;
    std::vector<std::string> children = std::vector<std::string>();
    // TODO: Dont' add children
    if (zk.get_children(q_path, children, error_code)) {
        if (children.size() == 1) {
            // Queue was empty before, so the just inserted element is the head
            peek_path = new_path;
            std::cout << "Queue was empty, now head is: " << peek_path << std::endl;
        }
    } else {
        // TODO: Handle exception
    }
    return new_path;
}
