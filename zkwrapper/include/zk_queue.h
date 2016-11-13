//
// Created by Prudhvi Boyapalli on 10/17/16.
//

#ifndef RDFS_ZK_QUEUE_H
#define RDFS_ZK_QUEUE_H

#include <zookeeper.h>
#include "zkwrapper.h"


/**
 * Adds a new item to the end of the queue with the given data.
 * q_path: the ZK path to the desired queue, i.e. "/work_queues/wait_for_acks"
 * pushed_data: the byte vector of data to store in the queue item
 * error_code: reference to integer for holding an error code
 * return: boolean indicating success or failure of the method
 */
bool push(const std::string &q_path, const std::vector<std::uint8_t> &pushed_data, int &error_code);

/**
 * Removes the first item from the queue.
 * q_path: the ZK path to the desired queue, i.e. "/work_queues/wait_for_acks"
 * popped_data: reference to a byte vector to hold the data from the popped item
 * error_code: reference to integer for holding an error code
 * return: boolean indicating success or failure of the method
 */
bool pop(const std::string &q_path, const std::vector<std::uint8_t> &popped_data, int &error_code);

/**
 * Gets the name of the first item in the queue
 * q_path: the ZK path to the desired queue, i.e. "/work_queues/wait_for_acks"
 * peeked_path: string reference which will hold the path to the first element
 * error_code: reference to integer for holding an error code
 * return: boolean indicating success or failure of the method
 */
bool peek(const std::string &q_path, std::string &peeked_path, int &error_code);

#endif //RDFS_ZK_QUEUE_H
