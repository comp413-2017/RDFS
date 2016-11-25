#include <iostream>
#include <string>
#include <thread>

#include <easylogging++.h>
#include <rpcserver.h>
#include <ConfigReader.h>

#include "Leases.h"

/**
 * The implementation of the rpc calls. 
 */
namespace lease {

const int LEASE_EXPIRATION_TIME = 10; // close file if its been an hour

Lease::Lease(std::string in) : filename(in) {}

/**
 * This happens upon a renewal of a lease
 */
void Lease::resetTimer() {
	my_time = 0;
}


std::string Lease::getFile() {
	return filename;
}

LeaseManager::LeaseManager() {}

/**
 * Create a lease for a file 
 */
bool LeaseManager::addLease(const std::string& client, const std::string& file) {
	LOG(DEBUG) << "Adding lease " << file << " for client";
	if (leases.find(client) == leases.end()) { // if this client is not in the map, init the vector
		std::vector<Lease> clientLeases;
		leases[client] = clientLeases; 
	}
	Lease fresh_lease(file);
	leases[client].push_back(fresh_lease);
	return true;
}

/**
 * Remove the lease associated with the client and file
 */
bool LeaseManager::removeLease(const std::string& client, const std::string& file) {
	int i = 0;
	if (leases.find(client) == leases.end()) { // if this client is not in the map, init the vector
		std::vector<Lease> clientLeases;
		leases[client] = clientLeases;
		return false;
	}
	for (Lease lease : leases[client]) {
		if (lease.getFile() == file) {
			leases[client].erase(leases[client].begin() + i);
			LOG(DEBUG) << "Removing lease for  " << client << " and " << file << " success";
			return true;
		}
		i++;
	}
	LOG(DEBUG) << "Could not remove lease for " << client << " and " << file;
	return false; // could not find the lease
}

/**
 * Reset the timer for all leases belonging to client
 */ 
void LeaseManager::renewLeases(std::string client) {
	LOG(DEBUG) << "Renewing leases for " << client;
	int i = 0;
	for (Lease lease : leases[client]) {
		lease.my_time = 0;
		leases[client][i++] = lease;
	}
}

void LeaseManager::renameLease(std::string file, std::string new_name) {
	for (const auto &myPair : leases ) {
		for (auto lease : myPair.second) {
			if (file == lease.getFile()) {
				removeLease(myPair.first, file);
				addLease(myPair.first, file);
				return;
			}
		}
	}
}

bool LeaseManager::checkLease(const std::string& client, const std::string& file) {
	LOG(DEBUG) << "Checking if lease for  " << client << " and " << file << " exists";
	int i = 0;
	if (leases.find(client) == leases.end()) { // if this client is not in the map, init the vector
		std::vector<Lease> clientLeases;
		leases[client] = clientLeases;
		return false;
	}
	for (Lease& lease : leases[client]) {
		if (lease.getFile() == file) {
			LOG(DEBUG) << "Found lease for  " << client << " and " << file << " with time " << lease.my_time ;
			return true;
		}
		i++;
	}
	return false;
}

/**
 * Loop through all the leases and return all that are expired 
 */
std::vector<std::pair<std::string, std::string>> LeaseManager::checkLeases(int time) {
	std::vector<std::pair<std::string, std::string>> expiredFiles;
	LOG(DEBUG) << "Checking for expired leases " << time;
	// iterate through map, and for each lease, call isExpired() 
	for (auto x = leases.begin(); x != leases.end(); x++) {
		std::string client = x->first;
		std::vector<Lease> clientLeases = x->second;
		int k = 0;
		for (Lease lease : clientLeases) {
			lease.my_time += time;
			LOG(DEBUG) << "Updated lease " << lease.getFile() << " with current time " << lease.my_time;
			if (lease.my_time >= LEASE_EXPIRATION_TIME) {
				LOG(DEBUG) << "Expired lease for client  " << client << " with file " << lease.getFile() << " at time" << time;
				expiredFiles.push_back(std::make_pair(client, lease.getFile()));
			}
			leases[client][k++] = lease;
		}
	}
	return expiredFiles; 
}

}
