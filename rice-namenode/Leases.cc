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

const int Lease::LEASE_EXPIRATION_TIME = 3600; // close file if its been an hour 

Lease::Lease(std::string in) : filename(in) {}

/**
 * This happens upon a renewal of a lease
 */
void Lease::resetTimer() {
	time = 0;
}

/**
 * This happens every LEASE_CHECK_TIME seconds. If the file is
 * expired, then we forcibly close it. Otherwise, we hadd 60
 * seconds onto the timer 
 */
bool Lease::isExpired(int time) {
	// TODO close the file and recover the blocks
	if (time >= LEASE_EXPIRATION_TIME) {
		return false;
	}
	time += time;
	return true; 	
}

std::string Lease::getFile() {
	return filename; 
}

LeaseManager::LeaseManager() {}

/**
 * Create a lease for a file 
 */
bool LeaseManager::addLease(std::string client, std::string file) {
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
bool LeaseManager::removeLease(std::string client, std::string file) {
	int i = 0;
	for (Lease lease : leases[client]) {
		if (lease.getFile() == file) {
			leases[client].erase(leases[client].begin() + i);
			return true; 
		}
		i++;
	}
	return false; // could not find the lease
}

/**
 * Reset the timer for all leases belonging to client
 */ 
void LeaseManager::renewLeases(std::string client) {
	for (Lease lease : leases[client]) {
		lease.resetTimer();
	}
}

/**
 * Loop through all the leases and return all that are expired 
 */
std::vector<std::string> LeaseManager::checkLeases(int time) {
	std::vector<std::string> expiredFiles;
	LOG(INFO) << "Checking leases";
	// iterate through map, and for each lease, call isExpired() 
	for (auto x = leases.begin(); x != leases.end(); x++) {
		std::string client = x->first;
		std::vector<Lease> clientLeases = x->second;
		for (Lease lease : clientLeases) {
			if (lease.isExpired(time)) {
				expiredFiles.push_back(lease.getFile());
			}
		}
	}
	return expiredFiles; 
}

}
