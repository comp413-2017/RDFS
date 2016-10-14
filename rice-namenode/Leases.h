#include <iostream>
#include <unordered_map>
#include <vector>

#pragma once

/**
 * Takes care of leases and lease operations. A namenode, upon creation,
 * creates a lease manager. The namenode then spawns a thread which 
 * periodically calls checkLeases to get all expired files. A lease is
 * created upon a file being created by a specific client, and we expect
 * the client to renew the lease periodically until the client closes the
 * file.  
 */
namespace lease {

class Lease {
	public:
		Lease(std::string);
		void resetTimer();
		bool isExpired(int);
		std::string getFile();

	private:
		int time = 0;
		static const int LEASE_EXPIRATION_TIME;		
		std::string filename; 
};

class LeaseManager {
	public:
		LeaseManager();
		bool addLease(std::string, std::string);
		bool removeLease(std::string, std::string);
		void renewLeases(std::string);
		std::vector<std::string> checkLeases(int);

	private:
		std::unordered_map<std::string, std::vector<Lease>> leases;
};

} // namespace
