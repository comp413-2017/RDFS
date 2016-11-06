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

/**
 * To be used by the LeaseManager
 * This class simply contains the timer and name of the file
 */
class Lease {
	public:
		Lease(std::string);

		/**
		 * Set the time to 0
		 */
		void resetTimer();

		/**
		 * Return true if this lease is expired
		 * @param exp_time the time that the file will expire at
		 */
		bool isExpired(int exp_time);

		/**
		 * Get the name of the file
		 */
		std::string getFile();

	private:
		int time = 0;
		static const int LEASE_EXPIRATION_TIME;		
		std::string filename; 
};

/**
 * This is a wrapper around a mapping of clients to a list of their leases
 * Can add, remove, and update leases.
 */
class LeaseManager {
	public:
		LeaseManager();

		/**
		 * Add the file to the list of client's leases, returns false if file
		 * cannot be owned
		 * @param client
		 * @param file that the client is trying to own
		 */
		bool addLease(const std::string& client, const std::string& file);
		/**
		 * Returns false if we could not remove the lease (since it does not exist)
		 * @param client
		 * @param file to remove
		 */

		bool removeLease(const std::string& client, const std::string& file);
		/**
		 * Reset the timer on all leases for a client
		 * @param client
		 */
		void renewLeases(std::string client);

		/**
		 * Check all the leases in the system.
		 * Returns a list of files which need to be closed because they have expired
		 * @param exp_time the time that a file needs to surpass to be forcibly closed
		 * TODO return a list of pairs (client, file)
		 */
		std::vector<std::string> checkLeases(int exp_time);

	private:
		std::unordered_map<std::string, std::vector<Lease>> leases;
};

} // namespace
