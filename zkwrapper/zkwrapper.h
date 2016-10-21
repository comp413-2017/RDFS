#ifndef RDFS_ZKWRAPPER_H
#define RDFS_ZKWRAPPER_H

#include <string>
#include <vector>
#include <iostream>
#include <memory>
#include <cstring>
#include <zookeeper.h>
#include <map>
/**
 * Class representing a ZooKeeper op. Performs manual memory management on the data it contains: issues with
 * string.c_str()
 */
class ZooOp {
	public:

		ZooOp(const std::string& path_in, const std::vector<std::uint8_t>& data_in) {
			this->path = new char [path_in.size() + 1];
			strcpy (this->path, path_in.c_str());
			if (data_in.size() != 0) { // Only save non-empty data
				this->num_bytes = data_in.size();
				this->data = new char[this->num_bytes];
				memcpy(this->data, data_in.data(), data_in.size());
			}
			op = new zoo_op_t();
		}

		~ZooOp() {
			delete path;
			if (data) {
				delete data;
			}
			delete op;
		}

		zoo_op_t * op = nullptr;
		char * path = nullptr;
		char * data = nullptr;
		int num_bytes = 0;
};

class ZKWrapper {
	public:
		ZKWrapper(std::string host, int* errorcode);

		std::string translate_error(int errorcode);

		int create(const std::string &path, const std::vector<std::uint8_t> &data, int* errorcode, int flag = 0) const;

		int create_sequential(const std::string &path, const std::vector<std::uint8_t> &data,
				std::string &new_path, bool ephemeral) const;

		int recursive_create(const std::string &path, const std::vector<std::uint8_t> &data) const;

		bool exists(const std::string &path, const int watch) const;

		bool wexists(const std::string &path, watcher_fn watch, void* watcherCtx) const;

		int delete_node(const std::string &path) const;

		/**
		 * Recursively deletes the znode directory rooted at path. If the path ends with "/" then it only deletes
		 * the children of the paths, else it deletes the znode at the defined path itself.
		 * @param path
		 * @return
		 */
		int recursive_delete(const std::string path) const;

		std::vector <std::string> get_children(const std::string &path, const int watch) const;

		/* This function is similar to zoo_getchildren except it allows one specify
		 * a watcher object rather than a boolean watch flag.
		 */
		std::vector <std::string> wget_children(const std::string &path, watcher_fn  watch, void* watcherCtx) const;

		std::vector<std::uint8_t> get(const std::string &path, const int watch) const;

		std::vector<std::uint8_t> wget(const std::string &path, watcher_fn watch, void* watcherCtx) const;

		int set(const std::string &path, const std::vector<std::uint8_t> &data, int version = -1) const;

		/**
		 * @param path path of znode
		 * @param data data to initialize the node with. Set to the empty string to create an empty ZNode
		 * @param flags node flags: ZOO_EPHEMERAL, ZOO_SEQUENCE, ZOO_EPHEMERAL || ZOO_SEQUENCE
		 * @return a ZooOp to be used in execute_multi
		 */
		// TODO: Create a path buffer for returning sequential path names
		std::shared_ptr<ZooOp> build_create_op(const std::string& path, const std::vector<std::uint8_t> &data, const int flags = 0) const;

		/**
		 * @param path of znode
		 * @param version Checks the version of the znode before deleting. Defaults to -1, which does not perform the
		 *                check.
		 * @return a ZooOp to be used in execute_multi
		 */
		std::shared_ptr<ZooOp> build_delete_op(const std::string& path, int version = -1) const;

		/**
		 * @param path
		 * @param data
		 * @param version
		 * @return
		 */
		std::shared_ptr<ZooOp> build_set_op(const std::string& path, const std::vector<std::uint8_t> &data, int version = -1) const;

		/**
		 * Runs all of the zookeeper operations within the operations vector atomically (without ordering).
		 * Atomic execution mean that either all of the operations will succeed, else they will all
		 * be rolled back.
		 *
		 * @param operations a vector of operations to be executed
		 * @param results a vector that maps to the results of each of the executed operations
		 * @return the ZOO_API return code. Expect a 0 for non-error.
		 */
		int execute_multi(const std::vector<std::shared_ptr<ZooOp>> operations, std::vector<zoo_op_result>& results) const;

		void close();

		static std::vector<uint8_t> get_byte_vector(const std::string &string);

		static const std::vector<std::uint8_t> EMPTY_VECTOR;

	private:
		zhandle_t *zh;

		friend void watcher(zhandle_t *zzh, int type, int state, const char *path,
				void *watcherCtx);

		const static std::uint32_t MAX_PAYLOAD = 65536;
		const static std::uint32_t MAX_PATH_LEN = 512;

		const std::map<int, std::string> error_message = {
			{0, "ZOK"},
			{-1,"ZSYSTEMERROR"},
			{-2,"ZRUNTIMEINCONSISTENCY"},
			{-3,"ZDATAINCONSISTENCY"},
			{-4,"ZCONNECTIONLOSS"},
			{-5,"ZMARSHALLINGERROR"},
			{-6,"ZUNIMPLEMENTED"},
			{-7,"ZOPERATIONTIMEOUT"},
			{-8,"ZBADARGUMENTS"},
			{-9,"ZINVALIDSTATE"},
			{-100,"ZAPIERROR"},
			{-101,"ZNONODE"},
			{-102,"ZNOAUTH"},
			{-103,"ZBADVERSION"},
			{-108,"ZNOCHILDRENFOREPHEMERALS"},
			{-110,"ZNODEEXISTS"},
			{-111,"ZNOTEMPTY"},
			{-112,"ZSESSIONEXPIRED"},
			{-113,"ZINVALIDCALLBACK"},
			{-114,"ZINVALIDACL"},
			{-115,"ZAUTHFAILED"},
			{-116,"ZCLOSING"},
			{-117,"ZNOTHING"},
			{-118,"ZSESSIONMOVED"},
			{-120,"ZNEWCONFIGNOQUORUM"},
			{-121,"ZRECONFIGINPROGRESS"},
		};
};


#endif //RDFS_ZKWRAPPER_H
