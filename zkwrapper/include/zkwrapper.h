// Copyright 2017 Rice University, COMP 413 2017

#ifndef ZKWRAPPER_INCLUDE_ZKWRAPPER_H_
#define ZKWRAPPER_INCLUDE_ZKWRAPPER_H_

#include <zookeeper.h>
#include <easylogging++.h>
#include <string.h>
#include <string>
#include <vector>
#include <iostream>
#include <memory>
#include <cstring>
#include <map>

enum ZK_ERRORS {
  OK = 0,
  PATH_NOT_FOUND = -1
  // TODO(2016): Add more errors as needed
};

/**
 * Class representing a ZooKeeper op. Performs manual memory management on the
 * data it contains: issues with string.c_str()
 */
class ZooOp {
 public:
  ZooOp(const std::string &path_in,
        const std::vector<std::uint8_t> &data_in) {
    this->path = new char[path_in.size() + 1];
    snprintf(this->path, path_in.size() + 1, "%s", path_in.c_str());
    if (data_in.size() != 0) {  // Only save non-empty data
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

  zoo_op_t *op = nullptr;
  char *path = nullptr;
  char *data = nullptr;
  int num_bytes = 0;
};

class ZKWrapper {
 public:
  /**
   * Initializes zookeeper
   *
   * @param host The location of where Zookeeper is running. For local
   *        development this will usually be 'localhost:2181'
   * @param error_code Integer reference, set to a value in ZK_ERRORS
   *        Otherwise, an error code is returned. The meaning of an error code
   *        can be retrieved from translate_error()
   * @param root
   */
  ZKWrapper(std::string host, int &error_code, std::string root = "");

  /**
   * Prepends the ZooKeeper root to all paths passed in
   * @param path the original path
   * @return the updated path
   */
  std::string prepend_zk_root(const std::string &path) const;

  std::string removeZKRoot(const std::string &path) const;

  /**
   * Translate numerical error code to zookeeper error string
   *
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return A string translation of the error code
   */
  static std::string translate_error(int error_code);

  /**
   * Create a znode in zookeeper
   *
   * @param path The location of the new znode within the zookeeper structure
   * @param data The data contained in this znode
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool create(const std::string &path,
              const std::vector<std::uint8_t> &data,
              int &error_code,
              bool ephemeral,
              bool sync = true) const;

  bool create_ephemeral(const std::string &path,
                        const std::vector<std::uint8_t> &data,
                        int &error_code,
                        bool sync = true) const;

  /**
   * Creates a sequential znode
   *
   * @param path The path to the new sequential znode. The last component must
   *        end in "-" like: '/foo/bar-'. When the node is created, a 10 digit
   *        sequential ID unique to the parent node will be appended to the name.
   * @param data The data contained in this znode
   * @param new_path Will contain the value of the newly created path
   * @param ephemeral If true, the created node will ephemeral
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool create_sequential(const std::string &path,
                         const std::vector<std::uint8_t> &data,
                         std::string &new_path,
                         bool ephemeral,
                         int &error_code,
                         bool sync = true) const;

  /**
   * Recursively creates a new znode, non-existent znodes in the specified path
   * will be created
   *
   * @param path The path to create
   * @param data The data to store in the new znode
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool recursive_create(const std::string &path,
                        const std::vector<std::uint8_t> &data,
                        int &error_code,
                        bool sync = true) const;

  /**
   * Checks if a znode exists or not.
   *
   * @param path The path to the node
   * @param exist Set to true if a znode exists at the given path, false otherwise
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool exists(const std::string &path, bool &exist, int &error_code) const;

  /**
   * This function is similar to 'exists' except it allows the caller to
   * specify a watcher object rather than a boolean watch flag.
   *
   * @param path The path to the znode that needs to be checked
   * @param exist Set to true if a znode exists at the given path, false otherwise
   * @param watch A watcher function
   * @param watcherCtx User specific data, will be passed to the watcher callback.
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool wexists(const std::string &path,
               bool &exist,
               watcher_fn watch,
               void *watcherCtx,
               int &error_code) const;

  /**
   * Deletes a znode from zookeeper
   *
   * @param path The path to the znode that should be deleted
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool delete_node(const std::string &path,
                   int &error_code,
                   bool sync = true) const;

  /**
   * Recursively deletes the znode specified in the path and any children of that path
   *
   * @param path The path the znode (and its children) which will be deleted
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool recursive_delete(const std::string &path, int &error_code) const;

  /**
   * This function gets a list of children of the znode specified by the path
   *
   * @param path The path of parent node
   * @param children Reference to a vector which will be populated with the
   *        names of the children znodes of the given path
   *        TODO: How large should this vector be when passed in?
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool get_children(const std::string &path,
                    std::vector<std::string> &children,
                    int &error_code) const;

  /**
   * Similar to 'get_children', except it allows one to specify
   * a watcher object rather than a boolean watch flag.
   *
   * @param path The path to get children of and the node to place the watch on
   * @param children Reference to a vector which will be populated with the
   *        names of the children znodes of the given path
   *        TODO: How large should this vector be when passed in?
   * @param watch A watcher function
   * @param watcherCtx User specific data, will be passed to the watcher callback.
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool wget_children(const std::string &path,
                     std::vector<std::string> &children,
                     watcher_fn watch,
                     void *watcherCtx,
                     int &error_code) const;

  /**
   * Gets the data associated with a node
   *
   * @param path The path to the node
   * @param data Reference to a vector which will be filled with the znode data
   *        Should be of size MAX_PAYLOAD when passed in, will be resized in this method
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool get(const std::string &path,
           std::vector<std::uint8_t> &data,
           int &error_code,
           int length = MAX_PAYLOAD) const;

  /**
   * Gets the info associated with a znode
   *
   * @param path The path to the node
   * @param stat Reference to a stat struct to be filled with znode info
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   *		   False otherwise (caller should check 'error_code' value)
   */
  bool get_info(const std::string &path,
                struct Stat &stat,
                int &error_code) const;

  /**
   * This function is similar to 'get' except it allows one to specify
   * a watcher object rather than a boolean watch flag.
   *
   * @param path The path to the node
   * @param data Reference to a vector which will be filled with the znode data
   *        Should be of size MAX_PAYLOAD when passed in, will be resized in this method
   * @param watch A watcher function
   * @param watcherCtx User specific data, will be passed to the watcher callback.
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool wget(const std::string &path,
            std::vector<std::uint8_t> &data,
            watcher_fn watch,
            void *watcherCtx,
            int &error_code,
            int length = MAX_PAYLOAD) const;

  /**
   * Sets the data in a given znode
   *
   * @param path The path to the znode
   * @param data The data that this znode should contain
   * @param version A version number indicating changes to the data at this node
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation completed successfully,
   * 		   False otherwise (caller should check 'error_code' value)
   */
  bool set(const std::string &path,
           const std::vector<std::uint8_t> &data,
           int &error_code,
           bool sync = true,
           int version = -1) const;

  /**
   * @param path path of znode
   * @param data data to initialize the node with. Set to the empty string to
   *        create an empty znode
   * @param flags node flags: ZOO_EPHEMERAL, ZOO_SEQUENCE, ZOO_EPHEMERAL || ZOO_SEQUENCE
   * @return a ZooOp to be used in execute_multi
   */
  // TODO(2016): Crexate a path buffer for returning sequential path names
  std::shared_ptr<ZooOp> build_create_op(const std::string &path,
                                         const std::vector<std::uint8_t> &data,
                                         const int flags = 0) const;

  /**
   * @param path of znode
   * @param version Checks the version of the znode before deleting. Defaults to -1, which does not perform the
   *                check.
   * @return a ZooOp to be used in execute_multi
   */
  std::shared_ptr<ZooOp> build_delete_op(const std::string &path,
                                         int version = -1) const;

  /**
   * @param path
   * @param data
   * @param version
   * @return
   */
  std::shared_ptr<ZooOp> build_set_op(const std::string &path,
                                      const std::vector<std::uint8_t> &data,
                                      int version = -1) const;

  /**
   * Runs all of the zookeeper operations within the operations vector atomically (without ordering).
   * Atomic execution mean that either all of the operations will succeed, else they will all
   * be rolled back.
   *
   * @param operations a vector of operations to be executed
   * @param results a vector that maps to the results of each of the executed operations
   * @param error_code Int reference, set to a value in ZK_ERRORS
   * @return True if the operation worked successfully; false otherwise.
   */
  bool execute_multi(const std::vector<std::shared_ptr<ZooOp>> operations,
                     std::vector<zoo_op_result> &results,
                     int &error_code,
                     bool sync = true) const;

  /**
   * Flush changes inside of ZooKeeper
   * @param full_path the full path of the znode directory to be flushed. Must be qualified with the ZooKeeper root
   * @param synchronous Whether this operation is blocking
   * @return true on success
   */
  bool flush(const std::string &full_path, bool synchronous = true) const;

  void close();

  static std::vector<uint8_t> get_byte_vector(const std::string &string);

  static void print_error(int error) {
    LOG(ERROR) << "Got error: " << translate_error(error);
  }

  static const std::vector<std::uint8_t> EMPTY_VECTOR;

 private:
  zhandle_t *zh;

  friend void watcher(zhandle_t *zzh, int type, int state, const char *path,
                      void *watcherCtx);

  std::string root = "";
  static const std::uint32_t MAX_PAYLOAD = 65536;
  static const std::uint32_t MAX_PATH_LEN = 512;
  static const int NUM_SEQUENTIAL_DIGITS = 10;

  static const std::map<int, std::string> error_message;
  static const std::string CLASS_NAME;
};

#endif  // ZKWRAPPER_INCLUDE_ZKWRAPPER_H_
