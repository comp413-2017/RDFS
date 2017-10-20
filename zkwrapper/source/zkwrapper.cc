// Copyright 2017 Rice University, COMP 413 2017

#include "zkwrapper.h"

#include <mutex>
#include <condition_variable>
#include <thread>

int init = 0;
zhandle_t *zh;
clientid_t myid;

const std::vector<std::uint8_t>
    ZKWrapper::EMPTY_VECTOR = std::vector<std::uint8_t>(0);

const std::map<int, std::string> ZKWrapper::error_message = {
    {0, "ZOK"},
    {-1, "ZSYSTEMERROR"},
    {-2, "ZRUNTIMEINCONSISTENCY"},
    {-3, "ZDATAINCONSISTENCY"},
    {-4, "ZCONNECTIONLOSS"},
    {-5, "ZMARSHALLINGERROR"},
    {-6, "ZUNIMPLEMENTED"},
    {-7, "ZOPERATIONTIMEOUT"},
    {-8, "ZBADARGUMENTS"},
    {-9, "ZINVALIDSTATE"},
    {-100, "ZAPIERROR"},
    {-101, "ZNONODE"},
    {-102, "ZNOAUTH"},
    {-103, "ZBADVERSION"},
    {-108, "ZNOCHILDRENFOREPHEMERALS"},
    {-110, "ZNODEEXISTS"},
    {-111, "ZNOTEMPTY"},
    {-112, "ZSESSIONEXPIRED"},
    {-113, "ZINVALIDCALLBACK"},
    {-114, "ZINVALIDACL"},
    {-115, "ZAUTHFAILED"},
    {-116, "ZCLOSING"},
    {-117, "ZNOTHING"},
    {-118, "ZSESSIONMOVED"},
    {-120, "ZNEWCONFIGNOQUORUM"},
    {-121, "ZRECONFIGINPROGRESS"},
    {-999, "ZKWRAPPERDEFAULTERROR"},
};

/**
 * Currently watches the zookeeper handle and closes zookeeper on
 * failed auths or expired sessions.
 *
 * @param zzh zookeeper handle
 * @param type type of event
 * @param state state of the event
 * @param path path to the watcher node
 * @param watcherCtx the state of the watcher
 */
void watcher(zhandle_t *zzh,
             int type,
             int state,
             const char *path,
             void *watcherCtx) {
  LOG(INFO) << "[Global watcher] Watcher triggered on path '" << path << "'";
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      return;
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      zookeeper_close(zzh);
      exit(1);
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      zookeeper_close(zzh);
      exit(1);
    }
  }
}

std::string ZKWrapper::translate_error(int errorcode) {
  std::string message;
  message = error_message.at(errorcode);
  return message;
}

ZKWrapper::ZKWrapper(std::string host, int &error_code, std::string root_path) {
  // TODO(2016): Move these default values to some readable CONSTANT value
  zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
  if (!zh) {
    LOG(ERROR) << "zk init failed!";
    error_code = -999;
  }
  init = 1;
  if (root_path.size() != 0) {
    bool root_exists = false;
    while (!root_exists) {
      if (!exists(root_path, root_exists, error_code)) {
        LOG(ERROR) << "Failed to check if root directory "
                << root
                << " exists "
                << error_code;
      }
      if (!root_exists) {
        if (!recursive_create(root_path, EMPTY_VECTOR, error_code)) {
          LOG(ERROR) << "Failed to create root directory "
                  << root
                  << " with error "
                  << error_code;
        } else {
          root_exists = true;
          break;
        }
      }
    }

    if (!root_exists) {
      if (!recursive_create(root_path, EMPTY_VECTOR, error_code)) {
        LOG(ERROR) << "Failed to create root directory "
                << root
                << " with error "
                << error_code;
      }
    }
  }
  LOG(INFO) << "SUCCESSFULLY STARTED ZKWRAPPER";
  root = root_path;
}

std::string ZKWrapper::prepend_zk_root(const std::string &path) const {
  if (root.size() == 0) {
    return path;
  }
  if (path == "/") {
    return root;
  }
  return root + path;
}

std::string ZKWrapper::removeZKRoot(const std::string &path) const {
  if (path.substr(0, root.size()) == root) {
    return path.substr(root.size());
  }
  return path;
}

/* Wrapper Implementation of Zookeeper Functions */

bool ZKWrapper::create(const std::string &path,
                       const std::vector<std::uint8_t> &data,
                       int &error_code,
                       bool ephemeral,
                       bool sync) const {
  if (!init) {
    LOG(ERROR) << "Attempt to create before init!";
    error_code = -999;
    return false;
  }
  auto real_path = prepend_zk_root(path);
  LOG(INFO) << "creating ZNode at " << real_path;
  int flag = (ephemeral) ? ZOO_EPHEMERAL : 0;
  int rc = zoo_create(zh,
                      real_path.c_str(),
                      reinterpret_cast<const char *>(data.data()),
                      data.size(),
                      &ZOO_OPEN_ACL_UNSAFE,
                      flag,
                      nullptr,
                      0);
  error_code = rc;

  if (sync && !rc) {
    flush(prepend_zk_root(path));
  }

  if (!rc)
    return true;
  LOG(ERROR) << "Failed to create ZNode at " << real_path;
  print_error(error_code);
  return false;
}

// TODO(2016): Modify this
bool ZKWrapper::create_sequential(const std::string &path,
                                  const std::vector<std::uint8_t> &data,
                                  std::string &new_path,
                                  bool ephemeral,
                                  int &error_code,
                                  bool sync) const {
  LOG(INFO) << "Starting sequential for " << path;
  if (!init) {
    LOG(ERROR) << "Attempt to create sequential before init!";
    return false;
  }
  int flag = ZOO_SEQUENCE;
  if (ephemeral) {
    flag = flag | ZOO_EPHEMERAL;
  }
  LOG(INFO) << "Attempting to generate new path" << new_path;
  LOG(INFO) << "Creating seq ZNode at " << prepend_zk_root(path);

  int len = prepend_zk_root(path).size();
  new_path.resize(MAX_PATH_LEN);
  int rc = zoo_create(zh,
                      prepend_zk_root(path).c_str(),
                      reinterpret_cast<const char *>(data.data()),
                      data.size(),
                      &ZOO_OPEN_ACL_UNSAFE,
                      flag,
                      reinterpret_cast<char *>(&new_path[0]),
                      MAX_PATH_LEN);
  error_code = rc;
  if (rc) {  // Z_OK is 0, so if we receive anything else fail
    LOG(ERROR) << "Create for " << prepend_zk_root(path) << " failed " << rc;
    print_error(error_code);
    return false;
  }
  int i = 0;
  LOG(INFO) << "NEW path is " << new_path;
  new_path.resize(len + NUM_SEQUENTIAL_DIGITS);

  if (sync && !rc) {
    flush(new_path);
  }

  new_path = removeZKRoot(new_path);
  LOG(INFO) << "NEW path is now this" << new_path;

  return true;
}

bool ZKWrapper::recursive_create(const std::string &path,
                                 const std::vector<std::uint8_t> &data,
                                 int &error_code,
                                 bool sync) const {
  for (int i = 1; i < path.length(); ++i) {
    if (path[i] == '/') {
      LOG(INFO) << "Generating " << path.substr(0, i);
      if (!create(path.substr(0, i),
                  ZKWrapper::EMPTY_VECTOR,
                  error_code,
                  false,
                  false)) {
        if (error_code != ZNODEEXISTS) {
          LOG(ERROR) << "Failed to recursively create " << path;
          print_error(error_code);
          return false;
        }
      }
      error_code = ZOK;
    }
  }
  LOG(INFO) << "Generating " << path;
  return create(path, data, error_code, false, sync);
}

bool ZKWrapper::wget(const std::string &path,
                     std::vector<std::uint8_t> &data,
                     watcher_fn watch,
                     void *watcherCtx,
                     int &error_code,
                     int length) const {
  // TODO(2016): Make this a constant value.
  // Define a smarter retry policy for oversized data
  int len = length;
  data.resize(len);
  struct Stat stat;
  error_code = zoo_wget(zh,
                        prepend_zk_root(path).c_str(),
                        watch,
                        watcherCtx,
                        reinterpret_cast<char *>(data.data()),
                        &len,
                        &stat);
  if (error_code != ZOK) {
    LOG(ERROR) << "wget on " << path << " failed";
    print_error(error_code);
    return false;
  }
  data.resize(len);
  return true;
}

bool ZKWrapper::get(const std::string &path,
                    std::vector<std::uint8_t> &data,
                    int &error_code,
                    int length) const {
  // TODO(2016): Make this a constant value.
  // Define a smarter retry policy for oversized data
  struct Stat stat;
  int len = length;
  // TODO(2016): Perhaps we can be smarter about this
  data.resize(len);
  error_code = zoo_get(zh,
                       prepend_zk_root(path).c_str(),
                       0,
                       reinterpret_cast<char *>(data.data()),
                       &len,
                       &stat);
  if (error_code != ZOK) {
    LOG(ERROR) << "get on " << path << " failed";
    print_error(error_code);
    return false;
  }
  data.resize(len);
  return true;
}

bool ZKWrapper::set(const std::string &path,
                    const std::vector<std::uint8_t> &data,
                    int &error_code,
                    bool sync,
                    int version) const {
  error_code = zoo_set(zh,
                       prepend_zk_root(path).c_str(),
                       reinterpret_cast<const char *>(data.data()),
                       data.size(),
                       version);
  if (error_code != ZOK) {
    LOG(ERROR) << "set on " << path << " failed";
    print_error(error_code);
    return false;
  }

  if (sync) {
    flush(prepend_zk_root(path));
  }

  return true;
}

bool ZKWrapper::exists(const std::string &path,
                       bool &exist,
                       int &error_code) const {
  // TODO(2016): for now watch argument is set to 0, need more error checking
  int rc = zoo_exists(zh, prepend_zk_root(path).c_str(), 0, 0);
  error_code = rc;
  if (rc == ZOK) {
    exist = true;
    return true;
  } else if (rc == ZNONODE) {
    exist = false;
    return true;
  } else {
    // NOTE: value exist is not updated in this case
    LOG(ERROR) << "exists on " << path << " failed";
    print_error(error_code);
    return false;
  }
}

bool ZKWrapper::wexists(const std::string &path,
                        bool &exist,
                        watcher_fn watch,
                        void *watcherCtx,
                        int &error_code) const {
  struct Stat stat;
  int rc = zoo_wexists(zh,
                       prepend_zk_root(path).c_str(),
                       watch,
                       watcherCtx, &stat);
  error_code = rc;
  if (rc == ZOK) {
    exist = true;
    return true;
  } else if (rc == ZNONODE) {
    exist = false;
    return true;
  } else {
    // NOTE: value exist is not updated in this case
    LOG(ERROR) << "wexists on " << path << " failed";
    print_error(error_code);
    return false;
  }
}

bool ZKWrapper::delete_node(const std::string &path,
                            int &error_code,
                            bool sync) const {
  // NOTE: use -1 for version, check will not take place.
  error_code = zoo_delete(zh, prepend_zk_root(path).c_str(), -1);
  if (error_code != ZOK) {
    LOG(ERROR) << "delete on " << path << " failed";
    print_error(error_code);
    return false;
  }

  // TODO(2016)
  if (sync && !error_code) {
    flush(prepend_zk_root(path));
  }
  return true;
}

bool ZKWrapper::get_info(const std::string &path,
                         struct Stat &stat,
                         int &error_code) const {
  std::vector<std::uint8_t> data;
  int len = MAX_PAYLOAD;
  data.resize(len);

  error_code = zoo_get(zh,
                       prepend_zk_root(path).c_str(),
                       0,
                       reinterpret_cast<char *>(data.data()),
                       &len,
                       &stat);
  if (error_code != ZOK) {
    LOG(ERROR) << "get on " << path << " failed";
    print_error(error_code);
    return false;
  }
  data.resize(len);
  return true;
}

// TODO(2016): Modify
bool ZKWrapper::recursive_delete(const std::string &path,
                                 int &error_code) const {
  LOG(INFO) << "Recursively deleting " << path;
  bool root = ("/" == path);
  bool endsSlash = path[path.size() - 1] == '/';
  int rc = 0;

  std::string znodePath = endsSlash ? path.substr(0, path.size() - 1) : path;
  std::vector<std::string> children;
  if (!get_children(root ? "/" : znodePath, children, rc)) {
    LOG(ERROR) << "recursive_delete on "
            << path
            << " failed: couldn't get children";
    return false;
  }

  for (auto child : children) {
    LOG(INFO) << "child is " << child;
    if (child.size() == 0) {
      continue;
    }
    std::string newPath = znodePath + "/" + child;
    int result = recursive_delete(newPath, error_code);
    rc = (result != 0) ? result : rc;
  }

  int result = delete_node(path, error_code);
  rc = (result != 0) ? result : rc;

  if (error_code != ZOK) {
    LOG(ERROR) << "recursive_delete on " << path << " failed.";
    return false;
  }
  return true;
}

bool ZKWrapper::get_children(const std::string &path,
                             std::vector<std::string> &children,
                             int &error_code) const {
  struct String_vector stvector;
  struct String_vector *vector = &stvector;
  error_code = zoo_get_children(zh, prepend_zk_root(path).c_str(), 0, vector);
  if (error_code != ZOK) {
    LOG(ERROR) << "get_children on " << path << " failed";
    print_error(error_code);
    return false;
  }
  int i;
  for (i = 0; i < stvector.count; i++) {
    children.push_back(stvector.data[i]);
  }
  return true;
}

bool ZKWrapper::wget_children(const std::string &path,
                              std::vector<std::string> &children,
                              watcher_fn watch,
                              void *watcherCtx,
                              int &error_code) const {
  struct String_vector stvector;
  struct String_vector *vector = &stvector;
  error_code = zoo_wget_children(zh,
                                 prepend_zk_root(path).c_str(),
                                 watch,
                                 watcherCtx,
                                 vector);
  if (error_code != ZOK) {
    LOG(ERROR) << "wget_children on " << path << " failed";
    print_error(error_code);
    return false;
  }

  int i;
  for (i = 0; i < stvector.count; i++) {
    children.push_back(stvector.data[i]);
  }
  return true;
}

/* Multi-Operations */

std::shared_ptr<ZooOp> ZKWrapper::build_create_op(
        const std::string &path,
        const std::vector<std::uint8_t> &data,
        const int flags) const {
  auto op = std::make_shared<ZooOp>(prepend_zk_root(path), data);
  zoo_create_op_init(op->op,
                     op->path,
                     op->data,
                     op->num_bytes,
                     &ZOO_OPEN_ACL_UNSAFE,
                     flags,
                     nullptr,
                     0);
  return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_delete_op(const std::string &path,
                                                  int version) const {
  auto op = std::make_shared<ZooOp>(prepend_zk_root(path),
                                    ZKWrapper::EMPTY_VECTOR);
  zoo_delete_op_init(op->op, op->path, version);
  return op;
}

std::shared_ptr<ZooOp> ZKWrapper::build_set_op(
        const std::string &path,
        const std::vector<std::uint8_t> &data,
        int version) const {
  auto op = std::make_shared<ZooOp>(prepend_zk_root(path), data);
  zoo_set_op_init(op->op,
                  op->path,
                  op->data,
                  op->num_bytes,
                  version,
                  nullptr);
  return op;
}

bool ZKWrapper::execute_multi(const std::vector<std::shared_ptr<ZooOp>> ops,
                              std::vector<zoo_op_result> &results,
                              int &error_code,
                              bool sync) const {
  std::vector<zoo_op_t> trueOps = std::vector<zoo_op_t>();
  results.resize(ops.size());
  for (auto op : ops) {
    trueOps.push_back(*(op->op));
  }
  error_code = zoo_multi(zh, ops.size(), &trueOps[0], &results[0]);
  if (error_code != ZOK) {
    LOG(ERROR) << "multiop failed";
    print_error(error_code);
    return false;
  }

  if (sync && !error_code) {
    flush(root);
  }
  return true;
}

std::vector<uint8_t> ZKWrapper::get_byte_vector(const std::string &string) {
  std::vector<uint8_t> vec(string.begin(), string.end());
  return vec;
}

void ZKWrapper::close() {
  zookeeper_close(zh);
}

bool ZKWrapper::flush(const std::string &full_path, bool synchronous) const {
  // flush is only blocking when the synchronous flag is set
  if (synchronous) {
    // I tried using condition variables,
    // but it ended up failing on occasion, so sadly I resort to polling :(
    bool *flag = reinterpret_cast<bool *>(malloc(sizeof(bool)));

    // Lambda to call on function completion
    string_completion_t completion = [](int rc,
                                        const char *value,
                                        const void *data) {
      bool *flag_ptr = const_cast<bool *>(reinterpret_cast<const bool *>(data));
      *flag_ptr = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      free(flag_ptr);
    };

    int rc = zoo_async(zh, full_path.c_str(), completion, flag);

    // Exit early if async failed
    if (rc) {
      LOG(ERROR) << "Flushing " << full_path << " failed";
      print_error(rc);
      return rc;
    }

    // Wait for the asynchronous function to complete
    int count = 0;
    // ZooKeeper is guaranteed to be synced within 2 seconds
    while (!(*flag) && count < 2000) {
      count++;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (count == 2000) {
      LOG(ERROR) << "SYNC FOR" << full_path << " was slow";
    }
    return rc;
  } else {
    auto no_op = [&](int rc, const char *value, const void *data) {};
    return zoo_async(zh, full_path.c_str(), no_op, nullptr);
  }
}
