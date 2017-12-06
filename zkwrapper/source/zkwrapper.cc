// Copyright 2017 Rice University, COMP 413 2017

#include "zkwrapper.h"
#include <mutex>
#include <condition_variable>
#include <thread>

const std::vector<std::uint8_t>
    ZKWrapper::EMPTY_VECTOR = std::vector<std::uint8_t>(0);

const int ZKWrapper::MAX_PAYLOAD = 65536;
const int ZKWrapper::MAX_PATH_LEN = 512;
const int ZKWrapper::NUM_SEQUENTIAL_DIGITS = 10;
const int ZKWrapper::DEFAULT_ZK_RECV_TIMEOUT = 10000;
const int ZKWrapper::INITIAL_CONNECTION_TIMEOUT_MILLIS = 20000;
const int ZKWrapper::INITIAL_CONNECTION_RETRY_INTERVAL_MILLIS = 2500;
const int ZKWrapper::ROOT_CREATION_RETRY_LIMIT = 5;
const int ZKWrapper::ROOT_CREATION_RETRY_INTERVAL_MILLIS = 2500;

/* TODO(2017) most of the shared_ptr's can be replaced by unique_ptr's. */

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
  LOG(INFO) << "[Global watcher] Watcher triggered on path '" << path << "'"
            << " with type " << ZKWrapper::translate_watch_event_type(type)
            << " and state " << ZKWrapper::translate_watch_state(state);
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      auto zkWrapper = reinterpret_cast<ZKWrapper *>(watcherCtx);
      zkWrapper->connected = true;
      return;
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      zookeeper_close(zzh);
      exit(1);
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      zookeeper_close(zzh);
      exit(1);
    }
  }
  LOG(INFO) << "[Global watcher] Falling through and doing nothing";
}

void ZKWrapper::watcher_znode_data(zhandle_t *zzh,
            int type,
            int state,
            const char *path,
            void *watcherCtx) {
  LOG(INFO) << "[Znode watcher] Watcher triggered on path '" << path << "'"
            << " with type " << ZKWrapper::translate_watch_event_type(type)
            << " and state " << ZKWrapper::translate_watch_state(state);

  auto zkWrapper = reinterpret_cast<ZKWrapper *>(watcherCtx);
  auto src = zkWrapper->removeZKRootAndDir(zkWrapper->root + "/fileSystem",
                       std::string(path));
  zkWrapper->cache->remove(src);
}

std::string ZKWrapper::translate_error(int errorcode) {
  static const std::map<int, std::string> ERROR_MESSAGES = {
          {0,    "ZOK"},
          {-1,   "ZSYSTEMERROR"},
          {-2,   "ZRUNTIMEINCONSISTENCY"},
          {-3,   "ZDATAINCONSISTENCY"},
          {-4,   "ZCONNECTIONLOSS"},
          {-5,   "ZMARSHALLINGERROR"},
          {-6,   "ZUNIMPLEMENTED"},
          {-7,   "ZOPERATIONTIMEOUT"},
          {-8,   "ZBADARGUMENTS"},
          {-9,   "ZINVALIDSTATE"},
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
          {-997, "ZKWRAPPERINSUFFICIENTBUFFER"},
          {-998, "ZKWRAPPERUNINITIALIZED"},
          {-999, "ZKWRAPPERDEFAULTERROR"},
  };
  try {
    return ERROR_MESSAGES.at(errorcode);
  } catch (const std::out_of_range&) {
    return "Unknown error code " + std::to_string(errorcode);
  }
}

std::string ZKWrapper::translate_watch_event_type(int type) {
  static const std::map<int, std::string> WATCH_EVENT_TYPE = {
          {ZOO_CREATED_EVENT,     "ZOO_CREATED_EVENT"},
          {ZOO_DELETED_EVENT,     "ZOO_DELETED_EVENT"},
          {ZOO_CHANGED_EVENT,     "ZOO_CHANGED_EVENT"},
          {ZOO_CHILD_EVENT,       "ZOO_CHILD_EVENT"},
          {ZOO_SESSION_EVENT,     "ZOO_SESSION_EVENT"},
          {ZOO_NOTWATCHING_EVENT, "ZOO_NOTWATCHING_EVENT"}
  };
  try {
    return WATCH_EVENT_TYPE.at(type);
  } catch (const std::out_of_range&) {
    return "Unknown type " + std::to_string(type);
  }
}

std::string ZKWrapper::translate_watch_state(int state) {
  static const std::map<int, std::string> WATCH_STATE = {
          {ZOO_EXPIRED_SESSION_STATE, "ZOO_EXPIRED_SESSION_STATE"},
          {ZOO_AUTH_FAILED_STATE,     "ZOO_AUTH_FAILED_STATE"},
          {ZOO_CONNECTING_STATE,      "ZOO_CONNECTING_STATE"},
          {ZOO_ASSOCIATING_STATE,     "ZOO_ASSOCIATING_STATE"},
          {ZOO_CONNECTED_STATE,       "ZOO_CONNECTED_STATE"}
  };
  try {
    return WATCH_STATE.at(state);
  } catch (const std::out_of_range&) {
    return "Unknown state " + std::to_string(state);
  }
}

ZKWrapper::ZKWrapper(const std::string &host,
                     int &error_code,
                     const std::string &root_path) {
  zh = zookeeper_init(host.c_str(), watcher, DEFAULT_ZK_RECV_TIMEOUT,
                      nullptr, this, 0);
  if (!zh) {
    LOG(ERROR) << "zookeeper_init failed!";
    error_code = ZKWRAPPERDEFAULTERROR;
    initializing = false;
    return;
  }

  for (int j = 0; !connected && j < INITIAL_CONNECTION_TIMEOUT_MILLIS;
       j += INITIAL_CONNECTION_RETRY_INTERVAL_MILLIS) {
    std::this_thread::sleep_for(std::chrono::milliseconds(
            INITIAL_CONNECTION_RETRY_INTERVAL_MILLIS));
  }
  if (!connected) {
    LOG(ERROR) << "[zkwrapper] initial connection timed out";
    error_code = ZKWRAPPERDEFAULTERROR;
    initializing = false;
    return;
  }

  if (!root_path.empty()) {
    bool root_exists = false;
    int i;
    for (i = 0; i < ROOT_CREATION_RETRY_LIMIT && !root_exists; ++i,
            std::this_thread::sleep_for(std::chrono::milliseconds(
                    ROOT_CREATION_RETRY_INTERVAL_MILLIS))) {
      if (!exists(root_path, root_exists, error_code)) {
        LOG(ERROR) << "Failed to check if root directory "
                   << root
                   << " exists "
                   << error_code;
        continue;
      } else {
        break;
      }
    }
    if (i == ROOT_CREATION_RETRY_LIMIT) {
      LOG(ERROR) << "Reached retry limit of "
                 << std::to_string(ROOT_CREATION_RETRY_LIMIT)
                 << " trying to test existence of root " << root_path;
      error_code = ZKWRAPPERDEFAULTERROR;
      initializing = false;
      return;
    }
    if (!root_exists) {
      for (i = 0; i < ROOT_CREATION_RETRY_LIMIT; ++i,
              std::this_thread::sleep_for(std::chrono::milliseconds(
                      ROOT_CREATION_RETRY_INTERVAL_MILLIS))) {
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
      if (i == ROOT_CREATION_RETRY_LIMIT) {
        LOG(ERROR) << "Reached retry limit of "
                   << std::to_string(ROOT_CREATION_RETRY_LIMIT)
                   << " trying to create root " << root_path;
        error_code = ZKWRAPPERDEFAULTERROR;
        initializing = false;
        return;
      }
    }
    root = root_path;
  }

  cache = new lru::Cache<std::string,
      std::shared_ptr<std::vector<unsigned char>>>(512, 10);
  initialized = true;
  initializing = false;
  error_code = ZOK;
  LOG(INFO) << "SUCCESSFULLY INITIALIZED ZKWRAPPER";
}

ZKWrapper::~ZKWrapper() {
    /*
     * TODO(2017) We would do "delete cache;" here, except for the fact that
     * we risk double freeing due to zkWrapper being potentially copied in
     * zk_lock. See the TODO in zk_lock.h
     */
    /*
     * TODO(2017) We would do "close();" here, but ZKWrapper cloning is again
     * causing issues.
     * One way to convince yourself that "close();" is being properly called is
     * to reduce the max number of connections that ZooKeeper accepts and see
     * if all the unit tests still pass. The "maxClientCnxns" configuration
     * option can be changed to achieve this. We had to bump this number since
     * connections are not being properly reclaimed.
     */
}

std::string ZKWrapper::prepend_zk_root(const std::string &path) const {
  if (root.empty()) {
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

std::string ZKWrapper::removeZKRootAndDir(const std::string &prefix,
                                          const std::string &path) const {
  if (path.substr(0, root.size()) == root) {
    auto temp = path.substr(root.size());
    if (temp.substr(0, prefix.size()) == prefix) {
      return temp.substr(prefix.size());
    }
    return temp;
  }
  return path;
}

/* Wrapper Implementation of Zookeeper Functions */

bool ZKWrapper::create(const std::string &path,
             const std::vector<std::uint8_t> &data,
             int &error_code,
             bool ephemeral,
             bool sync) const {
  if (!initialized && !initializing) {
    LOG(ERROR) << "Attempt to create before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  auto real_path = prepend_zk_root(path);
  LOG(INFO) << "creating ZNode at " << real_path;
  int flag = (ephemeral) ? ZOO_EPHEMERAL : 0;
  int rc = zoo_create(zh,
            real_path.c_str(),
            reinterpret_cast<const char *>(data.data()),
            static_cast<int>(data.size()),
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
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  LOG(INFO) << "Starting sequential for " << path;
  int flag = ZOO_SEQUENCE;
  if (ephemeral) {
    flag = flag | ZOO_EPHEMERAL;
  }
  LOG(INFO) << "Attempting to generate new path" << new_path;
  LOG(INFO) << "Creating seq ZNode at " << prepend_zk_root(path);

  auto len = prepend_zk_root(path).size();
  new_path.resize(static_cast<size_t>(MAX_PATH_LEN));
  int rc = zoo_create(zh,
            prepend_zk_root(path).c_str(),
            reinterpret_cast<const char *>(data.data()),
            static_cast<int>(data.size()),
            &ZOO_OPEN_ACL_UNSAFE,
            flag,
            &new_path[0],
            MAX_PATH_LEN);
  error_code = rc;
  if (rc != ZOK) {  // Z_OK is 0, so if we receive anything else fail
    LOG(ERROR) << "Create for " << prepend_zk_root(path) << " failed " << rc;
    print_error(error_code);
    return false;
  }
  LOG(INFO) << "NEW path is " << new_path;
  new_path.resize(len + NUM_SEQUENTIAL_DIGITS);

  if (sync) {
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
  if (!initialized && !initializing) {
    LOG(ERROR) << "Attempt to recursive_create before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  for (std::size_t i = 1; i < path.length(); ++i) {
    if (path[i] == '/') {
      LOG(INFO) << "Creating " << path.substr(0, i);
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
           bool resize) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  auto len = static_cast<int>(data.size());
  struct Stat stat{};
  if (data.empty() && resize) {
    /*
     * The caller didn't bother to allocate a non-trivial buffer, so we do it
     * here to prevent the first get operation from being pointless.
     *
     * The 2016 folks claim that MAX_PAYLOAD is a sane default size.
     * We (2017) have no idea why.
     */
    len = MAX_PAYLOAD;
    data.resize(static_cast<uint64_t>(len));
  }
RETRY_WGET:
  error_code = zoo_wget(zh,
              prepend_zk_root(path).c_str(),
              watch,
              watcherCtx,
              reinterpret_cast<char *>(data.data()),
              &len,
              &stat);
  if (error_code == ZOK) {
    if (len < stat.dataLength && resize) {
      len = stat.dataLength;
      data.resize(static_cast<uint64_t>(stat.dataLength));
      resize = false;
      goto RETRY_WGET;
    }
    if (len < stat.dataLength) {
      /* The data didn't fit in the buffer. */
      LOG(ERROR) << "node data larger than buffer";
      error_code = ZKWRAPPERINSUFFICIENTBUFFER;
      return false;
    } else {
      data.resize(static_cast<uint64_t>(len));
      /*
       * TODO(2017) How does zookeeper distinguish different watches? It seems
       * capable of handling multiple watches on a single znode, but what is
       * the criteria for uniqueness? For now, just assume that the watcher
       * that's supposed to invalidate the cache is being overwritten, so we
       * need to invalidate cache here to prevent stale values from lingering
       * around in the cache forever.
       */
      cache->remove(path);
    }
  } else {
    LOG(ERROR) << "wget on " << path << " failed";
    print_error(error_code);
    return false;
  }
  data.resize(static_cast<std::size_t>(len));
  return true;
}

bool ZKWrapper::get(const std::string &path,
          std::vector<std::uint8_t> &data,
          int &error_code,
          bool resize) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  LOG(INFO) << "Get on path " << path << " in ZkWrapper";

  if (cache->contains(path)) {
    LOG(INFO) << "Found path " << path << " in ZkWrapper cache";
    auto cached_data = cache->get(path);
    data = *cached_data.get();
  } else {
    if (wget(path, data, watcher_znode_data, const_cast<ZKWrapper *>(this),
             error_code, resize)) {
      auto data_copy = std::make_shared<std::vector<uint8_t>>(data);
      cache->insert(path, data_copy);
    } else {
      LOG(ERROR) << "get on " << path << " failed";
      print_error(error_code);
      return false;
    }
  }
  return true;
}

bool ZKWrapper::set(const std::string &path,
          const std::vector<std::uint8_t> &data,
          int &error_code,
          bool sync,
          int version) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  error_code = zoo_set(zh,
             prepend_zk_root(path).c_str(),
             reinterpret_cast<const char *>(data.data()),
             static_cast<int>(data.size()),
             version);
    cache->remove(path);

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
  if (!initialized && !initializing) {
    LOG(ERROR) << "Attempt to exists before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  int rc = zoo_exists(zh, prepend_zk_root(path).c_str(), 0, nullptr);
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
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  struct Stat stat{};
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
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  /* Use -1 for version to delete the node regardless of its version. */
  error_code = zoo_delete(zh, prepend_zk_root(path).c_str(), -1);
  if (error_code != ZOK) {
    LOG(ERROR) << "delete on " << path << " failed";
    print_error(error_code);
    return false;
  }

  if (sync) {
    flush(prepend_zk_root(path));
  }
  return true;
}

bool ZKWrapper::get_info(const std::string &path,
             struct Stat &stat,
             int &error_code) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  std::vector<std::uint8_t> data;
  auto len = MAX_PAYLOAD;
  data.resize(static_cast<std::size_t>(len));

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
  data.resize(static_cast<std::size_t>(len));
  return true;
}

// TODO(2016): Modify
bool ZKWrapper::recursive_delete(const std::string &path,
                 int &error_code) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
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

  for (const auto &child : children) {
    LOG(INFO) << "child is " << child;
    if (child.empty()) {
      continue;
    }
    std::string newPath = znodePath;
    newPath.append("/").append(child);
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
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  struct String_vector stvector{};
  error_code = zoo_get_children(zh, prepend_zk_root(path).c_str(),
                                0, &stvector);
  if (error_code != ZOK) {
    LOG(ERROR) << "get_children on " << path << " failed";
    print_error(error_code);
    return false;
  }

  for (int i = 0; i < stvector.count; i++) {
    children.emplace_back(stvector.data[i]);
  }
  return true;
}

bool ZKWrapper::wget_children(const std::string &path,
                std::vector<std::string> &children,
                watcher_fn watch,
                void *watcherCtx,
                int &error_code) const {
  if (!initialized) {
    LOG(ERROR) << "Attempt to " << __func__ << " before init!";
    error_code = ZKWRAPPERUNINITIALIZED;
    return false;
  }
  struct String_vector stvector{};
  error_code = zoo_wget_children(zh,
                   prepend_zk_root(path).c_str(),
                   watch,
                   watcherCtx,
                   &stvector);
  if (error_code != ZOK) {
    LOG(ERROR) << "wget_children on " << path << " failed";
    print_error(error_code);
    return false;
  }

  for (int i = 0; i < stvector.count; i++) {
    children.emplace_back(stvector.data[i]);
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
  for (const auto &op : ops) {
    trueOps.push_back(*(op->op));
  }
  error_code = zoo_multi(zh, static_cast<int>(ops.size()),
                         &trueOps[0], &results[0]);
  if (error_code != ZOK) {
    LOG(ERROR) << "multiop failed";
    print_error(error_code);
    return false;
  }

  if (sync) {
    flush(root);
  }
  return true;
}

std::vector<uint8_t> ZKWrapper::get_byte_vector(const std::string &string) {
  std::vector<uint8_t> vec(string.begin(), string.end());
  return vec;
}

void ZKWrapper::close() {
    if (connected) {
        zookeeper_close(zh);
        connected = false;
    }
}

bool ZKWrapper::flush(const std::string &full_path, bool synchronous) const {
  // flush is only blocking when the synchronous flag is set
  if (synchronous) {
    // I tried using condition variables,
    // but it ended up failing on occasion, so sadly I resort to polling :(
    auto flag = reinterpret_cast<bool *>(malloc(sizeof(bool)));

    // Lambda to call on function completion
    string_completion_t completion = [](int rc,
                      const char *value,
                      const void *data) {
      auto flag_ptr = const_cast<bool *>(reinterpret_cast<const bool *>(data));
      *flag_ptr = true;
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      free(flag_ptr);
    };

    int rc = zoo_async(zh, full_path.c_str(), completion, flag);

    // Exit early if async failed
    if (rc) {
      LOG(ERROR) << "Flushing " << full_path << " failed";
      print_error(rc);
      return false;
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
    return true;
  } else {
    auto no_op = [&](int rc, const char *value, const void *data) {};
    return zoo_async(zh, full_path.c_str(), no_op, nullptr) == ZOK;
  }
}
