// Copyright 2017 Rice University, COMP 413 2017

#ifndef ZOOKEEPER_INCLUDE_ZK_NN_CLIENT_H_
#define ZOOKEEPER_INCLUDE_ZK_NN_CLIENT_H_

#define MIN_XMITS 'x'
#define MAX_FREE_SPACE 'f'

#include "zk_client_common.h"
#include <google/protobuf/message.h>
#include <queue>
#include <string>
#include <vector>
#include <utility>
#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include "erasurecoding.pb.h"
#include <ConfigReader.h>
#include "util.h"
#include "LRUCache.h"

#define MAX_USERNAME_LEN 256

#define MAX_USERNAME_LEN 256

#define MAX_USERNAME_LEN 256

namespace zkclient {

typedef enum class FileStatus : int {
  UnderConstruction,
  FileComplete,
  UnderDestruction
} FileStatus;

/**
* This is the basic znode to describe a file
*/
typedef struct {
  uint32_t replication;  // the block replication factor.
  bool isEC;  // 1 if EC file. 0 if replication based.
  uint64_t blocksize;
  // 1 for under construction, 0 for complete
  zkclient::FileStatus under_construction;
  int filetype;  // 0 or 1 for dir, 2 for file, 3 for symlinks (not supported)
  std::uint64_t length;
  // https://hadoop.apache.org/docs/r2.4.1/api/org/apache/hadoop/fs/
  // FileSystem.html#setOwner(org.apache.hadoop.fs.Path,
  // java.lang.String,
  // java.lang.String)
  std::uint64_t access_time;
  std::uint64_t modification_time;
  char owner[MAX_USERNAME_LEN];  // the client who created the file
  char group[MAX_USERNAME_LEN];
  char permissions[20][MAX_USERNAME_LEN];  // max 20 users can view the file.
  int perm_length;  // number of slots filled in permissions
  int permission_number;
} FileZNode;

/**
 * Lease info that stores in each lease node.
 */
typedef struct {
  std::string clientName;   // Same as the
                            // clientName passed
                            // from RenewLeaseRequestProto
} LeaseInfo;

/**
 * Client info that stores in each client node
 */
typedef struct {
  uint64_t timestamp;    // std::time()
} ClientInfo;

struct TargetDN {
  char policy;
  std::string dn_id;
  uint64_t free_bytes;    // free space on disk
  uint32_t num_xmits;        // current number of xmits

  TargetDN(std::string id, int bytes, int xmits, char policy) : policy(policy),
                                  dn_id(id),
                                  free_bytes(bytes),
                                  num_xmits(xmits) {
  }

  bool operator<(const struct TargetDN &other) const {
    // If storage policy is 'x' for xmits, choose the min xmits node
    if (policy == MIN_XMITS) {
    if (num_xmits == other.num_xmits) {
      return free_bytes < other.free_bytes;
    }
    return num_xmits > other.num_xmits;

    // Default policy is choose the node with the most free space
    } else {
    if (free_bytes == other.free_bytes) {
      return num_xmits > other.num_xmits;
    }
    return free_bytes < other.free_bytes;
    }
  }
};

using hadoop::hdfs::AddBlockRequestProto;
using hadoop::hdfs::AddBlockResponseProto;
using hadoop::hdfs::AbandonBlockRequestProto;
using hadoop::hdfs::AbandonBlockResponseProto;
using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::GetFileInfoRequestProto;
using hadoop::hdfs::GetFileInfoResponseProto;
using hadoop::hdfs::HdfsFileStatusProto;
using hadoop::hdfs::DeleteRequestProto;
using hadoop::hdfs::DeleteResponseProto;
using hadoop::hdfs::CreateRequestProto;
using hadoop::hdfs::CreateResponseProto;
using hadoop::hdfs::CompleteRequestProto;
using hadoop::hdfs::CompleteResponseProto;
using hadoop::hdfs::RenameRequestProto;
using hadoop::hdfs::RenameResponseProto;
using hadoop::hdfs::MkdirsRequestProto;
using hadoop::hdfs::MkdirsResponseProto;
using hadoop::hdfs::GetListingRequestProto;
using hadoop::hdfs::GetListingResponseProto;
using hadoop::hdfs::LocatedBlocksProto;
using hadoop::hdfs::GetBlockLocationsRequestProto;
using hadoop::hdfs::GetBlockLocationsResponseProto;
using hadoop::hdfs::LocatedBlockProto;
using hadoop::hdfs::GetContentSummaryRequestProto;
using hadoop::hdfs::GetContentSummaryResponseProto;
using hadoop::hdfs::ContentSummaryProto;
using hadoop::hdfs::DatanodeInfoProto;
using hadoop::hdfs::SetOwnerRequestProto;
using hadoop::hdfs::SetOwnerResponseProto;
using hadoop::hdfs::RenameResponseProto;
using hadoop::hdfs::SetPermissionRequestProto;
using hadoop::hdfs::SetPermissionResponseProto;
using hadoop::hdfs::RenewLeaseRequestProto;
using hadoop::hdfs::RenewLeaseResponseProto;
using hadoop::hdfs::RecoverLeaseRequestProto;
using hadoop::hdfs::RecoverLeaseResponseProto;
using hadoop::hdfs::ErasureCodingPolicyProto;
using hadoop::hdfs::ECSchemaProto;
using hadoop::hdfs::GetErasureCodingPoliciesRequestProto;
using hadoop::hdfs::GetErasureCodingPoliciesResponseProto;
using hadoop::hdfs::GetErasureCodingPolicyRequestProto;
using hadoop::hdfs::GetErasureCodingPolicyResponseProto;
using hadoop::hdfs::SetErasureCodingPolicyResponseProto;
using hadoop::hdfs::SetErasureCodingPolicyRequestProto;

/**
 * This is used by ClientNamenodeProtocolImpl to communicate the zookeeper.
 */
class ZkNnClient : public ZkClientCommon {
 public:
  char policy;
  const char* EC_REPLICATION = "REPLICATION";
  const char* DEFAULT_EC_POLICY = "RS-6-3-1024k";  // the default policy.
  uint32_t DEFAULT_EC_CELLCIZE = 1024*1024;  // the default cell size is 64kb.
  uint32_t DEFAULT_EC_ID = 1;
  const uint32_t DEFAULT_DATA_UNITS = 6;
  const uint32_t DEFAULT_PARITY_UNITS = 3;
  const char* DEFAULT_EC_CODEC_NAME = "rs";
  std::string DEFAULT_STORAGE_ID = "1";  // the default storage id.
  ECSchemaProto DEFAULT_EC_SCHEMA;
  ErasureCodingPolicyProto RS_SOLOMON_PROTO;

  enum class ListingResponse {
      Ok,                    // 0
      FileDoesNotExist,      // 1
      FailedChildRetrieval,  // 2
      FileAccessRestricted   // 3
  };

  enum class DeleteResponse {
      Ok,
      FileDoesNotExist,
      FileUnderConstruction,
      FileIsDirectoryMismatch,
      FailedChildRetrieval,
      FailedBlockRetrieval,
      FailedDataNodeRetrieval,
      FailedZookeeperOp,
      FileAccessRestricted
  };

  enum class GetFileInfoResponse {
    Ok,
    FileDoesNotExist,
    FailedReadZnode,
    FileAccessRestricted
  };

  enum class MkdirResponse {
      Ok,
      FailedZnodeCreation
  };

  enum class CreateResponse {
      Ok,
      FileAlreadyExists,
      FailedMkdir,
      FailedCreateZnode
  };

  enum class RenameResponse {
      Ok,
      FileDoesNotExist,
      RenameOpsFailed,
      InvalidType,
      MultiOpFailed,
      FileAccessRestricted
  };

  enum class ErasureCodingPoliciesResponse {
      Ok
  };

  enum class ErasureCodingPolicyResponse {
      Ok,
      FileDoesNotExist
  };

  enum class SetErasureCodingPolicyResponse {
      Ok,
      FileDoesNotExist,
      FailedZookeeperOp
  };

  explicit ZkNnClient(std::string zkIpAndAddress)
                        : ZkClientCommon(zkIpAndAddress),
                          cache(new lru::Cache<std::string,
                          std::shared_ptr<GetListingResponseProto>>(64, 10)) {
    mkdir_helper("/", false);
    populateDefaultECProto();
  }

  /**
   * Use this constructor to build ZkNnClient with a custom ZKWrapper.
   * Which will allow you to set a root
   * directory for all operations on this client
   * @param zk_in shared pointer to a ZKWrapper
   * @param secureMode boolean indicating using secure mode or not
   * @return ZkNnClient
   */
  explicit ZkNnClient(std::shared_ptr<ZKWrapper> zk_in,
                      bool secureMode = false)
                        : ZkClientCommon(zk_in),
                          cache(new lru::Cache<std::string,
                          std::shared_ptr<GetListingResponseProto>>(64, 10)) {
    mkdir_helper("/", false);
    isSecureMode = secureMode;
    populateDefaultECProto();
  }
  void register_watches();
  /**
   * Returns the current timestamp in milliseconds
   */
  uint64_t current_time_ms();
  /**
   * Returns the latest timestamp by the client
   */
  uint64_t get_client_lease_timestamp(std::string client_name);

  /**
   * These methods will correspond to proto calls that the client namenode protocol handles
   */
  void renew_lease(RenewLeaseRequestProto &req,
                   RenewLeaseResponseProto &res);
  void recover_lease(RecoverLeaseRequestProto &req,
                     RecoverLeaseResponseProto &res);
  /**
   * Get info of the file.
   * @param req GetFileInfoRequestProto
   * @param res GetFileInfoResponseProto
   * @return GetFileInfoResponse
   */
  GetFileInfoResponse get_info(GetFileInfoRequestProto &req,
                               GetFileInfoResponseProto &res,
                               std::string client_name = "default");
<<<<<<< HEAD

  /**
   * Create the file.
   * @param req CreateRequestProto
   * @param res CreateResponseProto
   * @return CreateResponse
   */
=======
>>>>>>> merging permissions changes
  CreateResponse create_file(CreateRequestProto &request,
                                         CreateResponseProto &response);

  /**
   * Get locations of blocks.
   * @param req GetBlockLocationsRequestProto
   * @param res GetBlockLocationsResponseProto
   * @param client_name client's name as string
   * @return
   */
  void get_block_locations(GetBlockLocationsRequestProto &req,
                           GetBlockLocationsResponseProto &res,
                           std::string client_name = "default");
  /**
   * Make a directory.
   * @param req MkdirsRequestProto
   * @param res MkdirsResponseProto
   * @return MkdirResponse
   */
  MkdirResponse mkdir(MkdirsRequestProto &req,
                      MkdirsResponseProto &res);

  /**
   * Destroy the file.
   * @param req DeleteRequestProto
   * @param res DeleteResponseProto
   * @param client_name client's name as string
   * @return MkdirResponse
   */
  DeleteResponse destroy(DeleteRequestProto &req,
                         DeleteResponseProto &res,
                         std::string client_name = "default");

  /**
   * Complete the file.
   * @param req CompleteRequestProto
   * @param res CompleteResponseProto
   * @param client_name client's name as string
   * @return 
   */
  void complete(CompleteRequestProto &req,
                CompleteResponseProto &res,
                std::string client_name = "default");

  /**
   * Rename the file.
   * @param req RenameRequestProto
   * @param res RenameResponseProto
   * @param client_name client's name as string
   * @return RenameResponse
   */
  RenameResponse rename(RenameRequestProto &req,
                        RenameResponseProto &res,
                        std::string client_name = "default");

  /**
   * Get listing of the file.
   * @param req GetListingRequestProto
   * @param res GetListingResponseProto
   * @param client_name client's name as string
   * @return ListingResponse
   */
  ListingResponse get_listing(GetListingRequestProto &req,
                              GetListingResponseProto &res,
                              std::string client_name = "default");
  /**
   * Get content of the file.
   * @param req GetContentSummaryRequestProto
   * @param res GetContentSummaryResponseProto
   * @param client_name client's name as string
   * @return
   */
  void get_content(GetContentSummaryRequestProto &req,
                   GetContentSummaryResponseProto &res,
                   std::string client_name = "default");

  /**
   * Sets file info content.
   */
  void set_file_info_content(ContentSummaryProto *status,
                             const std::string &path,
                             FileZNode &znode_data);

  void set_node_policy(char policy);

  char get_node_policy();

  /**
   * Returns the erasure coding policies loaded in Namenode, excluding REPLICATION
   * policy.
   */
  ErasureCodingPoliciesResponse get_erasure_coding_policies(
      GetErasureCodingPoliciesRequestProto &req,
      GetErasureCodingPoliciesResponseProto &res);

  /**
   * Returns the erasure coding policy of a file or a directory specified by the path.
   */
  ErasureCodingPolicyResponse get_erasure_coding_policy_of_path(
      GetErasureCodingPolicyRequestProto &req,
      GetErasureCodingPolicyResponseProto &res);

  /**
   * Sets the erasure coding policy of a path by the given erasure coding policy name.
   */
  SetErasureCodingPolicyResponse set_erasure_coding_policy_of_path(
      SetErasureCodingPolicyRequestProto &req,
      SetErasureCodingPolicyResponseProto &res);

//  bool modifyAclEntries(ModifyAclEntriesRequestProto req,
//                        ModifyAclEntriesResponseProto res);
  /**
   * Sets the permission of the file.
   * @param req SetPermissionRequestProto
   * @param res SetPermissionResponseProto
   * @return boolean indicating whether operation succeeded or not
   */
  bool set_permission(SetPermissionRequestProto &req,
                      SetPermissionResponseProto &res);

  /**
   * Sets the owner of the file.
   * @param req SetOwnerRequestProto
   * @param res SetOwnerResponseProto
   * @param client_name client's name as string
   * @return boolean indicating whether operation succeeded or not
   */
  bool set_owner(SetOwnerRequestProto &req,
                 SetOwnerResponseProto &res,
                 std::string client_name = "default");
  /**
   * Adds a block by making appropriate namespace changes and returns information about
   * the set of DataNodes that the block data should be hosted by.
   * @param req AddBlockRequestProto
   * @param res AddBlockResponseProto
   * @param client_name client's name as string
   * @return boolean indicating whether operation succeeded or not
   */
  bool add_block(AddBlockRequestProto &req,
                 AddBlockResponseProto &res,
                 std::string client_name = "default");
  /*
   * Sets the owner of the file.
   * @param req SetOwnerRequestProto
   * @param res SetOwnerResponseProto
   * @return boolean indicating whether operation succeeded or not
   */
  bool set_owner(SetOwnerRequestProto &req, SetOwnerResponseProto &res);

  /**
   * A helper method that achieves the above add_block method.
   * Does
   * 1) Creates namespace changes to the given file.
   * 2) Generates a block id. The id is generated randomly for replication
   * blocks and based on the hierarchical naming scheme for EC blocks.
   * 3) Finds a set of data nodes on which to allocate the new block.
   * In the case of replication, the set of DataNodes has primary / secondary
   * replicas of the block.
   * In the case of EC, each DataNode hosts a block group.
   */
  bool add_block(const std::string &fileName,
                 u_int64_t &block_id,
                 std::vector<std::string> &dataNodes,
                 uint32_t replication_factor);

  /**
   * Makes metadata changes required to add a new block group.
   * This helper method is called for an EC file.
   * @param filePath the file specified by its path.
   * @param block_group_id the block group id to be generated.
   * @param dataNodes the set of data nodes on which to allocate each storage block.
   * @param blockIndices the set of block indices within a block gorup.
   * @param total_num_storage_blocks the number of data + parity storage blocks.
   * @return true if successful. false otherwise.
   */
  bool add_block_group(const std::string &filePath,
                       u_int64_t &block_group_id,
                       std::vector<std::string> &dataNodes,
                       std::vector<char> &blockIndices,
                       uint32_t total_num_storage_blocks);

  /**
   * Given a file, figure out the number of storage blocks to have within a block group.
   * @param fileName the file name.
   * @param block_group_id the block group id.
   * @return the number of storage blocks to have within a block group.
   */
  uint32_t get_total_num_storage_blocks(
          const std::string &fileName,
          u_int64_t &block_group_id);

  /**
   * Given the block group id and index in the block group, returns the hierarchical block id.
   * @param block_group_id the id of a block group this storage block belongs to.
   * @param index_within_group the index within the block group.
   * @return the storage block id.
   */
  u_int64_t generate_storage_block_id(
          uint64_t block_group_id,
          uint64_t index_within_group);
  /**
   * Generates the block group id.
   * @return an 64 bit unsigned integer that has bit 2 ~ bit 48 arbitrarily filled.
   */
  u_int64_t generate_block_group_id();

  /**
   * Gets the block group id from the storage block id.
   * i.e. bit 2 ~ bit 48.
   * @param storage_block_id the given storage block id.
   * @return the block group id.
   */
  u_int64_t get_block_group_id(u_int64_t storage_block_id);

  /**
   * Gets the index within the block group.
   * @param storage_block_id the given storage block id.
   * @return the index within the block group.
   */
  u_int64_t get_index_within_block_group(u_int64_t storage_block_id);

  /**
   * Abandons the block - basically reverses all of add block's multiops
   * @param req AbandonBlockRequestProtoProto
   * @param res AbandonBlockResponseProto
   * @param client_name client's name as string
   * @return boolean indicating whether operation succeeded or not
   */
  bool abandon_block(AbandonBlockRequestProto &req,
                     AbandonBlockResponseProto &res,
                     std::string client_name = "default");

  bool previousBlockComplete(uint64_t prev_id);
  /**
   * Information that the protocol might need to respond to individual rpc calls
   */
  bool file_exists(const std::string &path);

  /**
   * Reads the blocksize of the given block_id from zookeeper and returns
   */
  bool get_block_size(const u_int64_t &block_id, uint64_t &blocksize);

  // this is public because we have not member functions in this file
  static const std::string CLASS_NAME;

  bool find_live_datanodes(const uint64_t blockId, int error_code,
                           std::vector<std::string> &live_data_nodes);

  bool find_datanode_for_block(std::vector<std::string> &datanodes,
                                        std::vector<std::string> &excluded_dns,
                                         const u_int64_t blockId,
                                         uint32_t replication_factor,
                                         uint64_t blocksize);

  bool find_all_datanodes_with_block(const uint64_t &block_uuid,
                                     std::vector<std::string> &rdatanodes,
                                     int &error_code);

  bool rename_ops_for_file(const std::string &src, const std::string &dst,
                           std::vector<std::shared_ptr<ZooOp>> &ops);
  bool rename_ops_for_dir(const std::string &src, const std::string &dst,
                          std::vector<std::shared_ptr<ZooOp>> &ops);

  /**
   * Look through the wait_for_acks work queue to check the replication
   * status of the pending blocks and take an appropriate action to
   * ensure that the blocks get replicated
   */
  bool check_acks();

  // get locations given src, offset, and length
  void get_block_locations(const std::string &src,
                           google::protobuf::uint64 offset,
                           google::protobuf::uint64 length,
                           LocatedBlocksProto *blocks,
                           std::string client_name = "default");

  /**
   * Read a znode corresponding to a file into znode_data
   */
  void read_file_znode(FileZNode &znode_data, const std::string &path);

  bool cache_contains(const std::string &path);

  int cache_size();

 private:
  /**
   * Given a vector of DN IDs, sorts them from fewest to most number of transmits
   */
  bool sort_by_xmits(const std::vector<std::string> &unsorted_dn_ids,
                     std::vector<std::string> &sorted_dn_ids);

  /**
   * Set the file status proto with information from the znode struct and the path
   *
   */
  void set_file_info(HdfsFileStatusProto *fs,
                     const std::string &path,
                     FileZNode &node);
  /**
   * Given the client name, get the client path.
   */
  std::string ClientZookeeperPath(const std::string & clientname);
  /**
    * Given the filesystem path, get the full zookeeper path for leases
    */
  std::string LeaseZookeeperPath(const std::string & hadoopPath);

  /**
   * Given the filesystem path, get the full zookeeper path for the blocks
   * where the data is located
   */
  std::string ZookeeperBlocksPath(const std::string &hadoopPath);

   /**
   * Given the filesystem path, get the full zookeeper path for the dir
   * where the file metadata is written
   */
  std::string ZookeeperFilePath(const std::string &hadoopPath);

  /**
   * Use to read values from config
   */
  config_reader::ConfigReader config;

  /**
   * Crate a znode corresponding to a file of "filetype", with path "path", with
   * znode data contained in "znode_data"
   */
  bool create_file_znode(const std::string &path, FileZNode *znode_data);

  /**
   * Set the default information in a directory znode struct
   */
  void set_mkdir_znode(FileZNode *znode_data);
  /**
   * Create the directories at path. If create_parent is true, then we create
   * all the parent directories which are not in zookeeper already. Return false
   * if the creation did not work, true otherwise
   */
  MkdirResponse mkdir_helper(const std::string &path, bool create_parent);

  /**
   * Serialize a znode struct representation to a byte array to feed into zookeeper
   */
  void file_znode_struct_to_vec(FileZNode *znode_data,
                                std::vector<std::uint8_t> &data);
  template <class T>
  void znode_data_to_vec(T *znode_data, std::vector<std::uint8_t> &data);
  template <class T>
  void read_znode_data(T &znode_data, const std::string &path);

  /**
   * Try to delete a node and log error if we couldnt and set response to false
   */
  void delete_node_wrapper(std::string &path, DeleteResponseProto &response);

  DeleteResponse destroy_helper(const std::string &path,
                      std::vector<std::shared_ptr<ZooOp>> &ops);
  /**
   * Give a vector of block IDs, executes a multiop which creates items in
   * the ec_recover queue and children nodes indicating which datanote to
   * read from for those items.
   */
  bool recover_ec_blocks(const std::vector<std::string> &to_ec_recover,
                        int error_code);


  /**
   * Give a vector of block IDs, executes a multiop which creates items in
   * the replicate queue and children nodes indicating which datanote to
   * read from for those items.
   */
  bool replicate_blocks(const std::vector<std::string> &to_replicate,
                        int error_code);

  /**
   * Calculates the approximate number of milliseconds that have elapsed
   * since the znode at the given path was created.
   */
  int ms_since_creation(std::string &path);

  /**
   * Modifies the LocatedBlockProto with the proper block information
   */
  // bool updateLocatedBlockProto(LocatedBlockProto* location,
  //                              uint64_t block_id);

  /**
   * Modifies the DatanodeInfoProto with information about the specified datanode.
   * Datanode is represented as a string as most calls to this function follow a
   * getChild() request. Returns true on success
   */
  bool buildDatanodeInfoProto(DatanodeInfoProto *dn_info,
                              const std::string &data_node);

  /**
   * Builds an empty token. Returns true on success.
   */
  bool buildTokenProto(hadoop::common::TokenProto *token);

  /**
   * Build an extended block proto. Returns true on success
   */
  bool buildExtendedBlockProto(ExtendedBlockProto *eb,
                               const std::uint64_t &block_id,
                               const uint64_t &block_size);

  /**
   * Watches /health for new datanodes, attaches watchers to new datanodes' heartbeats.
   */
  static void watcher_health(zhandle_t *zzh, int type, int state,
                             const char *path, void *watcherCtx);

  /**
   * Watches datanode heartbeats.
   */
  static void watcher_health_child(zhandle_t *zzh, int type, int state,
                                   const char *path, void *watcherCtx);

  static void watcher_listing(zhandle_t *zzh, int type, int state,
                              const char *path, void *watcherCtx);

  /**
   * Returns whether the input client is still alive.
   */
  bool lease_expired(std::string lease_holder_client);

  /**
  * Informs Zookeeper when the DataNode has deleted a block.
  * @param uuid The UUID of the block deleted by the DataNode.
  * @param size_bytes The number of bytes in the block
  * @return True on success, false on error.
  */
  bool blockDeleted(uint64_t uuid, std::string id);

  /**
   * Populates DEFAULT_EC_SCHEMA and RS_SOLOMON_PROTO fields.
   */
  void populateDefaultECProto();

  /**
   * Check access to a file
   * @param username client's username
   * @param znode_data reference to the fileZNode being accessed
   * @return boolean indicating whether the given username has access
   *                 to a znode or not
   */
  bool checkAccess(std::string username, FileZNode &znode_data);

  const int UNDER_CONSTRUCTION = 1;
  const int FILE_COMPLETE = 0;
  const int UNDER_DESTRUCTION = 2;

  const int IS_FILE = 2;
  const int IS_DIR = 1;
  // TODO(2016): Should eventually be read from a conf file
  // in millisecons, 10 minute timeout when waiting for
  // replication acknowledgements
  const int ACK_TIMEOUT = 600000;

  // Boolean indicating whether zk_nn is in secure mode
  bool isSecureMode = false;
  const uint64_t EXPIRATION_TIME =
    2 * 60 * 60 * 1000;  // 2 hours in milliseconds.

  lru::Cache<std::string, std::shared_ptr<GetListingResponseProto>> *cache;
};

}  // namespace zkclient

#endif  // ZOOKEEPER_INCLUDE_ZK_NN_CLIENT_H_
