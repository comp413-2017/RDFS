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
//  std::string ecPolicyName;  // the specified EC policy name.
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
  char owner[256];  // the client who created the file
  char group[256];
} FileZNode;

struct TargetDN {
  char policy;
  std::string dn_id;
  uint64_t free_bytes;    // free space on disk
  uint32_t num_xmits;        // current number of xmits

  TargetDN(std::string id, int bytes, int xmits, char policy) : dn_id(id),
                                                   free_bytes(bytes),
                                                   policy(policy),
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
using hadoop::hdfs::ErasureCodingPolicyProto;
using hadoop::hdfs::ECSchemaProto;
using hadoop::hdfs::GetErasureCodingPoliciesRequestProto;
using hadoop::hdfs::GetErasureCodingPoliciesResponseProto;


/**
 * This is used by ClientNamenodeProtocolImpl to communicate the zookeeper.
 */
class ZkNnClient : public ZkClientCommon {
 public:
  char policy;

  const char* EC_REPLICATION = "EC_REPLICATION";
  const char* DEFAULT_EC_POLICY = EC_REPLICATION;  // the default policy.
  uint32_t DEFAULT_EC_CELLCIZE = 1024*1024;  // the default cell size is 64kb.
  uint32_t DEFAULT_EC_ID = 1;
  const uint32_t DEFAULT_DATA_UNITS = 6;
  const uint32_t DEFAULT_PARITY_UNITS = 3;
  const char* DEFAULT_EC_CODEC_NAME = "rs";
  const char* DEFAULT_EC_POLICY_NAME = "RS-6-3-1024k";
  ECSchemaProto DEFAULT_EC_SCHEMA;
  ErasureCodingPolicyProto RS_SOLOMON_PROTO;


enum class ListingResponse {
      Ok,                   // 0
      FileDoesNotExist,     // 1
      FailedChildRetrieval  // 2
  };

  enum class DeleteResponse {
      Ok,
      FileDoesNotExist,
      FileUnderConstruction,
      FileIsDirectoryMismatch,
      FailedChildRetrieval,
      FailedBlockRetrieval,
      FailedDataNodeRetrieval,
      FailedZookeeperOp
  };

  enum class GetFileInfoResponse {
    Ok,
    FileDoesNotExist,
    FailedReadZnode
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
      MultiOpFailed
  };
  enum class ErasureCodingPoliciesResponse {
      Ok
  };

  explicit ZkNnClient(std::string zkIpAndAddress);

  /**
   * Use this constructor to build ZkNnClient with a custom ZKWrapper.
   * Which will allow you to set a root
   * directory for all operations on this client
   * @param zk_in shared pointer to a ZKWrapper
   * @return ZkNnClient
   */
  explicit ZkNnClient(std::shared_ptr<ZKWrapper> zk_in);
  void register_watches();

  /**
   * These methods will correspond to proto calls that the client namenode protocol handles
   */

  GetFileInfoResponse get_info(GetFileInfoRequestProto &req,
                               GetFileInfoResponseProto &res);
  ZkNnClient::CreateResponse  create_file(CreateRequestProto &request,
                                          CreateResponseProto &response);
  void get_block_locations(GetBlockLocationsRequestProto &req,
                           GetBlockLocationsResponseProto &res);
  DeleteResponse destroy(DeleteRequestProto &req, DeleteResponseProto &res);
  MkdirResponse mkdir(MkdirsRequestProto &req, MkdirsResponseProto &res);
  void complete(CompleteRequestProto &req, CompleteResponseProto &res);
  RenameResponse rename(RenameRequestProto &req, RenameResponseProto &res);
  ListingResponse get_listing(GetListingRequestProto &req,
                              GetListingResponseProto &res);
  void get_content(GetContentSummaryRequestProto &req,
                   GetContentSummaryResponseProto &res);

  void set_file_info_content(ContentSummaryProto *status,
                             const std::string &path, FileZNode &znode_data);

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
   * Adds a block by making appropriate namespace changes and returns information about
   * the set of DataNodes that the block data should be hosted by.
   */
  bool add_block(AddBlockRequestProto &req, AddBlockResponseProto &res);

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
   * @param fileName the file name.
   * @param block_group_id the block group id to be generated.
   * @param dataNodes the set of data nodes on which to allocate each storage block.
   * @param storageBlockIDs the set of storage block ids to be generated.
   * @param total_num_storage_blocks the number of data + parity storage blocks.
   * @return true if successful. false otherwise.
   */
  bool add_block_group(const std::string &fileName,
                       u_int64_t &block_group_id,
                       std::vector<std::string> &dataNodes,
                       std::vector<u_int64_t> &storageBlockIDs,
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
   */
  bool abandon_block(AbandonBlockRequestProto &req,
                     AbandonBlockResponseProto &res);

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

  bool find_datanode_for_block(std::vector<std::string> &datanodes,
                               const std::uint64_t blockId,
                               uint32_t replication_factor,
                               bool newBlock,
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
                           LocatedBlocksProto *blocks);

  /**
   * Read a znode corresponding to a file into znode_data
   */
  void read_file_znode(FileZNode &znode_data, const std::string &path);

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

  /**
   * Try to delete a node and log error if we couldnt and set response to false
   */
  void delete_node_wrapper(std::string &path, DeleteResponseProto &response);

  DeleteResponse destroy_helper(const std::string &path,
                      std::vector<std::shared_ptr<ZooOp>> &ops);

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

  /**
   * Returns the current timestamp in milliseconds
   */
  uint64_t current_time_ms();

  /**
  * Informs Zookeeper when the DataNode has deleted a block.
  * @param uuid The UUID of the block deleted by the DataNode.
  * @param size_bytes The number of bytes in the block
  * @return True on success, false on error.
  */
  bool blockDeleted(uint64_t uuid, std::string id);


  /**
   * Determines what the redundancy form of a file specified by @path should be and returns the corresponding value.
   *
   * If the ecPolicyString is empty, it uses the default redundancy form, which is zkclient::DEFAULT_EC_POLICY.
   * TODO: we may choose to implement what HDFS does, which is to inherit the redundancy form of its parent directory.
   * TODO: the path parameter is need in that case.
   * If not empty, simply returns the given ecPolicyString.
   * @param ecPolicyString the ecPolicyname part of CreateRequestProto.
   * @param path the file path.
   * @return zkclient::REPLICATION or zkclinet::EC.
   */
  std::string determineRedundancyForm(
          const std::string &ecPolicyString,
          const std::string &path);

  /**
   * Populates DEFAULT_EC_SCHEMA and RS_SOLOMON_PROTO fields.
   */
  void populateDefaultECProto();

  const int UNDER_CONSTRUCTION = 1;
  const int FILE_COMPLETE = 0;
  const int UNDER_DESTRUCTION = 2;
  const int IS_FILE = 2;
  const int IS_DIR = 1;
  // TODO(2016): Should eventually be read from a conf file
  // in millisecons, 10 minute timeout when waiting for
  // replication acknowledgements
  const int ACK_TIMEOUT = 600000;
};

}  // namespace zkclient

#endif  // ZOOKEEPER_INCLUDE_ZK_NN_CLIENT_H_
