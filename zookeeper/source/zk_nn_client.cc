#ifndef RDFS_ZKNNCLIENT_CC
#define RDFS_ZKNNCLIENT_CC

#include "../include/zk_nn_client.h"
#include "zkwrapper.h"
#include <iostream>
#include <sstream>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include "zk_lock.h"

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include <google/protobuf/message.h>
#include <ConfigReader.h>
#include <easylogging++.h>

#include <boost/algorithm/string.hpp>
#include "zk_dn_client.h"

namespace zkclient{

    using namespace hadoop::hdfs;

    const std::string ZkNnClient::CLASS_NAME = ": **ZkNnClient** : ";

    ZkNnClient::ZkNnClient(std::string zkIpAndAddress) : ZkClientCommon(zkIpAndAddress) {
        mkdir_helper( "/", false);
    }

    ZkNnClient::ZkNnClient(std::shared_ptr <ZKWrapper> zk_in) : ZkClientCommon(zk_in) {
        mkdir_helper( "/", false);
    }

    /*
     * A simple print function that will be triggered when
     * namenode loses a heartbeat
     */
    void notify_delete() {
        printf("No heartbeat, no childs to retrieve\n");
    }

    void ZkNnClient::register_watches() {

        int error_code;
        std::vector <std::string> children = std::vector <std::string>();

        /* Place a watch on the health subtree */


        if (!(zk->wget_children(HEALTH, children, zk->watcher_health_factory(ZkClientCommon::HEALTH_BACKSLASH), nullptr, error_code))) {
            // TODO: Handle error
            LOG(ERROR) << CLASS_NAME << "[In register_watchers], wget failed " << error_code;
        }

        for (int i = 0; i < children.size(); i++) {
            LOG(INFO) << CLASS_NAME << "[In register_watches] Attaching child to " << children[i] << ", ";
            std::vector <std::string> ephem = std::vector <std::string>();
            if(!(zk->wget_children(HEALTH_BACKSLASH + children[i], ephem, zk->watcher_health_child, nullptr, error_code))) {
                // TODO: Handle error
                LOG(ERROR) << CLASS_NAME << "[In register_watchers], wget failed " << error_code;
            }
            /*
               if (ephem.size() > 0) {
               LOG(INFO) << CLASS_NAME << "Found ephem " << ephem[0];
               } else {
               LOG(INFO) << CLASS_NAME << "No ephem found for " << children[i];
               }
             */
        }
    }

    bool ZkNnClient::file_exists(const std::string& path) {
        bool exists;
        int error_code;
        if (zk->exists(ZookeeperPath(path), exists, error_code)) {
            return exists;
        } else {
            // TODO: Handle error
        }
    }



    // --------------------------- PROTOCOL CALLS ---------------------------------------

    void ZkNnClient::read_file_znode(FileZNode& znode_data, const std::string& path) {
        int error_code;
        std::vector<std::uint8_t> data(sizeof(znode_data));
        if (!zk->get(ZookeeperPath(path), data, error_code)) {
            LOG(ERROR) << "We could not read the file znode at " << path;
            return; // don't bother reading the data
        }
        std::uint8_t *buffer = &data[0];
        memcpy(&znode_data, buffer, sizeof(znode_data));
    }

    void ZkNnClient::file_znode_struct_to_vec(FileZNode* znode_data, std::vector<std::uint8_t>& data) {
        memcpy(&data[0], znode_data, sizeof(*znode_data));
    }

    void ZkNnClient::add_block(AddBlockRequestProto& req, AddBlockResponseProto& res) {

        // Build a new block for the response
        auto block = res.mutable_block();

        // TODO: Make sure we are appending / not appending ZKPath at every step....
        const std::string file_path = req.src();

        LOG(INFO) << CLASS_NAME << "Attempting to add block to existing file " << file_path;

        FileZNode znode_data;
        if (!file_exists(file_path)) {
            LOG(ERROR) << CLASS_NAME << "Requested file " << file_path << " does not exist";
            return;
        }
        read_file_znode(znode_data, file_path);
        if (znode_data.filetype != IS_FILE) { // Assert that the znode we want to modify is a file
            LOG(ERROR) << CLASS_NAME << "Requested file " << file_path << " is not a file";
            return;
        }

        uint32_t replication_factor = znode_data.replication;
        uint64_t block_size = znode_data.blocksize;
        assert(block_size > 0);

        std::uint64_t block_id;
        auto data_nodes = std::vector<std::string>();

        add_block(file_path, block_id, data_nodes, replication_factor);

        block->set_offset(0); // TODO: Set this
        block->set_corrupt(false);

        buildExtendedBlockProto(block->mutable_b(), block_id, block_size);

        for (auto data_node : data_nodes) {
            buildDatanodeInfoProto(block->add_locs(), data_node);
        }

        // Construct security token.
        buildTokenProto(block->mutable_blocktoken());
    }

    void ZkNnClient::get_info(GetFileInfoRequestProto& req, GetFileInfoResponseProto& res) {
        const std::string& path = req.src();

        if (file_exists(path)) {
            LOG(INFO) << CLASS_NAME << "File exists";
            // read the node into the file node struct
            FileZNode znode_data;
            read_file_znode(znode_data, path);

            // set the file status in the get file info response res
            HdfsFileStatusProto* status = res.mutable_fs();

            set_file_info(status, path, znode_data);
            LOG(INFO) << CLASS_NAME << "Got info for file ";
            return;
        }
        LOG(INFO) << CLASS_NAME << "No file to get info for";
    }

    /**
     * Create a node in zookeeper corresponding to a file
     */
    int ZkNnClient::create_file_znode(const std::string& path, FileZNode* znode_data) {
        int error_code;
        if (!file_exists(path)) {
            LOG(INFO)<< "Creating file znode at " << path;
            {
                LOG(INFO) << CLASS_NAME << znode_data->replication;
                LOG(INFO) << CLASS_NAME << znode_data->owner;
                LOG(INFO) << CLASS_NAME << "size of znode is " << sizeof(*znode_data);
            }
            // serialize struct to byte vector
            std::vector<std::uint8_t> data(sizeof(*znode_data));
            file_znode_struct_to_vec(znode_data, data);
            // crate the node in zookeeper
            if (!zk->create(ZookeeperPath(path), data, error_code)) {
                LOG(ERROR) << CLASS_NAME << "Create failed" << error_code;
                return 0;
                // TODO : handle error
            }
            return 1;
        }
        return 0;
    }

    bool ZkNnClient::destroy_helper(const std::string& path, std::vector<std::shared_ptr<ZooOp>>& ops){
        LOG(INFO) << "Destroying " << path;
        if (!file_exists(path)){
            LOG(ERROR) << path << " does not exist";
            return false;
        }
        int error_code;
        FileZNode znode_data;
        read_file_znode(znode_data, path);
        std::vector<std::string> children;
        if (!zk->get_children(ZookeeperPath(path), children, error_code)) {
            LOG(FATAL) << "Failed to get children for " << path;
            return false;
        }
        if (znode_data.filetype == IS_DIR){
            for (auto& child : children){
                auto child_path = util::concat_path(path, child);
                if (!destroy_helper(child_path, ops)){
                    return false;
                }
            }
        }
        else if (znode_data.filetype == IS_FILE){
            if (znode_data.under_construction == UNDER_CONSTRUCTION){
                LOG(ERROR) << path << " is under construction, so it cannot be deleted.";
                return false;
            }
            for (auto& child : children){
                auto child_path = util::concat_path(path, child);
                child_path = ZookeeperPath(child_path);
                ops.push_back(zk->build_delete_op(child_path));
                std::vector<std::uint8_t> block_vec;
                std::uint64_t block;
                if (!zk->get(child_path, block_vec, error_code, sizeof(block))){
                    return false;
                }
                block = *reinterpret_cast<std::uint64_t *>(block_vec.data());
                std::vector<std::string> datanodes;

                if (!zk->get_children(util::concat_path(BLOCK_LOCATIONS, std::to_string(block)), datanodes, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed getting datanode locations for block: " << block << " with error: " << error_code;
                    return false;
                }
                // push delete commands onto ops
                for (auto& dn : datanodes){
                    auto delete_queue = util::concat_path(DELETE_QUEUES, dn);
                    auto delete_item = util::concat_path(delete_queue, "block-");
                    // TODO: saving this until DataNode team creates delete work queues
                    //ops.push_back(zk->build_create_op(delete_item, block_vec, ZOO_SEQUENCE));
                }
            }
        }
        ops.push_back(zk->build_delete_op(ZookeeperPath(path)));
        return true;
    }


    /**
     * Go down directories recursively. If a child is a file, then put its deletion on a queue.
     * Files delete themselves, but directories are deleted by their parent (so root can't be deleted)
     */
    void ZkNnClient::destroy(DeleteRequestProto& request, DeleteResponseProto& response) {

        int error_code;
        const std::string& path = request.src();
        bool recursive = request.recursive();
        response.set_result(true);
        if (!file_exists(path)) {
            LOG(ERROR) << CLASS_NAME << "Cannot delete " << path << " because it doesn't exist.";
            response.set_result(false);
            return;
        }
        FileZNode znode_data;
        read_file_znode(znode_data, path);

        if (znode_data.filetype == IS_FILE && znode_data.under_construction == UNDER_CONSTRUCTION){
            LOG(ERROR) << CLASS_NAME << "Cannot delete " << path << " because it is under construction.";
            response.set_result(false);
            return;
        }
        if (znode_data.filetype == IS_DIR && !recursive){
            LOG(ERROR) << CLASS_NAME << "Cannot delete " << path << " because it is a directory. Use recursive = true.";
            response.set_result(false);
            return;
        }
        std::vector<std::shared_ptr<ZooOp>> ops;
        if (!destroy_helper(path, ops)){
            response.set_result(false);
            return;
        }
        std::vector<zoo_op_result> results;
        if (!zk->execute_multi(ops, results, error_code)) {
            LOG(ERROR) << CLASS_NAME << "Failed to execute multi op to delete " << path;
            response.set_result(false);
        }
    }

    /**
     * Create a file in zookeeper
     */
    int ZkNnClient::create_file(CreateRequestProto& request, CreateResponseProto& response) {
        LOG(INFO) << CLASS_NAME << "Gonna try and create a file on zookeeper";
        const std::string& path = request.src();
        const std::string& owner = request.clientname();
        bool create_parent = request.createparent();
        std::uint64_t blocksize = request.blocksize();
        std::uint32_t replication = request.replication();
        std::uint32_t createflag = request.createflag();

        if (file_exists(path)) {
            // TODO solve this issue of overwriting files
            LOG(ERROR) << CLASS_NAME << "File already exists";
            return 0;
        }

        // If we need to create directories, do so
        if (create_parent) {
            std::string directory_paths = "";
            std::vector<std::string> split_path;
            boost::split(split_path, path, boost::is_any_of("/"));
            LOG(INFO) << CLASS_NAME << split_path.size();
            for (int i = 0; i < split_path.size() - 1; i++) {
                directory_paths += split_path[i];
            }
            // try and make all the parents
            if (!mkdir_helper(directory_paths, true))
                return 0;
        }

        // Now create the actual file which will hold blocks
        FileZNode znode_data;
        znode_data.length = 0;
        znode_data.under_construction = UNDER_CONSTRUCTION;
        uint64_t mslong = current_time_ms();
        znode_data.access_time = mslong;
        znode_data.modification_time = mslong;
        strcpy(znode_data.owner, owner.c_str());
        strcpy(znode_data.group, owner.c_str());
        znode_data.replication = replication;
        znode_data.blocksize = blocksize;
        znode_data.filetype = IS_FILE;

        // if we failed, then do not set any status
        if (!create_file_znode(path, &znode_data))
            return 0;

        HdfsFileStatusProto* status = response.mutable_fs();
        set_file_info(status, path, znode_data);

        return 1;
    }

    void ZkNnClient::complete(CompleteRequestProto& req, CompleteResponseProto& res) {

        // TODO: Completion makes a few guarantees that we should handle

        int error_code;
        // change the under construction bit
        const std::string& src = req.src();
        FileZNode znode_data;
        read_file_znode(znode_data, src);
        znode_data.under_construction = FILE_COMPLETE;
        std::vector<std::uint8_t> data(sizeof(znode_data));
        file_znode_struct_to_vec(&znode_data, data);
        if (!zk->set(ZookeeperPath(src), data, error_code)) {
            LOG(ERROR) << CLASS_NAME << " complete could not change the construction bit";
            res.set_result(false);
        }
        res.set_result(true);
    }

    /**
     * Rename a file in the zookeeper filesystem
     */
    void ZkNnClient::rename(RenameRequestProto& req, RenameResponseProto& res) {
        if(!rename_file(req.src(), req.dst())) {
            res.set_result(false);
        }
        res.set_result(true);
    }

    // ------- make a directory

    /**
     * Set the default information for a directory znode
     */
    void ZkNnClient::set_mkdir_znode(FileZNode* znode_data) {
        znode_data->length = 0;
        uint64_t mslong = current_time_ms();
        znode_data->access_time = mslong; // TODO what are these
        znode_data->modification_time = mslong;
        znode_data->blocksize = 0;
        znode_data->replication = 0;
        znode_data->filetype = IS_DIR;
    }

    /**
     * Make a directory in zookeeper
     */
    void ZkNnClient::mkdir(MkdirsRequestProto& request, MkdirsResponseProto& response) {
        const std::string& path = request.src();
        bool create_parent = request.createparent();
        if (!mkdir_helper(path, create_parent)) {
            response.set_result(false);
        }
        response.set_result(true);
    }

    /**
     * Helper for creating a directory znode. Iterates over the parents and crates them
     * if necessary.
     */
    bool ZkNnClient::mkdir_helper(const std::string& path, bool create_parent) {
        if (create_parent) {
            std::vector<std::string> split_path;
            boost::split(split_path, path, boost::is_any_of("/"));
            bool not_exist = false;
            std::string unroll;
            std::string p_path = "";
            for (int i = 0; i < split_path.size(); i++) {
                p_path += split_path[i] + "/";
                if (!file_exists(p_path)) {
                    // keep track of the path where we start creating directories
                    if (not_exist == false) {
                        unroll = p_path;
                    }
                    not_exist = true;
                    FileZNode znode_data;
                    set_mkdir_znode(&znode_data);
                    int error;
                    if ((error = create_file_znode(path, &znode_data))) {
                        // TODO unroll the created directories
                        return false;
                    }
                }
            }
        }
        else {
            FileZNode znode_data;
            set_mkdir_znode(&znode_data);
            return create_file_znode(path, &znode_data);
        }
        return true;
    }

    void ZkNnClient::get_block_locations(GetBlockLocationsRequestProto& req, GetBlockLocationsResponseProto& res) {

        int error_code;

        const std::string &src = req.src();
        const std::string zk_path = ZookeeperPath(src);

        google::protobuf::uint64 offset = req.offset();
        google::protobuf::uint64 length = req.length();

        LocatedBlocksProto* blocks = res.mutable_locations();

        FileZNode znode_data;
        read_file_znode(znode_data, src);

        blocks->set_underconstruction(false);
        blocks->set_islastblockcomplete(true);
        blocks->set_filelength(znode_data.length);

        uint64_t block_size = znode_data.blocksize;

        LOG(INFO) << CLASS_NAME << "Block size of " << zk_path << " is " << block_size;

        auto sorted_blocks = std::vector<std::string>();

        // TODO: Make more efficient
        if(!zk->get_children(zk_path, sorted_blocks, error_code)) {
            LOG(ERROR) << CLASS_NAME << "Failed getting children of " << zk_path << " with error: " << error_code;
        }
        uint64_t size = 0;
        for (auto sorted_block : sorted_blocks) {
            LOG(INFO) << CLASS_NAME << "Considering block " << sorted_block;
            if (size > offset + length) {
                // at this point the start of the block is at a higher offset than the segment we want
                LOG(INFO) << CLASS_NAME << "Breaking at block " << sorted_block;
                break;
            }
            if (size + block_size >= offset) {
                auto data = std::vector<uint8_t>();
                if (!zk->get(zk_path + "/" + sorted_block, data, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed to get " << zk_path << "/" << sorted_block << " info: " << error_code;
                    return; // TODO: Signal error
                }
                uint64_t block_id = *(uint64_t *)(&data[0]);
                LOG(INFO) << CLASS_NAME << "Found block " << block_id << " for " << zk_path;

                // TODO: This block of code should be moved to a function, repeated with add_block
                LocatedBlockProto* located_block = blocks->add_blocks();
                located_block->set_corrupt(0);
                located_block->set_offset(size); // TODO: This offset may be incorrect

                buildExtendedBlockProto(located_block->mutable_b(), block_id, block_size);

                auto data_nodes = std::vector<std::string>();

                LOG(INFO) << CLASS_NAME << "Getting datanode locations for block: " << "/block_locations/" + std::to_string(block_id);

                if (!zk->get_children("/block_locations/" + std::to_string(block_id), data_nodes, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed getting datanode locations for block: " << "/block_locations/" + std::to_string(block_id) << " with error: " << error_code;
                }

                LOG(INFO) << CLASS_NAME << "Found block locations " << data_nodes.size();

                for (auto data_node :data_nodes) {
                    buildDatanodeInfoProto(located_block->add_locs(), data_node);
                }
                buildTokenProto(located_block->mutable_blocktoken());
            }
            size += block_size;
        }
    }


    // ---------------------------------------- HELPERS ----------------------------------------

    std::string ZkNnClient::ZookeeperPath(const std::string &hadoopPath){
        std::string zkpath = NAMESPACE_PATH;
        if (hadoopPath.size() == 0) {
            LOG(ERROR) << " this hadoop path is invalid";
        }
        if (hadoopPath.at(0) != '/'){
            zkpath += "/";
        }
        zkpath += hadoopPath;
        if (zkpath.at(zkpath.length() - 1) == '/'){
            zkpath.at(zkpath.length() - 1) = '\0';
        }
        return zkpath;
    }

    void ZkNnClient::set_file_info(HdfsFileStatusProto* status, const std::string& path, FileZNode& znode_data) {
        HdfsFileStatusProto_FileType filetype;
        // get the filetype, since we do not want to serialize an enum
        switch(znode_data.filetype) {
            case(0):
                filetype = HdfsFileStatusProto::IS_DIR;
                break;
            case(1):
                filetype = HdfsFileStatusProto::IS_DIR;
                break;
            case(2):
                filetype = HdfsFileStatusProto::IS_FILE;
                break;
            default:
                break;

        }

        FsPermissionProto* permission = status->mutable_permission();
        // Shorcut to set permission to 777.
        permission->set_perm(~0);
        status->set_filetype(filetype);
        status->set_path(path);
        status->set_length(znode_data.length);
        status->set_blocksize(znode_data.blocksize);
        std::string owner(znode_data.owner);
        std::string group(znode_data.group);
        status->set_owner(owner);
        status->set_group(group);

        status->set_modification_time(znode_data.modification_time);
        status->set_access_time(znode_data.access_time);
        LOG(INFO) << CLASS_NAME << "Successfully set the file info ";
    }

    bool ZkNnClient::add_block(const std::string& file_path, std::uint64_t& block_id, std::vector<std::string> & data_nodes, uint32_t replicationFactor) {

        if (!file_exists(file_path)) {
            LOG(ERROR) << CLASS_NAME << "Cannot add block to non-existent file" << file_path;
            return false;
        }

        FileZNode znode_data;
        read_file_znode(znode_data, file_path);
        if (znode_data.under_construction) { // TODO: This is a faulty check
            LOG(WARNING) << "Last block for " << file_path << " still under construction";
        }
        // TODO: Check the replication factor

        std::string block_id_str;

        util::generate_uuid(block_id);
        block_id_str = std::to_string(block_id);
        LOG(INFO) << CLASS_NAME << "Generated block id " << block_id_str;

        if (!find_datanode_for_block(data_nodes, block_id, replicationFactor, true)) {
            return false;
        }

        // Add another block size to the file length and update zookeeper.
        znode_data.length += znode_data.blocksize;
        int error_code;
        std::vector<std::uint8_t> new_data(sizeof(znode_data));
        file_znode_struct_to_vec(&znode_data, new_data);
        if (!zk->set(ZookeeperPath(file_path), new_data, error_code)) {
            LOG(ERROR) << "Set failed" << error_code;
            return 0;
            // TODO : handle error
        }

        // Generate the massive multi-op for creating the block

        std::vector<std::uint8_t> data;
        data.resize(sizeof(u_int64_t));
        memcpy(&data[0], &block_id, sizeof(u_int64_t));

        LOG(INFO) << CLASS_NAME << "Generating block for " << ZookeeperPath(file_path);

        // ZooKeeper multi-op to
        auto seq_file_block_op = zk->build_create_op(ZookeeperPath(file_path + "/block_"), data, ZOO_SEQUENCE);
        auto ack_op = zk->build_create_op("/work_queues/wait_for_acks/" + block_id_str, ZKWrapper::EMPTY_VECTOR);
        auto block_location_op = zk->build_create_op("/block_locations/" + block_id_str, ZKWrapper::EMPTY_VECTOR);

        std::vector<std::shared_ptr<ZooOp>> ops = {seq_file_block_op, ack_op, block_location_op};

        auto results = std::vector <zoo_op_result>();
        int err;
        // TODO: Perhaps we have to perform a more fine grained analysis of the results
        if (!zk->execute_multi(ops, results, err)) {
            LOG(ERROR) << CLASS_NAME << "Failed to write the addBlock multiop, ZK state was not changed";
            ZKWrapper::print_error(err);
            return false;
        }
        return true;
    }

    // TODO: To simplify signature, could just get rid of the newBlock param
    // and always check for preexisting replicas
    bool ZkNnClient::find_datanode_for_block(std::vector<std::string>& datanodes, const u_int64_t blockId, uint32_t replication_factor, bool newBlock) {
        // TODO: Actually perform this action
        // TODO: Perhaps we should keep a cached list of nodes

        std::vector<std::string> live_data_nodes = std::vector <std::string>();
        int error_code;

        // Get all of the live datanodes
        if (zk->get_children(HEALTH, live_data_nodes, error_code)) {

            // LOG(INFO) << CLASS_NAME << "Found live DNs: " << live_data_nodes;
            auto excluded_datanodes = std::vector <std::string>();
            if (!newBlock) {
                // Get the list of datanodes which already have a replica
                if (zk->get_children(BLOCK_LOCATIONS + std::to_string(blockId), excluded_datanodes, error_code)) {
                    // Remove the excluded datanodes from the live list
                    for (auto excluded : excluded_datanodes) {
                        std::vector<std::string>::iterator it = std::find(
                                live_data_nodes.begin(),
                                live_data_nodes.end(),
                                excluded);

                        if (it != live_data_nodes.end()) {
                            // The excluded dn was found, so remove it
                            live_data_nodes.erase(it);
                        }
                    }
                } else {
                    LOG(ERROR) << CLASS_NAME << "Error getting children of: " << BLOCK_LOCATIONS + std::to_string(blockId);
                    return false;
                }
            }
            // LOG(INFO) << CLASS_NAME << "Live DNs after excluding those with existing replicas: " << live_data_nodes;

            /* for each child, check if the ephemeral node exists */
            for(auto datanode : live_data_nodes) {
                bool isAlive;
                if (!zk->exists(HEALTH_BACKSLASH + datanode + HEARTBEAT, isAlive, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed to check if datanode: " + datanode << " is alive: " << error_code;
                }
                if (isAlive) {
                    datanodes.push_back(datanode);
                }
                if (datanodes.size() == replication_factor) {
                    LOG(INFO) << CLASS_NAME << "Found " << replication_factor << " datanodes";
                    break;
                }
            }
        } else {
            LOG(ERROR) << CLASS_NAME << "Failed to get list of datanodes at " + HEALTH + " " << error_code;
            return false;
        }

        // TODO: Read strategy from config, but as of now select the first few blocks that are valid

        if (datanodes.size() > replication_factor) {
            LOG(ERROR) << CLASS_NAME << "Failed to find at least " << replication_factor << " datanodes at " + HEALTH;
            return false;
        }
        return true;
    }

    /**
     * Updates /fileSystem in ZK to reflect a file rename
     * @param src The path to the source file (not znode) within the filesystem
     * @param dst The path to the renamed destination file (not znode) within the filesystem
     * @return Boolean indicating success or failure of the rename
     */
    bool ZkNnClient::rename_file(std::string src, std::string dst) {
        LOG(INFO) << "Renaming '"  << src << "' to '" << dst << "'";

        int error_code = 0;
        auto data = std::vector<std::uint8_t>();
        auto ops = std::vector<std::shared_ptr<ZooOp>>();

        std::string src_znode = ZookeeperPath(src);
        std::string dst_znode = ZookeeperPath(dst);

        // TODO: if one of these fails, should we try to undo? Use a multiop here?

        // Get the payload from the old filesystem znode for the src
        zk->get(src_znode, data, error_code);
        if (error_code != ZOK) {
            LOG(ERROR) << "Failed to get data from '" << src_znode << "' when renaming.";
            return false;
        }

        // Create a new znode in the filesystem for the dst
        ops.push_back(zk->build_create_op(dst_znode, data));

        // Copy over the data from the children of the src_znode into new children of the dst_znode
        auto children = std::vector<std::string>();
        zk->get_children(src_znode, children, error_code);
        if (error_code != ZOK) {
            LOG(ERROR) << "Failed to get children of znode '" << src_znode << "' when renaming.";
            return false;
        }

        for (auto child : children) {
            // Get child's data
            auto child_data = std::vector<std::uint8_t>();
            zk->get(src_znode + "/" + child, child_data, error_code);
            if (error_code != ZOK) {
                LOG(ERROR) << "Failed to get data from '" << child << "' when renaming.";
                return false;
            }

            // Create new child of dst_znode with this data
            ops.push_back(zk->build_create_op(dst_znode + "/" + child, child_data));

            // Delete src_znode's child
            ops.push_back(zk->build_delete_op(src_znode + "/" + child));
        }

        // Remove the old znode for the src
        ops.push_back(zk->build_delete_op(src_znode));

        std::vector<zoo_op_result> results = std::vector<zoo_op_result>();
        if (!zk->execute_multi(ops, results, error_code)) {
            LOG(ERROR) << "Failed multiop when renaming: '" << src << "' to '" << dst << "'";
            for (int i = 0; i < results.size(); i++) {
                LOG(ERROR) << "\t MULTIOP #" << i << " ERROR CODE: " << results[i].err;
            }
            return false;
        }

        LOG(INFO) << "Successfully renamed '"  << src << "' to '" << dst << "'";
        return true;
    }

    /**
     * Checks that each block UUID in the wait_for_acks dir:
     *	 1. has REPLICATION_FACTOR many children
     *	 2. if the block UUID was created more than ACK_TIMEOUT seconds ago
     * TODO: Add to header file
     * @return
     */
    bool ZkNnClient::check_acks() {
        int error_code = 0;

        // Get the current block UUIDs that are waiting to be fully replicated
        // TODO: serialize block_uuids as u_int64_t rather than strings
        auto block_uuids = std::vector<std::string>();
        // TODO: Change all path constants in zk_client_common to NOT end in /
        if (!zk->get_children(WORK_QUEUES + WAIT_FOR_ACK, block_uuids, error_code)) {
            LOG(ERROR) << CLASS_NAME << "ERROR CODE: " << error_code << " occurred in check_acks when getting children for " << WORK_QUEUES + WAIT_FOR_ACK;
            return false; // TODO: Is this the right return val?
        }
        LOG(INFO) << CLASS_NAME << "Checking acks for: " << block_uuids.size() << " blocks";

        for (auto block_uuid : block_uuids) {
            LOG(INFO) << CLASS_NAME << "Considering block: " << block_uuid;
            std::string block_path = WORK_QUEUES + WAIT_FOR_ACK_BACKSLASH + block_uuid;

            auto data = std::vector<std::uint8_t>();
            if (!zk->get(block_path, data, error_code)) {
                LOG(ERROR) << CLASS_NAME << "Error getting payload at: " << block_path;
                return false;
            }
            int replication_factor = unsigned(data[0]);
            LOG(INFO) << CLASS_NAME << "Replication factor for " << block_uuid << " is " << replication_factor;

            // Get the datanodes with have replicated this block
            auto existing_dn_replicas = std::vector<std::string>();
            if (!zk->get_children(block_path, existing_dn_replicas, error_code)) {
                LOG(ERROR) << CLASS_NAME << "ERROR CODE: " << error_code << " occurred in check_acks when getting children for " << block_path;
                return false;
            }
            LOG(INFO) << CLASS_NAME << "Found " << existing_dn_replicas.size() << " replicas of " << block_uuid;

            int elapsed_time = seconds_since_creation(block_path);
            if (elapsed_time < 0) {
                LOG(ERROR) << CLASS_NAME << "Failed to get elapsed time";
            }

            if (existing_dn_replicas.size() == 0 && elapsed_time > ACK_TIMEOUT) {
                // Block is not available on any DNs and cannot be replicated.
                // Emit error and remove this block from wait_for_acks
                LOG(ERROR) << CLASS_NAME << block_path << " has 0 replicas! Delete from wait_for_acks.";
                if (!zk->delete_node(block_path, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed to delete: " << block_path;
                    return false;
                }
                return false;

            } else if (existing_dn_replicas.size() < replication_factor && elapsed_time > ACK_TIMEOUT) {
                LOG(INFO) << CLASS_NAME << "Not yet enough replicas after time out for " << block_uuid;
                // Block hasn't been replicated enough, request remaining replicas
                int replicas_needed = replication_factor - existing_dn_replicas.size();
                LOG(INFO) << CLASS_NAME << replicas_needed << " replicas are needed";
                replicate_block(block_uuid, replicas_needed, existing_dn_replicas);

            } else if (existing_dn_replicas.size() == replication_factor) {
                LOG(INFO) << CLASS_NAME << "Enough replicas have been made, no longer need to wait on " << block_path;
                if (!zk->recursive_delete(block_path, error_code)) {
                    LOG(ERROR) << CLASS_NAME << "Failed to delete: " << block_path;
                    return false;
                }
            } else {
                LOG(INFO) << CLASS_NAME << "Not enough replicas, but still time left" << block_path;
            }
        }

        return true;
    }

    /**
     * Creates 'num_replicas' many work items for the given 'block_uuid' in
     * the replicate work queue, ensuring that the new replicas are not on
     * an excluded datanode
     */
    bool ZkNnClient::replicate_block(const std::string &block_uuid_str, int num_replicas, std::vector<std::string> &excluded_datanodes) {
        int error_code = 0;
        u_int64_t block_uuid = std::strtoll(block_uuid_str.c_str(), NULL, 10);
        auto datanodes = std::vector<std::string>();
        if(!find_datanode_for_block(datanodes, block_uuid, num_replicas, false)) {
            LOG(ERROR) << CLASS_NAME << "Failed to get available datanodes for replications";
        }

        // Create a payload vector containing the block_uuid
        std::vector<std::uint8_t> data;
        data.resize(sizeof(u_int64_t));
        memcpy(&data[0], &block_uuid, sizeof(u_int64_t));

        for (auto dn_id : datanodes) {
            // The path of the sequential node, ends in "-"
            std::string replicate_block_path = WORK_QUEUES + REPLICATE_BACKSLASH + dn_id + "/block-";

            // Create an item on the replica queue for this block replica on the given datanode
            std::string new_path;
            if (!zk->create_sequential(replicate_block_path,
                                       data,
                                       new_path,
                                       false,
                                       error_code)) {
                LOG(ERROR) << CLASS_NAME << "Failed to create_seq: " << replicate_block_path;
                return false;
            }
            LOG(INFO) << CLASS_NAME << "Created: " << new_path;
        }

        return true;
    }

    int ZkNnClient::seconds_since_creation(std::string &path) {
        // TODO
        return 1;
    }

    /**
     * Returns the current timestamp in milliseconds
     */
    uint64_t ZkNnClient::current_time_ms() {
        // http://stackoverflow.com/questions/19555121/how-to-get-current-timestamp-in-milliseconds-since-1970-just-the-way-java-gets
        struct timeval tp;
        // Get current timestamp
        gettimeofday(&tp, NULL);
        // Convert to milliseconds
        return (uint64_t) tp.tv_sec * 1000L + tp.tv_usec / 1000;
    }

    bool ZkNnClient::buildDatanodeInfoProto(DatanodeInfoProto* dn_info, const std::string& data_node) {

        int error_code;

        std::vector<std::string> split_address;
        boost::split(split_address, data_node, boost::is_any_of(":"));
        assert(split_address.size() == 2);

        auto data = std::vector<std::uint8_t>();
        if (zk->get(HEALTH_BACKSLASH + data_node + STATS, data, error_code)) {
            LOG(ERROR) << CLASS_NAME << "Getting data node stats failed with " << error_code;
        }

        zkclient::DataNodePayload * payload = (zkclient::DataNodePayload *) (&data[0]);

        DatanodeIDProto* id = dn_info->mutable_id();
        id->set_ipaddr(split_address[0]);
        id->set_hostname("localhost"); // TODO: Fill out with the proper value
        id->set_datanodeuuid("1234");
        id->set_xferport(payload->xferPort);
        id->set_infoport(50020);
        id->set_ipcport(payload->ipcPort);
        return true;
    }

    bool ZkNnClient::buildTokenProto(hadoop::common::TokenProto* token) {
        token->set_identifier("open");
        token->set_password("sesame");
        token->set_kind("foo");
        token->set_service("bar");
        return true;
    }

    bool ZkNnClient::buildExtendedBlockProto(ExtendedBlockProto* eb, const std::uint64_t& block_id,
                                             const uint64_t& block_size) {
        eb->set_poolid("0");
        eb->set_blockid(block_id);
        eb->set_generationstamp(1);
        eb->set_numbytes(block_size);
        return true;
    }
}

#endif