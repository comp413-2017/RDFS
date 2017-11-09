// Copyright 2017 Rice University, COMP 413 2017

#include "data_transfer_server.h"

#include <easylogging++.h>

#include <functional>
#include <iostream>
#include <thread>

#include <asio.hpp>
#include <boost/crc.hpp>
#include <boost/lockfree/spsc_queue.hpp>

#define ERROR_AND_RETURN(msg) LOG(ERROR) << msg; return

using asio::ip::tcp;
using hadoop::hdfs::BaseHeaderProto;
using hadoop::hdfs::BlockOpResponseProto;
using hadoop::hdfs::CHECKSUM_NULL;
using hadoop::hdfs::ChecksumProto;
using hadoop::hdfs::OpBlockChecksumResponseProto;
using hadoop::hdfs::OpReadBlockProto;
using hadoop::hdfs::OpWriteBlockProto;
using hadoop::hdfs::OpWriteBlockProto_BlockConstructionStage;
using hadoop::hdfs::ClientOperationHeaderProto;
using hadoop::hdfs::ClientReadStatusProto;
using hadoop::hdfs::DatanodeIDProto;
using hadoop::hdfs::DatanodeInfoProto;
using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::PacketHeaderProto;
using hadoop::hdfs::PipelineAckProto;
using hadoop::hdfs::ReadOpChecksumInfoProto;
using hadoop::hdfs::SUCCESS;

// Default from CommonConfigurationKeysPublic.java#IO_FILE_BUFFER_SIZE_DEFAULT
const size_t PACKET_PAYLOAD_BYTES = 4096 * 4;

TransferServer::TransferServer(int port,
                               std::shared_ptr<nativefs::NativeFS> &fs,
                               std::shared_ptr<zkclient::ZkClientDn> &dn,
                               int max_xmits) : port(port), fs(fs), dn(dn),
                                                max_xmits(max_xmits) {}

bool TransferServer::receive_header(tcp::socket &sock, uint16_t *version,
                                    unsigned char *type) {
  return (rpcserver::read_int16(sock, version) &&
    rpcserver::read_byte(sock, type));
}

bool TransferServer::write_header(tcp::socket &sock, uint16_t version,
                                  unsigned char type) {
  return (rpcserver::write_int16(sock, version) &&
    rpcserver::write_byte(sock, type));
}

void TransferServer::handle_connection(tcp::socket sock) {
  for (;;) {
    asio::error_code error;
    uint16_t version;
    unsigned char type;
    if (receive_header(sock, &version, &type)) {
      LOG(INFO)
        << "Got header version="
        << version
        << ", type="
        << static_cast<int>(type);
    } else {
      ERROR_AND_RETURN("Failed to receive header, maybe connection closed.");
    }
    // TODO(anyone): implement proto handlers based on type
    switch (type) {
      case (WRITE_BLOCK): {
        std::function<void(TransferServer &, tcp::socket &)> writeFn =
          &TransferServer::processWriteRequest;
        synchronize(writeFn, sock);
      }
        break;
      case (READ_BLOCK): {
        std::function<void(TransferServer &, tcp::socket &)> readFn =
          &TransferServer::processReadRequest;
        synchronize(readFn, sock);
      }
        break;
      case (READ_METADATA):
        ERROR_AND_RETURN("Handler for read-metadata not written yet.");
      case (REPLACE_BLOCK):
        ERROR_AND_RETURN("Handler for replace-block not written yet.");
      case (COPY_BLOCK):
        ERROR_AND_RETURN("Handler for copy-block not written yet.");
      case (BLOCK_CHECKSUM):
        ERROR_AND_RETURN("Handler for block-checksum not written yet.");
      case (TRANSFER_BLOCK):
        ERROR_AND_RETURN("Handler for transfer-block not written yet.");
      case (REQUEST_SHORT_CIRCUIT_FDS):
        ERROR_AND_RETURN("Handler for request-short-circuit-fds not written "
                         "yet.");
      case (RELEASE_SHORT_CIRCUIT_FDS):
        ERROR_AND_RETURN("Handler for release-short-circuit-fds not written "
                         "yet.");
      case (REQUEST_SHORT_CIRCUIT_SHM):
        ERROR_AND_RETURN("Handler for request-short-circuit-shm not written "
                         "yet.");
      case (BLOCK_GROUP_CHECKSUM):
        ERROR_AND_RETURN("Handler for block-group-checksum not written yet.");
      case (CUSTOM):
        ERROR_AND_RETURN("Handler for custom-op not written yet.");
      default:
        ERROR_AND_RETURN("Unknown operation type specified.");
    }
  }
}

void TransferServer::processWriteRequest(tcp::socket &sock) {
  OpWriteBlockProto proto;
  if (rpcserver::read_delimited_proto(sock, proto)) {
    LOG(DEBUG) << "Op a write block proto";
  } else {
    ERROR_AND_RETURN("Failed to op the write block proto.");
  }
  std::string response_string;
  buildBlockOpResponse(response_string);

  const ClientOperationHeaderProto header = proto.header();
  std::vector<DatanodeInfoProto> targets;
  for (int i = 0; i < proto.targets_size(); i++) {
    targets.push_back(proto.targets(i));
  }

  const DatanodeInfoProto src = proto.source();
  const OpWriteBlockProto_BlockConstructionStage stage =
    proto.stage();
  int pipelineSize = proto.pipelinesize();
  // num bytes in block
  uint64_t bytesInBlock = proto.minbytesrcvd();
  // num bytes sent
  uint64_t bytesSent = proto.maxbytesrcvd();

  if (rpcserver::write_delimited_proto(sock, response_string)) {
    LOG(INFO) << "Successfully sent response to client";
  } else {
    ERROR_AND_RETURN("Could not send response to client");
  }

  // read packets of block from client
  bool last_packet = false;
  std::string block_data;
  // 1 added to include the empty termination packet
  int max_capacity = bytesInBlock / PACKET_PAYLOAD_BYTES + 1;
  PacketHeaderProto last_header;
  while (!last_packet) {
    asio::error_code error;
    uint32_t payload_len;
    rpcserver::read_int32(sock, &payload_len);
    uint16_t header_len;
    rpcserver::read_int16(sock, &header_len);
    PacketHeaderProto p_head;
    rpcserver::read_proto(sock, p_head, header_len);
    // LOG(INFO) << "Receigin packet " << p_head.seqno();
    last_packet = p_head.lastpacketinblock();
    uint64_t data_len = p_head.datalen();
    uint32_t checksum_len = payload_len - sizeof(uint32_t) - data_len;
    std::string checksum(checksum_len, 0);
    std::string data(data_len, 0);
    error = rpcserver::read_full(sock, asio::buffer(&checksum[0],
                                                    checksum_len));
    if (error) {
      LOG(ERROR) << "Failed to read checksum for packet " << p_head.seqno();
      break;
    }
    error = rpcserver::read_full(sock, asio::buffer(&data[0], data_len));
    if (error) {
      LOG(ERROR) << "Failed to read packet " << p_head.seqno();
      break;
    }
    if (!last_packet && data_len != 0) {
      block_data += data;
    }
    // Wait free queue will return false on failure to insert element. Keep
    // trying until insert works
    if (!last_packet) {
      ackPacket(sock, p_head);
    } else {
      last_header = p_head;
    }
  }

  if (!fs->writeBlock(header.baseheader().block().blockid(), block_data)) {
    LOG(ERROR)
      << "Failed to allocate block "
      << header.baseheader().block().blockid();
    return;
  } else {
    if (dn->blockReceived(header.baseheader().block().blockid(),
                          block_data.length())) {
      ackPacket(sock, last_header);
    } else {
      LOG(ERROR) << "Failed to register received block with NameNode";
      return;
    }
  }

  LOG(INFO)
    << "Replicating onto "
    << proto.targets_size()
    << " target data nodes";
  for (int i = 0; i < proto.targets_size(); i++) {
    const DatanodeIDProto &dn_p = proto.targets(i).id();
    std::string dn_name = dn_p.ipaddr() + ":" + std::to_string(dn_p.ipcport());
    if (!dn->push_dn_on_repq(dn_name, header.baseheader().block().blockid())) {
      // do nothing, check for acks will pick this up (hopefully!)
    }
  }
}

void TransferServer::ackPacket(tcp::socket &sock,
                               PacketHeaderProto &p_head) {
  // ack packet
  PipelineAckProto ack;
  ack.set_seqno(p_head.seqno());
  // LOG(INFO) << "Acking " << p_head.seqno();
  ack.add_reply(SUCCESS);
  std::string ack_string;
  ack.SerializeToString(&ack_string);
  if (rpcserver::write_delimited_proto(sock, ack_string)) {
    // LOG(INFO) << "Successfully sent ack to client";
  } else {
    LOG(ERROR) << "Could not send ack to client";
  }
}

void TransferServer::processReadRequest(tcp::socket &sock) {
  OpReadBlockProto proto;
  if (rpcserver::read_delimited_proto(sock, proto)) {
    LOG(INFO) << "Op a read block proto" << std::endl;
  } else {
    ERROR_AND_RETURN("Failed to op the read block proto.");
  }
  std::string response_string;

  buildBlockOpResponse(response_string);
  if (rpcserver::write_delimited_proto(sock, response_string)) {
    LOG(INFO) << "Successfully sent response to client";
  } else {
    ERROR_AND_RETURN("Could not send BlockOpResponseProto in read request.");
  }

  uint64_t blockID = proto.header().baseheader().block().blockid();
  std::string block;
  bool success = fs->getBlock(blockID, block);
  if (!success) {
    LOG(ERROR) << "Failure on fs.getBlock";
  }

  uint64_t offset = proto.offset();
  uint64_t len = std::min(block.size() - offset, proto.len());
  if (offset > block.size()) {
    len = 0;
  }

  uint64_t seq = 0;
  while (len > 0) {
    PacketHeaderProto p_head;
    p_head.set_offsetinblock(offset);
    p_head.set_seqno(seq);
    p_head.set_lastpacketinblock(false);
    // The payload to write can be no more than what fits in the packet, or
    // remaining requested length.
    uint64_t payload_size = std::min(len, PACKET_PAYLOAD_BYTES);
    p_head.set_datalen(payload_size);
    p_head.set_syncblock(false);
    if (writePacket(sock, p_head, asio::buffer(&block[offset], payload_size))) {
      // LOG(INFO) << "Successfully sent packet " << seq << " to client";
      // LOG(INFO) << "Packet " << seq << " had " << payload_size << " bytes";
    } else {
      LOG(ERROR) << "Could not send packet " << seq << " to client";
      break;
    }
    offset += payload_size;
    // Decrement len, cannot underflow because payload size <= len.
    len -= payload_size;
    seq++;
  }

  if (len == 0) {
    // Finish by writing the last empty packet, if we finished.
    if (writeFinalPacket(sock, offset, seq)) {
      // Receive a status code from the client.
      ClientReadStatusProto status_proto;
      if (rpcserver::read_delimited_proto(sock, status_proto)) {
        LOG(INFO) << "Received read status from client.";
      } else {
        LOG(ERROR) << "Could not read status from client.";
      }
    } else {
      LOG(ERROR) << "Could not send final packet to client";
    }
  }
}

// Write the final 0-payload packet to the client, and return whether
// successful.
bool TransferServer::writeFinalPacket(tcp::socket &sock, uint64_t offset,
                                      uint64_t seq) {
  PacketHeaderProto p_head;
  p_head.set_offsetinblock(offset);
  p_head.set_seqno(seq);
  p_head.set_lastpacketinblock(true);
  p_head.set_datalen(0);
  // No payload, so empty string.
  return writePacket(sock, p_head, asio::buffer(""));
}

void TransferServer::buildBlockOpResponse(std::string &response_string) {
  BlockOpResponseProto response;
  response.set_status(SUCCESS);
  OpBlockChecksumResponseProto *checksum_res =
    response.mutable_checksumresponse();
  checksum_res->set_bytespercrc(13);
  checksum_res->set_crcperblock(7);
  checksum_res->set_md5("this is my md5");
  ReadOpChecksumInfoProto *checksum_info =
    response.mutable_readopchecksuminfo();
  checksum_info->set_chunkoffset(0);
  ChecksumProto *checksum = checksum_info->mutable_checksum();
  checksum->set_type(CHECKSUM_NULL);
  checksum->set_bytesperchecksum(17);
  response.SerializeToString(&response_string);
}

void TransferServer::serve(asio::io_service &io_service) {
  LOG(INFO) << "Transfer Server listens on :" << this->port;
  tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), this->port));

  for (;;) {
    tcp::socket sock(io_service);
    a.accept(sock);
    std::thread(&TransferServer::handle_connection, this, std::move(sock))
      .detach();
  }
}

void TransferServer::synchronize(std::function<void(TransferServer &,
                                                    tcp::socket &)> f,
                                 tcp::socket &sock) {
  std::unique_lock<std::mutex> lk(m);
  while (xmits.fetch_add(0) >= max_xmits) {
    cv.wait(lk);
  }
  xmits++;
  LOG(DEBUG) << "**********" << "num xmits is " << xmits.fetch_add(0);
  lk.unlock();
  f(*this, sock);
  xmits--;
  cv.notify_one();
}

bool TransferServer::remote_read(uint64_t len, std::string ip,
                                   std::string xferport,
                                   ExtendedBlockProto blockToTarget,
                                   std::string data, int &read_len) {
// connect to the datanode
  asio::io_service io_service;
  std::string port = xferport;
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(ip, port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

  tcp::socket sock(io_service);
  asio::connect(sock, endpoint_iterator);

  // construct the rpc op_read request
  std::string read_string;
  OpReadBlockProto read_block_proto;
  read_block_proto.set_len(len);
  LOG(INFO) << "Requesting replica of length " << len << std::endl;

  read_block_proto.set_offset(0);
  read_block_proto.set_sendchecksums(true);

  ClientOperationHeaderProto *header =
    read_block_proto.mutable_header();
  // TODO(anyone): same as DFS client for now
  header->set_clientname("DFSClient_NONMAPREDUCE_2010667435_1");

  BaseHeaderProto *base_proto = header->mutable_baseheader();
  base_proto->set_allocated_block(&blockToTarget);
  ::hadoop::common::TokenProto *token_header = base_proto->mutable_token();

  token_header->set_identifier("open");
  token_header->set_password("sesame");
  token_header->set_kind("foo");
  token_header->set_service("bar");

  // send the read request
  uint16_t version = 1000;  // TODO(anyone): what is the version
  unsigned char read_request = READ_BLOCK;
  read_block_proto.SerializeToString(&read_string);
  LOG(INFO) << " writing read request " << std::to_string(read_request);
  if (!write_header(sock, version, read_request)) {
    LOG(ERROR) << " could not write the header request to target datanode";
    return false;
  } else {
    LOG(INFO) << " successfully wrote the header request to target datanode";
  }

  if (!rpcserver::write_delimited_proto(sock, read_string)) {
    LOG(ERROR)
      << " could not write the delimited read request to target datanode";
    return false;
  } else {
    LOG(INFO)
      << " successfully wrote the delimited read request to target datanode";
  }

  // read the read response
  BlockOpResponseProto read_response;
  if (!rpcserver::read_delimited_proto(sock, read_response)) {
    LOG(ERROR) << " could not read the read response from target datanode";
    return false;
  } else {
    LOG(INFO) << " successfully read the read response from target datanode";
  }

  // LOCK
  std::unique_lock<std::mutex> lk(m);
  while (xmits.fetch_add(0) >= max_xmits) {
    cv.wait(lk);
  }
  xmits++;
  LOG(DEBUG) << "**********" << "num xmits is " << xmits.fetch_add(0);
  lk.unlock();

  // read in the packets while we still have them, and we haven't processed the
  // final packet
  read_len = 0;
  bool last_packet = false;
  while (read_len < len && !last_packet) {
    asio::error_code error;
    uint32_t payload_len;
    rpcserver::read_int32(sock, &payload_len);
    uint16_t header_len;
    rpcserver::read_int16(sock, &header_len);
    PacketHeaderProto p_head;
    rpcserver::read_proto(sock, p_head, header_len);
    // LOG(INFO) << "Receiving packet " << p_head.seqno();
    // read in the data
    if (!p_head.lastpacketinblock()) {
      uint64_t data_len = p_head.datalen();
      uint64_t seqno = p_head.seqno();
      uint64_t offset = p_head.offsetinblock();
      if (data_len + offset > len) {
        LOG(ERROR) << "Bad data length encountered.";
        xmits--;
        cv.notify_one();
        return false;
      }
      error = rpcserver::read_full(sock, asio::buffer(&data[offset], data_len));
      if (error) {
        LOG(ERROR) << "Failed to read packet " << p_head.seqno();
        xmits--;
        cv.notify_one();
        return false;
      }
      read_len += data_len;
    } else {
      last_packet = true;
    }
  }
  xmits--;
  cv.notify_one();
  return true;
}

bool TransferServer::replicate(uint64_t len, std::string ip,
                               std::string xferport,
                               ExtendedBlockProto blockToTarget) {
  LOG(INFO)
    << " replicating length "
    << len
    << " with ip "
    << ip
    << " and port "
    << xferport;

  LOG(INFO) << "blockToTarget is " << blockToTarget.blockid();

  if (fs->hasBlock(blockToTarget.blockid())) {
    LOG(INFO) << "Block already exists on this DN";
    return true;
  } else {
    LOG(INFO) << "Block not found on this DN, replicating...";
  }

  std::string data(len, 0);
  int read_len = 0;
  if (!remote_read(len, ip, xferport, blockToTarget, data, read_len)) {
    return false;
  }
  uint64_t block_id = blockToTarget.blockid();

  // TODO(anyone): send ClientReadStatusProto (delimited)
  if (!fs->writeBlock(block_id, data)) {
    LOG(ERROR)
      << "Failed to allocate block "
      << block_id;
    return false;
  } else {
    dn->blockReceived(block_id, read_len);
  }

  // Pretty confident we don't need this line,
  // but if we get a bug this is a place to check
  // base_proto->release_block();
  LOG(INFO) << "Replication complete, closing connection.";
  return true;
}

bool TransferServer::rmBlock(uint64_t block_id) {
  return fs->rmBlock(block_id);
}

bool TransferServer::writeBlock(uint64_t block_id, std::string data) {
  return fs->writeBlock(block_id, data);
}

bool TransferServer::sendStats() {
  uint64_t free_space = fs->getFreeSpace();
  // LOG(INFO) << "Sending stats " << free_space;
  return dn->sendStats(free_space, xmits.fetch_add(0));
}

bool TransferServer::poll_replicate() {
  return dn->poll_replication_queue();
}

bool TransferServer::poll_delete() {
  return dn->poll_delete_queue();
}

bool TransferServer::poll_reconstruct() {
  return dn->poll_reconstruct_queue();
}
