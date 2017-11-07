// Copyright 2017 Rice University, COMP 413 2017

#include <datatransfer.pb.h>
#include <hdfs.pb.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>

#include <asio.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include "native_filesystem.h"
#include "rpcserver.h"
#include "socket_reads.h"
#include "socket_writes.h"
#include "zk_dn_client.h"

#pragma once

/*
 * Operation types
 */
#define WRITE_BLOCK 80
#define READ_BLOCK 81
#define READ_METADATA 82
#define REPLACE_BLOCK 83
#define COPY_BLOCK 84
#define BLOCK_CHECKSUM 85
#define TRANSFER_BLOCK 86
#define REQUEST_SHORT_CIRCUIT_FDS 87
#define RELEASE_SHORT_CIRCUIT_FDS 88
#define REQUEST_SHORT_CIRCUIT_SHM 89
#define BLOCK_GROUP_CHECKSUM 90
#define CUSTOM 127

using asio::ip::tcp;
using hadoop::hdfs::ExtendedBlockProto;
using hadoop::hdfs::PacketHeaderProto;

class TransferServer {
 public:
  TransferServer(int port,
                 std::shared_ptr<nativefs::NativeFS> &fs,
                 std::shared_ptr<zkclient::ZkClientDn> &dn,
                 int max_xmits = 10);

  TransferServer(const TransferServer &other) {}

  void serve(asio::io_service &io_service);
  bool sendStats();

  /**
   * @param len the length of the block
   * @param ip the ip of the datanode we are sending the read request to
   * @param xferport the xfer port of the datandoe we are sending the read
   *                 request to
   * @param blockToTarget the block info of the block to replicate
   *
   * Send a read request to anotehr datanode for a certain block, stream in the
   * packets and write them to our disk
   */
  bool replicate(uint64_t len, std::string ip, std::string xferport,
                 ExtendedBlockProto blockToTarget);

  bool remote_read(uint64_t len, std::string ip,
                                   std::string xferport,
                                   ExtendedBlockProto blockToTarget,
                                   std::string data, int &read_len);

  bool rmBlock(uint64_t block_id);
  bool writeBlock(uint64_t block_id, std::string data);
  bool poll_replicate();
  bool poll_delete();
  bool poll_reconstruct();

 private:
  int max_xmits;
  int port;
  std::atomic<std::uint32_t> xmits{0};
  std::shared_ptr<nativefs::NativeFS> fs;
  std::shared_ptr<zkclient::ZkClientDn> dn;

  mutable std::mutex m;
  std::condition_variable cv;

  bool receive_header(tcp::socket &sock, uint16_t *version,
                      unsigned char *type);
  bool write_header(tcp::socket &sock, uint16_t version, unsigned char type);
  void handle_connection(tcp::socket sock);
  void processWriteRequest(tcp::socket &sock);
  void processReadRequest(tcp::socket &sock);
  void buildBlockOpResponse(std::string &response_string);
  void ackPacket(tcp::socket &sock, PacketHeaderProto &p_head);

  bool writeFinalPacket(tcp::socket &sock, uint64_t, uint64_t);
  template<typename BufType>
  bool writePacket(tcp::socket &sock, PacketHeaderProto p_head,
                   const BufType &payload);
  void synchronize(std::function<void(TransferServer &, tcp::socket &)> f,
                   tcp::socket &sock);
};

// Templated method to be generic across any asio buffer type.
template<typename BufType>
bool TransferServer::writePacket(tcp::socket &sock,
                                 PacketHeaderProto p_head,
                                 const BufType &payload) {
  std::string p_head_str;
  p_head.SerializeToString(&p_head_str);
  const uint16_t header_len = p_head_str.length();
  // Add 4 to account for the size of uint32_t.
  // Also add in |cksums| u4s; these are a part of the payload and thus the
  // payload length
  const uint32_t payload_len = 4 + asio::buffer_size(payload);
  // Write payload length, header length, header, payload.

  return (rpcserver::write_int32(sock, payload_len) &&
      rpcserver::write_int16(sock, header_len) &&
      rpcserver::write_proto(sock, p_head_str) &&
      payload_len - 4 == sock.write_some(payload));
}
