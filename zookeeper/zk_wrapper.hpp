#ifndef _ZK_WRAPPER_
#define _ZK_WRAPPER_

#include <string>

void init(
    const std::string& address
);

void close();

int create_znode(
  const std::string& path,
  const std::string& data,
  const int num_bytes
  );

std::string get_znode_value(
    const std::string& path
);

#endif
