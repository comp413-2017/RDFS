#include <zookeeper.h>
#include "zkwrapper.h"


int init = 0;
static zhandle_t *zh;

ZKWrapper::ZKWrapper(zhandle_t *handle) {
//    zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
//    if (!zh) {
//        exit(1);
//    }
    zh = handle;
    init = 1;

}


int ZKWrapper::create(std::string path, std::string data, int num_bytes) {
    if (!init) {
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, path.c_str(), data.c_str(), num_bytes, &ZOO_OPEN_ACL_UNSAFE, 0, 0, 0);
    if (rc != 0) {
        exit(1); // TODO: Handle error
    }
}

std::string ZKWrapper::get(std::string path) {
    char* buffer = new char[512];
    int buf_len = sizeof(buffer);
    struct Stat stat;

    int rc = zoo_get(zh, path.c_str(), 0, buffer, &buf_len, &stat);

    if (rc) {
        printf("Error when getting!");
        exit(1); // TODO: error handling
    }

    return std::string(buffer);
}

void ZKWrapper::close() {
    zookeeper_close(zh);
}


