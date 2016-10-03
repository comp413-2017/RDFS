#include <string>
#include "zk_wrapper.hpp"
#include <zookeeper.h>

static zhandle_t *zh;

int FALSE = 0;
int INIT = 0;

int main(int argc, char * argv[]) {

    return 0;
}

/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path,
             void *watcherCtx) {}

void init(std::string address) {
    zh = zookeeper_init(address.c_str(), watcher, 10000, 0, 0, 0);
    if (!zh) {
        exit(1);
    }
    INIT = 1;
}

void close() {
    zookeeper_close(zh);
}


int create_znode(std::string path, std::string value, int num_bytes){
    if (!INIT) {
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, path.c_str(), value.c_str(), num_bytes, &ZOO_OPEN_ACL_UNSAFE, FALSE, 0, 0);
    if (rc != 0) {
        exit(1); // TODO: Handle error
    }
}

std::string get_znode_value(std::string& path) {
    char* buffer = new char[512];
    int buflen = sizeof(buffer);
    struct Stat stat;

    int rc = zoo_get(zh, "/xyz", 0, buffer, &buflen, &stat);
    if (rc) {
//        fprintf(stderr, "Error %d for %s\n", rc, __LINE__);
        exit(1); // TODO: error handling
    }
    return std::string(buffer);
}
