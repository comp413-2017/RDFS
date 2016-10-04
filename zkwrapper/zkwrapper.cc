//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
//
// Modified by Zhouhan Chen on 10/4/16.
//

#include <zookeeper.h>
#include "zkwrapper.h"
#include <string.h>


int init = 0;
zhandle_t *zh;
clientid_t myid;
/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {

}



ZKWrapper::ZKWrapper(std::string host) {
    zh = zookeeper_init(host.c_str(), watcher, 10000, 0, 0, 0);
    if (!zh) {
	fprintf(stderr, "zk init failed!");
        exit(1);
    }
    if (zh == NULL)
	fprintf(stderr, "zk is null");	
        
    //zh = handle;
    init = 1;

}


int ZKWrapper::create(std::string path, std::string data, int num_bytes) {
    if (!init) {
	fprintf(stderr, "Attempt to create before init!");
        exit(1); // Error handle
    }
    int rc = zoo_create(zh, (const char *)path.c_str(), data.c_str(), num_bytes, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if (rc != 0) {
	fprintf(stderr,"error %d in zoo_create\n", rc);
        if (rc = -110)
		fprintf(stderr,"Node %s already exists.\n", path.c_str()); // TODO: add more error code checking
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


