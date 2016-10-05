//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
// Modified by Joe on 10/5/16
//

#ifndef RDFS_ZKWRAPPER_H
#define RDFS_ZKWRAPPER_H

#include <string>
#include <zookeeper.h>

class ZKWrapper {
    public:
        ZKWrapper(std::string host);
 	int create(std::string path, std::string data, int num_bytes);
        int exists(std::string path);
        int delete_node(std::string path);
	int get_children(std::string path);
	std::string get(std::string path);
        void close();

    private:
        zhandle_t *zh;
        friend void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);


};

#endif //RDFS_ZKWRAPPER_H
