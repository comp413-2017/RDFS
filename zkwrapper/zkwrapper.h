//
// Created by Prudhvi Boyapalli on 10/3/16.
// 
// Modified by Joe on 10/5/16
//

#ifndef RDFS_ZKWRAPPER_H
#define RDFS_ZKWRAPPER_H

#include <string>
#include <vector>
#include <zookeeper.h>

class ZKWrapper {
public:
    ZKWrapper(std::string host);

    int create(const std::string &path, const std::string &data, const int num_bytes, int flag = 0) const;

    int recursiveCreate(const std::string &path, const std::string &data, const int num_bytes) const;

    int exists(const std::string &path) const;

    int delete_node(const std::string &path) const;

    std::vector <std::string> get_children(const std::string &path, const int watch) const;

    std::string get(const std::string &path) const;

    void close();

private:
    zhandle_t *zh;

    friend void watcher(zhandle_t *zzh, int type, int state, const char *path,
                        void *watcherCtx);
};

#endif //RDFS_ZKWRAPPER_H
