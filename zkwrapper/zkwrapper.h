//
// Created by Prudhvi Boyapalli on 10/3/16.
//

#ifndef RDFS_ZKWRAPPER_H
#define RDFS_ZKWRAPPER_H

#include <string>

class ZKWrapper {
    public:
        ZKWrapper(zhandle_t *handle);
        int create(std::string path, std::string data, int num_bytes);
        std::string get(std::string path);
        void close();

    private:
        void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
};

#endif //RDFS_ZKWRAPPER_H
