#include <zookeeper.h> // TODO: this should be removed
#include <string>
#include <zkwrapper.h>
#include <iostream>

/** Watcher function -- empty for this example, not something you should
 * do in real code */
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx) {

}

int main(int argc, char* argv[]) {
//    int res0 = zk.init("localhost:2181");
    zhandle_t *zh = zookeeper_init("localhost:2181", watcher, 10000, 0, 0, 0);
    if (!zh) {
        exit(1);
    }
    ZKWrapper zk(zh);
    int res1 = zk.create("/xyz", "Hello from /xyz", 15);
    std::string res2 = zk.get("/xyz");
    std::cout << "Created Znode with value: " << res2 << std::endl;
}

