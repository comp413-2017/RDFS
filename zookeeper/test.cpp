#include <string>
#include <zkwrapper.h>
#include <iostream>


int main(int argc, char* argv[]) {
    /**
     * TODO: Need to find a better way to do this. Ideally, calling init should
     * be in the constructor of ZKWrapper, but it caused errors. Will fix later.
     *
     * Joe's comment: I fixed this issue by declaring watcher as a friend function of ZKWrapper.
     *                
     */
    //zhandle_t *zh = zookeeper_init("localhost:2181", watcher, 10000, 0, 0, 0);
    //if (!zh) {
    //    exit(1);
    //}
    ZKWrapper zk("localhost:2181");
    char path[] = "/testing";
    int rc = zk.exists(path);
    if (rc == 0){
        fprintf(stdout, "path %s exists\n", path);
        zk.delete_node(path);
    }
    else {
        fprintf(stdout, "path %s does not exist\n", path);
    }
    int res1 = zk.create(path, "Hello", 5);
    std::string res2 = zk.get(path);
    std::cout << "Created Znode with value: " << res2 << std::endl;
}

