#include <string>
#include <zkwrapper.h>
#include <iostream>
#include <cassert>
#include <algorithm>

int preTest(ZKWrapper zk) {
    zk.recursive_delete("/testing/");
    assert(zk.get_children("/testing", 0).size() == 0);
}

int testDeleteAll(ZKWrapper zk) {

    preTest(zk);
    zk.create("/testing/child1", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child1/child3", ZKWrapper::EMPTY_VECTOR);

    zk.recursive_delete("/testing/child1");
    assert(zk.exists("/testing/child1", 0));

    zk.create("/testing/child1", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child2", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child1/child1", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child1/child2", ZKWrapper::EMPTY_VECTOR);
    zk.create("/testing/child2/child1", ZKWrapper::EMPTY_VECTOR);

    zk.recursive_delete("/testing/");
    assert(zk.get_children("/testing", 0).size() == 0);
}

int testMultiOp(ZKWrapper zk) {

    preTest(zk);

    auto hello_vec = ZKWrapper::get_byte_vector("hello");
    auto jello_vec = ZKWrapper::get_byte_vector("jello");
    auto bye_vec = ZKWrapper::get_byte_vector("bye");
    auto op = zk.build_create_op("/testing/child1", hello_vec);
    auto op2 = zk.build_create_op("/testing/child2", jello_vec);
    auto op3 = zk.build_create_op("/testing/toDelete", bye_vec);

    auto operations = std::vector<std::shared_ptr<ZooOp>>();

    operations.push_back(op);
    operations.push_back(op2);
    operations.push_back(op3);

    std::vector<zoo_op_result> results = std::vector<zoo_op_result>();
    zk.execute_multi(operations, results);

    assert(hello_vec == zk.get("/testing/child1", 0));
    assert(jello_vec == zk.get("/testing/child2", 0));
    assert(bye_vec == zk.get("/testing/toDelete", 0));

    auto nhello_vec = ZKWrapper::get_byte_vector("new_hello");
    auto njello_vec = ZKWrapper::get_byte_vector("new_jello");
    auto op4 = zk.build_set_op("/testing/child1", nhello_vec);
    auto op5 = zk.build_set_op("/testing/child2", njello_vec);
    auto op6 = zk.build_delete_op("/testing/toDelete");

    operations = std::vector<std::shared_ptr<ZooOp>>();

    operations.push_back(op4);
    operations.push_back(op5);
    operations.push_back(op6);

    zk.execute_multi(operations, results);

    assert(nhello_vec == zk.get("/testing/child1", 0));
    assert(njello_vec == zk.get("/testing/child2", 0));
    assert(zk.exists("/toDelete", 0));
}

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
    std::vector<std::uint8_t> vec;
    if (zk.exists("/testing", 0)) {
        zk.create("/testing", vec);
    }

    testDeleteAll(zk);
    testMultiOp(zk);

    // zk.recursive_delete("/");
    /*
    zk.create("/testing", "hello", 5);
    zk.create("/testing/child", "world", 5);
    zk.get_children("/testing", 1);
    zk.delete_node("/testing/child");
    zk.delete_node("/testing");
    */

    /*

    */



    //TODO
    //I could not fix the linking errors when calling my functions
    // struct Stat st;
    // int rc = zk.add_watcher_exists(path, (char *)"context for watcher", &st);
    // if (ZOK != rc){                                                     
    //     printf("Problems  %d\n", rc);                                   
    // }

//    int rc = zk.exists(path);
//    if (rc == 0){
//        fprintf(stdout, "path %s exists\n", path);
//        zk.delete_node(path);
//    }
//    else {
//        fprintf(stdout, "path %s does not exist\n", path);
//    }

    //TODO
    //I need to fix the linking errors when calling my watcher functions
    // struct String_vector str;
    // rc = zk.add_watcher_getchildren(path, (char *)"context  for watcher", &str);
    // if (ZOK != rc){                                          
    //     printf("Problems  %d\n", rc);                        
    // } else {                                                 
    //     int i = 0;                                           
    //     while (i < str.count) {                              
    //         printf("Children %s\n", str.data[i++]);          
    //     }                                                    
    //     if (str.count) {                                     
    //         deallocate_String_vector(&str);                  
    //     }                                                    
    // } 

//    int res1 = zk.create(path, "Hello", 5);
//    std::string res2 = zk.get(path);
//    std::cout << "Created Znode with value: " << res2 << std::endl;
//    int res3 = zk.recursiveCreate("/testing/testing1/testing11/testing111", "Nothing", 7);
//    std::string res4 = zk.get("/testing/testing1/testing11/testing111");
//    std::cout << "Recursively created Znode at /testing/testing1/testing11/testing111 with value: " << res4 << std::endl;
//    zk.delete_node("/testing/testing1/testing11/testing111");
//    zk.delete_node("/testing/testing1/testing11");
//
//    // Simple test of get_children(...)
//    std::string testing2 = "/testing/testing2";
//    std::string testing3 = "/testing/testing3";
//    zk.create(testing2, "value of /testing/testing2", 26);
//    zk.create(testing3, "value of /testing/testing3", 26);
//    std::vector<std::string> res5 = zk.get_children("/testing");
//    std::cout << "The children of /testing are:" << std::endl;
//    int i;
//    for (i=0; i < res5.size(); i ++) {
//        std::cout << "\t" << res5[i] << std::endl;
//    }
//    zk.delete_node(testing3);
//    zk.delete_node(testing2);
//    zk.delete_node("/testing/testing1");
//    zk.delete_node("/testing");

}



