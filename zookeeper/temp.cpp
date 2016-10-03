//
// Created by Nicholas Kwon on 9/29/16.
//

#include "zk_wrapper.hpp"

int main(int argc, char * argv[]) {
    init("localhost:2181");
    create_znode("/xyz", "value", 5);
    get_znode_value("/xyz");

    return 0;
}



