#!/usr/bin/env bash

set -x

run_flaky_test() {
    for i in {1..3}
    do
        $1
        if [ $? == 0 ] 
	    then
		    return 0
	    fi
    done

    return 1
}

cd build/test
./ReplicationTest
./DeleteTest
run_flaky_test ./NameNodeTest
./NativeFsTest
run_flaky_test ./ZKDNClientTest
./ZKLockTest
run_flaky_test ./ZKWrapperTest
./ReadWriteTest
./StorageTest