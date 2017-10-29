#!/usr/bin/env bash

set -x

run_test() {
    for i in {1..3}
    do
        $1
        if [ $? == 0 ] 
	    then
		    return 0
	    fi
    done

    exit 1
}

cd build/test
run_test ./ErasureCodeTest
run_test ./ReadWriteTest
run_test ./ReplicationTest
run_test ./DeleteTest
run_test ./NameNodeTest
run_test ./NativeFsTest
run_test ./StorageTest
run_test ./ZKDNClientTest
run_test ./ZKLockTest
run_test ./ZKWrapperTest
