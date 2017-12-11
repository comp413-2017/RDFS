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
# TODO(LINKIWI): investigate failure root cause
# run_test ./ReadWriteTest
run_test ./DeleteTest
run_test "./NameNodeTest --gtest_filter=-*Performance*"
run_test ./NativeFsTest
run_test "./StorageTest --gtest_filter=-*Time"
run_test ./ZKDNClientTest
run_test ./ZKLockTest
run_test ./ZKWrapperTest
run_test ./UsernameTest
run_test ./LeaseTest
run_test ./AppendTestMJP
# PATH=/home/vagrant/hadoop-legacy/bin:$PATH run_test ./AppendFileTest
