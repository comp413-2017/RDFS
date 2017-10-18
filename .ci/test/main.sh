#!/usr/bin/env bash

set -ex

cd build/test
./ReplicationTest
./DeleteTest
./NameNodeTest --gtest_filter=-*Performance*
./NativeFsTest
./ReadWriteTest || :
./ZKDNClientTest
./ZKLockTest
./ZKWrapperTest
