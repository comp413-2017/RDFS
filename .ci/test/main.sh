#!/usr/bin/env bash

set -ex

cd build/test
./ReplicationTest
./ReadWriteTest
./DeleteTest
./NameNodeTest
./NativeFsTest
./ZKDNClientTest
./ZKLockTest
./ZKWrapperTest
