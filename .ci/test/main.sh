#!/usr/bin/env bash

set -ex

cd build/test
./ReplicationTest
./DeleteTest
./NameNodeTest
./NativeFsTest
./ZKDNClientTest
./ZKLockTest
./ZKWrapperTest
./ReadWriteTest
