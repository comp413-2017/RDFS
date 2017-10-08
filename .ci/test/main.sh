#!/usr/bin/env bash

set -ex

cd build/test
./ReadWriteTest
./ReplicationTest
./DeleteTest
./NameNodeTest
./NativeFsTest
./ZKDNClientTest
./ZKLockTest
./ZKWrapperTest
