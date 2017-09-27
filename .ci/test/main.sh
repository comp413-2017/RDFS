#!/usr/bin/env bash

set -ex

cd build/test
./DeleteTest || :
./NameNodeTest || :
./NativeFsTest || :
./ReadWriteTest || :
./ZKDNClientTest || :
./ZKLockTest || :
./ZKWrapperTest || :
