#!/usr/bin/env bash

set -ex

cd build/test
./NameNodeTest || :
./NativeFsTest || :
./ReadWriteTest || :
./ZKDNClientTest || :
./ZKLockTest || :
./ZKWrapperTest || :
