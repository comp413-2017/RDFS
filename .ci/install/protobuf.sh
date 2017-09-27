#!/usr/bin/env bash

set -ex

PROTOBUF_MIRROR=https://github.com/google/protobuf/releases/download/v3.0.0/protobuf-cpp-3.0.0.tar.gz

cd $HOME
wget --quiet -O protobuf.tar.gz $PROTOBUF_MIRROR
tar -xf protobuf.tar.gz
rm protobuf.tar.gz
cd protobuf-3.0.0
./autogen.sh
./configure --prefix=/usr
sudo make > /dev/null
sudo make install
sudo ldconfig
