#!/usr/bin/env bash

set -ex

PROTOBUF_CACHE=http://kevinlin.web.rice.edu/static/protobuf.zip

cd $HOME
if [ ! -d /tmp/cache/protobuf ]; then
    wget --quiet -O protobuf.zip $PROTOBUF_CACHE
    unzip protobuf.zip > /dev/null
    mv protobuf-3.0.0 /tmp/cache/protobuf
fi
ln -s /tmp/cache/protobuf protobuf-3.0.0
cd protobuf-3.0.0
sudo make install > /dev/null
sudo ldconfig
