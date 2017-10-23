#!/usr/bin/env bash

set -ex

PROTOBUF_CACHE=http://kevinlin.web.rice.edu/static/protobuf.zip

cd $HOME
if [ ! -f /tmp/cache/protobuf.zip ]; then
    wget --quiet -O /tmp/cache/protobuf.zip $PROTOBUF_CACHE
fi
unzip /tmp/cache/protobuf.zip > /dev/null
cd protobuf-3.0.0
sudo make install > /dev/null
sudo ldconfig
