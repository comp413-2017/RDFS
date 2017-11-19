#!/usr/bin/env bash

set -ex

ISAL_MIRROR=http://kevinlin.web.rice.edu/static/isal-2.tar.gz

cd $HOME
if [ -d /tmp/cache/isal ]; then
    rm -rf /tmp/cache/isal
fi
wget --quiet -O isal.tar.gz $ISAL_MIRROR
tar -xf isal.tar.gz -C /tmp
mv /tmp/isa-l_open_src_2.13 /tmp/cache/isal
cd /tmp/cache/isal
make && make install
cd $HOME
ln -s /tmp/cache/isal isal
