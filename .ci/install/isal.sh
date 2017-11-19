#!/usr/bin/env bash

set -ex

ISAL_MIRROR=http://kevinlin.web.rice.edu/static/isal-2.tar.gz

cd $HOME
if [ -d /tmp/cache/isal ]; then
    cd /tmp/cache/isal
    make clean
    cd $HOME
    rm -rf /tmp/cache/isal
fi
wget --quiet -O isal.tar.gz $ISAL_MIRROR
tar -xf isal.tar.gz -C /tmp
mv /tmp/isal /tmp/cache/isal
cd /tmp/cache/isal
./autogen.sh
./configure
make
sudo make install
cd $HOME
ln -s /tmp/cache/isal isal
