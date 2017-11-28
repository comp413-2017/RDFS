#!/usr/bin/env bash

set -ex

ISAL_MIRROR=http://kevinlin.web.rice.edu/static/isal-2.tar.gz

cd $HOME
wget --quiet -O isal.tar.gz $ISAL_MIRROR
tar -xf isal.tar.gz
cd $HOME/isal
./autogen.sh > /dev/null
./configure > /dev/null
make > /dev/null
sudo make install > /dev/null
cd $HOME
