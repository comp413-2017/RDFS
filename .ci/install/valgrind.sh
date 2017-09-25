#!/usr/bin/env bash

set -ex

VALGRIND_MIRROR=http://valgrind.org/downloads/valgrind-3.11.0.tar.bz2

cd $HOME
mkdir valgrindtemp
cd valgrindtemp
wget --quiet -O valgrind.tar.bz2 $VALGRIND_MIRROR
tar -xf valgrind.tar.bz2
cd valgrind-3.11.0
./configure --prefix=/usr
sudo make > /dev/null
sudo make install
cd ../..
sudo rm -rf valgrindtemp
