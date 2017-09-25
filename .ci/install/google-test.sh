#!/usr/bin/env bash

set -ex

GOOGLETEST_MIRROR=https://github.com/google/googletest/archive/release-1.7.0.tar.gz

cd $HOME
wget --quiet -O googletest.tar.gz $GOOGLETEST_MIRROR
tar -xf googletest.tar.gz
rm googletest.tar.gz
sudo mv googletest-release-1.7.0 /usr/src/gtest
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make > /dev/null
sudo cp -a include/gtest /usr/include
sudo cp *.a /usr/lib
sudo ldconfig
