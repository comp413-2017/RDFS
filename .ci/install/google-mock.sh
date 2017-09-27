#!/usr/bin/env bash

set -ex

GOOGLEMOCK_MIRROR=https://github.com/google/googlemock/archive/release-1.7.0.tar.gz

cd $HOME
wget --quiet -O googlemock.tar.gz $GOOGLEMOCK_MIRROR
tar -xf googlemock.tar.gz
rm googlemock.tar.gz
sudo mv googlemock-release-1.7.0 /usr/src/gmock
cd /usr/src/gmock
sudo cmake CMakeLists.txt
sudo make > /dev/null
sudo cp -a include/gmock /usr/include
sudo cp *.a /usr/lib
sudo ldconfig
