#!/usr/bin/env bash

set -ex

# Mimic the file system structure in the development environment
sudo ln -s $HOME /home/vagrant
sudo ln -s $TRAVIS_BUILD_DIR $HOME/rdfs

mkdir build
cd build
cmake ..
make
