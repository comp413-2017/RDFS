#!/usr/bin/env bash

set -ex

mkdir /home/vagrant/
mkdir /home/vagrant/rdfs/
mkdir /home/vagrant/rdfs/test/
mkdir /home/vagrant/rdfs/test/integration/
echo 'for i in range(0, 500000):
    print "mango" + str(i)' > /home/vagrant/rdfs/test/integration/generate_file.py

mkdir build
cd build
cmake ..
make
