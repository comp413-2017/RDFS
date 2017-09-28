#!/usr/bin/env bash

set -ex
if [ ! -d "/home/vagrant/rdfs" ]; then
  mkdir /home/vagrant/rdfs/
fi
if [ ! -d "/home/vagrant/rdfs/test" ]; then
  mkdir /home/vagrant/rdfs/test/
fi
if [ ! -d "/home/vagrant/rdfs/test/integration" ]; then
  mkdir /home/vagrant/rdfs/test/integration/
fi

echo 'for i in range(0, 500000):
    print "mango" + str(i)' > /home/vagrant/rdfs/test/integration/generate_file.py

mkdir build
cd build
cmake ..
make
