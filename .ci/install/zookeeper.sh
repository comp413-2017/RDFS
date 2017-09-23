#!/usr/bin/env bash

set -ex

ZOOKEEPER_MIRROR=http://mirror.reverse.net/pub/apache/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz

cd $HOME
wget --quiet -O zookeeper.tar.gz $ZOOKEEPER_MIRROR
tar -xf zookeeper.tar.gz
rm zookeeper.tar.gz
mv zookeeper-3.4.9 zookeeper
cat > zookeeper/conf/zoo.cfg <<EOF
tickTime=2000
dataDir=/var/zookeeper
clientPort=2181
EOF

# Client libraries
cd zookeeper
ant compile_jute
cd src/c
autoreconf -if
./configure
sudo make > /dev/null
sudo make install
