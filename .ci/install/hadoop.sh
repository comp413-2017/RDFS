#!/usr/bin/env bash

set -ex

HADOOP_MIRROR=http://kevinlin.web.rice.edu/static/hadoop-2.8.1.tar.gz

cd $HOME
if [ ! -d /tmp/cache/hadoop ]; then
    wget --quiet -O hadoop.tar.gz $HADOOP_MIRROR
    tar -xf hadoop.tar.gz -C /tmp
    mv /tmp/hadoop-2.8.1 /tmp/cache/hadoop
fi
ln -s /tmp/cache/hadoop hadoop
cp $TRAVIS_BUILD_DIR/config/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
cp $TRAVIS_BUILD_DIR/config/core-site.xml hadoop/etc/hadoop/core-site.xml
