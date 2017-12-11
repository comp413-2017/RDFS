#!/usr/bin/env bash

set -ex

HADOOP2_MIRROR=http://kevinlin.web.rice.edu/static/hadoop-2.8.1.tar.gz
HADOOP3_MIRROR=http://kevinlin.web.rice.edu/static/hadoop-3.0.0-beta1-2.tar.gz

cd $HOME
if [ ! -d /tmp/cache/hadoop3 ]; then
    wget --quiet -O hadoop.tar.gz $HADOOP3_MIRROR
    tar -xf hadoop.tar.gz -C /tmp
    mv /tmp/hadoop-3.0.0-beta1 /tmp/cache/hadoop3
fi
if [ ! -d /tmp/cache/hadoop2 ]; then
    wget --quiet -O hadoop.tar.gz $HADOOP2_MIRROR
    tar -xf hadoop.tar.gz -C /tmp
    mv /tmp/hadoop-2.8.1 /tmp/cache/hadoop2
fi
ln -s /tmp/cache/hadoop3 hadoop
ln -s /tmp/cache/hadoop2 hadoop-legacy
cp $TRAVIS_BUILD_DIR/config/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
cp $TRAVIS_BUILD_DIR/config/core-site.xml hadoop/etc/hadoop/core-site.xml
