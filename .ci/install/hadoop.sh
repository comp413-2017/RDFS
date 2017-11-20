#!/usr/bin/env bash

set -ex

HADOOP_MIRROR=http://kevinlin.web.rice.edu/static/hadoop-3.0.0-beta1-2.tar.gz

cd $HOME
if [ -d /tmp/cache/hadoop ]; then
    rm -rf /tmp/cache/hadoop
fi
wget --quiet -O hadoop.tar.gz $HADOOP_MIRROR
tar -xf hadoop.tar.gz -C /tmp
mv /tmp/hadoop-3.0.0-beta1 /tmp/cache/hadoop
ln -s /tmp/cache/hadoop hadoop
cp $TRAVIS_BUILD_DIR/config/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
cp $TRAVIS_BUILD_DIR/config/core-site.xml hadoop/etc/hadoop/core-site.xml
