#!/usr/bin/env bash

set -ex

HADOOP_MIRROR=http://kevinlin.web.rice.edu/static/hadoop-2.8.1.tar.gz

cd $HOME
wget --quiet -O hadoop.tar.gz $HADOOP_MIRROR
tar -xf hadoop.tar.gz
rm hadoop.tar.gz
mv hadoop-2.8.1 hadoop
cp $TRAVIS_BUILD_DIR/config/hdfs-site.xml hadoop/etc/hadoop/hdfs-site.xml
cp $TRAVIS_BUILD_DIR/config/core-site.xml hadoop/etc/hadoop/core-site.xml
