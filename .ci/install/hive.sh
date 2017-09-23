#!/usr/bin/env bash

set -ex

HIVE_MIRROR=http://apache.claz.org/hive/hive-2.1.1/apache-hive-2.1.1-bin.tar.gz

cd $HOME
wget --quiet -O hive.tar.gz $HIVE_MIRROR
tar -xf hive.tar.gz
rm hive.tar.gz
mv apache-hive-2.1.1-bin hive
