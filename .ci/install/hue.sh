#!/usr/bin/env bash

set -ex

cd $HOME
git clone https://github.com/cloudera/hue.git
cd hue
make apps > /dev/null
