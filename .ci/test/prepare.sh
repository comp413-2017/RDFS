#!/usr/bin/env bash

set -ex

mkdir build
cd build
cmake ..
make
