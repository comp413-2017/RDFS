#!/usr/bin/env bash

set -ex

IGNORED_RULES=( build/include_subdir build/c++11 runtime/references build/include )
SOURCE_DIRECTORIES=( native-filesystem rice-datanode rice-namenode rpcserver test zkwrapper zookeeper)

# Build `filter` argument for a list of ignored rules
rule_filter=""
for rule in "${IGNORED_RULES[@]}"
do
    rule_filter=-${rule},${rule_filter}
done

# Execute cpplint on all source directories
for directory in "${SOURCE_DIRECTORIES[@]}"
do
    cpplint \
        --filter=${rule_filter} \
        --recursive \
        ${directory}
done
