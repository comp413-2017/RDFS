#!/usr/bin/env bash

set -ex

IGNORED_RULES=( build/include_subdir build/c++11 runtime/references )
SOURCE_DIRECTORIES=( native-filesystem rice-datanode rice-namenode )

# Build `filter` argument for a list of ignored rules
rule_filter=""
for rule in "${IGNORED_RULES[@]}"
do
    rule_filter=-${rule},${rule_filter}
done

# Execute cpplint on all source directories
for directory in "${SOURCE_DIRECTORIES[@]}"
do
    # TODO(LINKIWI): Don't force exit 0 after all lint errors have actually been fixed
    cpplint \
        --filter=${rule_filter} \
        --recursive \
        ${directory} || :
done
