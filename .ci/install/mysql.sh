#!/usr/bin/env bash

set -ex

sudo bash -c 'cat > /etc/mysql/my.cnf' <<EOF
[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
bind-address=127.0.0.1
default-storage-engine=InnoDB
sql_mode=STRICT_ALL_TABLES
EOF
