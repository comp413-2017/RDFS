#!/usr/bin/env bash

if [ -z $1 ]; then
    echo 'Must specify port to sniff on!'
    exit 1
fi

echo "TCP Packet Sniffer: port $1"
echo "============================="
echo ""

# Print to standard output sniffed packet contents on any interface at the specified port
sudo tcpflow -c -i any port "$1"
