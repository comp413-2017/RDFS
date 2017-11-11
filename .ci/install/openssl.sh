#!/usr/bin/env bash

set -ex

OPENSSL_MIRROR=https://github.com/openssl/openssl/archive/OpenSSL_1_0_2g.tar.gz

cd $HOME
wget --quiet -O openssl.tar.gz $OPENSSL_MIRROR
sudo ln -sf /usr/local/ssl/bin/openssl $(which openssl)
tar -xf openssl.tar.gz
rm openssl.tar.gz
cd openssl-OpenSSL_1_0_2g
sudo ./config > /dev/null
sudo make > /dev/null
sudo make install > /dev/null
sudo ldconfig > /dev/null
