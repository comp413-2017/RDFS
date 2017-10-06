#!/usr/bin/env bash

set -ex

# Add deploy keys to environment
mkdir -p ~/.ssh
openssl aes-256-cbc -K $encrypted_493c05fcd547_key -iv $encrypted_493c05fcd547_iv -in .ci/keys/deploy_id_rsa.enc -out ~/.ssh/id_rsa -d
chmod 600 ~/.ssh/id_rsa

# Install linter
sudo pip install cpplint==1.3.0

if [ ! -d "/home/vagrant/rdfs" ]; then
 mkdir /home/vagrant/rdfs/
fi
if [ ! -d "/home/vagrant/rdfs/test" ]; then
 mkdir /home/vagrant/rdfs/test/
fi
if [ ! -d "/home/vagrant/rdfs/test/integration" ]; then
  mkdir /home/vagrant/rdfs/test/integration/
fi

# Create build folders and compile
mkdir build
cd build
cmake ..
make
