#!/usr/bin/env bash

set -ex

# Add deploy keys to environment
mkdir -p ~/.ssh
openssl aes-256-cbc -K $encrypted_493c05fcd547_key -iv $encrypted_493c05fcd547_iv -in .ci/keys/deploy_id_rsa.enc -out ~/.ssh/id_rsa -d
chmod 600 ~/.ssh/id_rsa

# Install linter
sudo pip install cpplint==1.3.0

# Mimic the file system structure in the development environment
sudo ln -s $HOME /home/vagrant
sudo ln -s $TRAVIS_BUILD_DIR $HOME/rdfs

# Create build folders and compile
mkdir build
cd build
cmake ..
make
