#!/usr/bin/env bash

set -ex

# Mimic the file system structure in the development environment
sudo ln -s $HOME /home/vagrant
sudo ln -s $TRAVIS_BUILD_DIR $HOME/rdfs

# Add deploy keys to environment
mkdir -p ~/.ssh
openssl aes-256-cbc -K $encrypted_493c05fcd547_key -iv $encrypted_493c05fcd547_iv -in .ci/keys/deploy_id_rsa.enc -out ~/.ssh/id_rsa -d
chmod 600 ~/.ssh/id_rsa

# Add SSL root CA to environment
sudo cp ~/rdfs/config/keys/server.crt /usr/local/share/ca-certificates/
sudo update-ca-certificates

# Install linter
sudo pip install cpplint==1.3.0

# Create build folders and compile
mkdir build
cd build
cmake -DOPENSSL_ROOT_DIR=/usr/local/ssl -DOPENSSL_LIBRARIES=/usr/local/ssl/lib ..
make
