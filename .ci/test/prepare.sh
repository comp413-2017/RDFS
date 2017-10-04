#!/usr/bin/env bash

set -ex
if [ ! -d "/home/vagrant/rdfs" ]; then
  mkdir /home/vagrant/rdfs/
fi
if [ ! -d "/home/vagrant/rdfs/test" ]; then
  mkdir /home/vagrant/rdfs/test/
fi
if [ ! -d "/home/vagrant/rdfs/test/integration" ]; then
  mkdir /home/vagrant/rdfs/test/integration/
fi

echo 'for i in range(0, 500000):
    print "mango" + str(i)' > /home/vagrant/rdfs/test/integration/generate_file.py

# Add deploy keys to environment
mkdir -p ~/.ssh
openssl aes-256-cbc -K $encrypted_493c05fcd547_key -iv $encrypted_493c05fcd547_iv -in .ci/keys/deploy_id_rsa.enc -out ~/.ssh/id_rsa -d
chmod 600 ~/.ssh/id_rsa

mkdir build
cd build
cmake ..
make
