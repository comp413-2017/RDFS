#!/bin/sh
# Provisioning the vagrant box for development on RDFS

set -e
set -x

apt-get update
# clean out redundant packages from vagrant base image
apt-get autoremove -y

# Install some basics
apt-get install -y language-pack-en zip unzip curl

apt-get install -y git build-essential cmake automake autoconf libtool libboost-all-dev libasio-dev

wget --quiet https://github.com/google/protobuf/releases/download/v3.0.0/protobuf-cpp-3.0.0.tar.gz
tar -xf protobuf-cpp-3.0.0.tar.gz
rm protobuf-cpp-3.0.0.tar.gz
cd protobuf-3.0.0; ./autogen.sh && ./configure --prefix=/usr && make && make install
cd /home/vagrant/; ldconfig

# Install and setup dependencies of hadoop
apt-get install -y ssh pdsh openjdk-8-jdk-headless
# passphraseless ssh
#ssh-keygen -b 2048 -t rsa -f /home/vagrant/.ssh/id_rsa -N ""
#cp /home/vagrant/.ssh/id_rsa.pub /home/vagrant/.ssh/authorized_keys

# Setup Apache hadoop for pseudo-distributed usage
wget --quiet http://kevinlin.web.rice.edu/static/hadoop-2.8.1.tar.gz
tar -xf hadoop-2.8.1.tar.gz
mv hadoop-2.8.1 /home/vagrant/hadoop3
rm hadoop-2.8.1.tar.gz
ln -s hadoop3 hadoop
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /home/vagrant/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /home/vagrant/hadoop/etc/hadoop/hadoop-env.sh

# add custom config files to Hadoop
cat /home/vagrant/rdfs/config/hdfs-site.xml > /home/vagrant/hadoop/etc/hadoop/hdfs-site.xml
cat /home/vagrant/rdfs/config/core-site.xml > /home/vagrant/hadoop/etc/hadoop/core-site.xml

# add hadoop to path
echo 'export PATH=/home/vagrant/hadoop/bin:$PATH' >> /home/vagrant/.bashrc

# add hadoop to classpath
echo 'export CLASSPATH=/home/vagrant/hadoop/share/hadoop/hdfs/*:/home/vagrant/hadoop/share/hadoop/common/*' >> /home/vagrant/.bashrc


# Download hadoop 2.7.4 as well, but do not set as default.
wget --quiet http://mirror.cc.columbia.edu/pub/software/apache/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
tar -xf hadoop-2.7.4.tar.gz
mv hadoop-2.7.4 /home/vagrant/hadoop2
rm hadoop-2.7.4.tar.gz
cp /home/vagrant/hadoop3/etc/hadoop/core-site.xml /home/vagrant/hadoop2/etc/hadoop/core-site.xml
cp /home/vagrant/hadoop3/etc/hadoop/hdfs-site.xml /home/vagrant/hadoop2/etc/hadoop/hdfs-site.xml



# Setup Apache zookeeper
wget --quiet http://mirror.reverse.net/pub/apache/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz
tar -xf zookeeper-3.4.9.tar.gz
mv zookeeper-3.4.9 /home/vagrant/zookeeper
rm zookeeper-3.4.9.tar.gz
cat > /home/vagrant/zookeeper/conf/zoo.cfg <<EOF
tickTime=2000
dataDir=/var/zookeeper
clientPort=2181
EOF

# Setup Apache Hive
wget --quiet http://apache.claz.org/hive/hive-2.1.1/apache-hive-2.1.1-bin.tar.gz
tar -xf apache-hive-2.1.1-bin.tar.gz
mv apache-hive-2.1.1-bin /home/vagrant/hive
rm apache-hive-2.1.1-bin.tar.gz
echo 'export HIVE_HOME=/home/vagrant/hive' >> /home/vagrant/.bashrc
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> /home/vagrant/.bashrc

# Setup mysql.
export DEBIAN_FRONTEND=noninteractive
apt-get --assume-yes -q install libmysqlclient-dev libmysql-java mysql-server sysv-rc-conf

cat > /etc/mysql/my.cnf <<EOF
[mysqld]
datadir=/var/lib/mysql
socket=/var/lib/mysql/mysql.sock
bind-address=127.0.0.1
default-storage-engine=InnoDB
sql_mode=STRICT_ALL_TABLES
EOF
service mysql start
sysv-rc-conf mysql on

# Setup Apache Hue
apt-get --assume-yes install maven libkrb5-dev libmysqlclient-dev libssl-dev libsasl2-dev libsasl2-modules-gssapi-mit libsqlite3-dev libtidy-0.99-0 libxml2-dev libxslt-dev libldap2-dev maven python-setuptools libgmp3-dev libffi-dev
git clone https://github.com/cloudera/hue.git
cd /home/vagrant/hue
make apps
cd /home/vagrant
echo 'export PATH=/home/vagrant/hue/build/env/bin:$PATH' >> /home/vagrant/.bashrc

# Set up the ZooKeeper client libraries
apt-get --assume-yes install ant
cd /home/vagrant/zookeeper
ant compile_jute
cd /home/vagrant/zookeeper/src/c
apt-get --assume-yes install autoconf
apt-get --assume-yes install libcppunit-dev
apt-get --assume-yes install libtool
autoreconf -if
./configure
make && make install

# Add Google Mock
apt-get install -y google-mock
cd /usr/src/gmock
cmake CMakeLists.txt
make
cp *.a /usr/lib

# Add Google Test
apt-get install -y libgtest-dev
cd /usr/src/gtest
cmake CMakeLists.txt
make
cp *.a /usr/lib
cd /home/vagrant

# Add Valgrind
sudo apt-get install -y libc6-dbg
mkdir valgrindtemp
cd valgrindtemp
wget --quiet http://valgrind.org/downloads/valgrind-3.11.0.tar.bz2
tar -xf valgrind-3.11.0.tar.bz2
cd valgrind-3.11.0
./configure --prefix=/usr && sudo make && sudo make install
cd ../..
rm -r valgrindtemp

# Download demo tables.
mkdir /home/vagrant/demo_script
cd /home/vagrant/demo_script
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/country.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/population.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/student.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/csv_generator.py
cd /home/vagrant


# Put everything under /home/vagrant and /home/vagrant/.ssh.
chown -R vagrant:vagrant /home/vagrant/*
chown -R vagrant:vagrant /home/vagrant/.ssh/*
# Allow us to write to /dev/sdb.
echo 'sudo chown vagrant:vagrant /dev/sdb' >> /home/vagrant/.bashrc

# add diff detector to path
echo 'python /home/vagrant/rdfs/utility/provision_diff.py' >> /home/vagrant/.bashrc
