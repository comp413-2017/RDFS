#!/bin/sh
# Provisioning the vagrant box for development on RDFS

set -e
set -x

apt-get update
# clean out redundant packages from vagrant base image
apt-get autoremove -y

# Install and setup dependencies of hadoop
apt-get install -y ssh pdsh openjdk-8-jdk-headless
# passphraseless ssh
#ssh-keygen -b 2048 -t rsa -f /home/vagrant/.ssh/id_rsa -N ""
#cp /home/vagrant/.ssh/id_rsa.pub /home/vagrant/.ssh/authorized_keys

# Setup Apache hadoop for pseudo-distributed usage
wget --quiet http://mirror.olnevhost.net/pub/apache/hadoop/common/hadoop-3.0.0-alpha1/hadoop-3.0.0-alpha1.tar.gz
tar -xf hadoop-3.0.0-alpha1.tar.gz
mv hadoop-3.0.0-alpha1 /home/ubuntu/hadoop3
rm hadoop-3.0.0-alpha1.tar.gz
ln -s hadoop3 hadoop
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /home/ubuntu/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre' >> /home/ubuntu/hadoop/etc/hadoop/hadoop-env.sh
cat > /home/ubuntu/hadoop3/etc/hadoop/core-site.xml <<EOF
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:5351</value>
    </property>
</configuration>
EOF

cat > /home/ubuntu/hadoop3/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <name>dfs.name.dir</name>
        <value>/home/ubuntu/hadoop/cache/dfs/name</value>
    </property>
</configuration>
EOF

# add custom config files to Hadoop
cat /home/ubuntu/rdfs/config/hdfs-site.xml > /home/ubuntu/hadoop/etc/hadoop/hdfs-site.xml
cat /home/ubuntu/rdfs/config/core-site.xml > /home/ubuntu/hadoop/etc/hadoop/core-site.xml

# add hadoop to path
echo 'export PATH=/home/ubuntu/hadoop/bin:$PATH' >> /home/ubuntu/.bashrc

# add hadoop to classpath
echo 'export CLASSPATH=/home/ubuntu/hadoop/share/hadoop/hdfs/*:/home/ubuntu/hadoop/share/hadoop/common/*' >> /home/ubuntu/.bashrc


# Download hadoop 2.7.3 as well, but do not set as default.
wget --quiet http://mirror.cc.columbia.edu/pub/software/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xf hadoop-2.7.3.tar.gz
mv hadoop-2.7.3 /home/ubuntu/hadoop2
rm hadoop-2.7.3.tar.gz
cp /home/ubuntu/hadoop3/etc/hadoop/core-site.xml /home/ubuntu/hadoop2/etc/hadoop/core-site.xml
cp /home/ubuntu/hadoop3/etc/hadoop/hdfs-site.xml /home/ubuntu/hadoop2/etc/hadoop/hdfs-site.xml

# Setup Apache Hive
wget --quiet http://mirror.symnds.com/software/Apache/hive/hive-2.1.0/apache-hive-2.1.0-bin.tar.gz
tar -xf apache-hive-2.1.0-bin.tar.gz
mv apache-hive-2.1.0-bin /home/ubuntu/hive
rm apache-hive-2.1.0-bin.tar.gz
echo 'export HIVE_HOME=/home/ubuntu/hive' >> /home/ubuntu/.bashrc
echo 'export PATH=$HIVE_HOME/bin:$PATH' >> /home/ubuntu/.bashrc

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
cd /home/ubuntu/hue
make apps
cd /home/ubuntu
echo 'export PATH=/home/ubuntu/hue/build/env/bin:$PATH' >> /home/ubuntu/.bashrc

# Download demo tables.
mkdir /home/ubuntu/demo_script
cd /home/ubuntu/demo_script
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/country.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/population.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/student.csv
wget --quiet https://github.com/Rice-Comp413-2016/RDFS/raw/demo-setup/demo_script/csv_generator.py
cd /home/ubuntu

