# Rice-HDFS

The current plan is to store all our code in this one repo, with separate directories for nameNode, dataNode, and code is needed by both (such as various protocals). We'll add more as we need them.


Check the wiki for documentation!

# Development
1. Install [Virtualbox](https://www.virtualbox.org/). Works with 5.1.
2. Install [Vagrant](https://vagrantup.com/). Works with 1.8.5.
3. Clone the repo: `git clone https://github.com/Rice-Comp413-2016/Rice-HDFS.git`
4. `cd Rice-HDFS`
5. `vagrant up` (takes 17 minutes from scratch for me)
   - I (Stu) had to "sudo" these commands
   - Make sure to do this from the repo directory (otherwise it asks for vagrant install) 
6. `vagrant ssh`.
7. You should be in the development environment. Things to know:
   - The username is `vagrant` and the password is `vagrant`.
   - The machine has 1G of memory allocated. Change Vagrantfile if you need
     more.
   - The folder /home/vagrant/rdfs is synced from here (here being the location
     of this readme), meaning that all edits you make to files under the
     project are immediately reflected in the dev machine.
   - Hadoop binaries such as `hdfs` are on the PATH.
   - Google protobuf 3.0 is installed, you can run `protoc` to generate C++
     headers from .proto specifications.
   - If you need external HTTP access, the machine is bound to the address
     33.33.33.33.

# Building
```
sudo apt-get install libboost-all-dev
sudo apt-get install libasio-dev 

mkdir build
cd build

sudo apt-get install libboost-all-dev
sudo apt-get install libasio-dev 

cmake ..
make
```
You will see a sample executable placed in `build/rice-namenode/namenode.` The
compiled protocols are in `build/proto`

# Testing
```
Run the namenode executable from build/rice-namenode. 
Then run something like "hdfs dfs -fs hdfs://localhost:port/ -mkdir foo"
where port is the port used by the namenode (it will print the port used)
``` 
