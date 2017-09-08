# Rice-HDFS

The current plan is to store all our code in this one repo, with separate directories for nameNode, dataNode, and code needed by both (such as various protocols). We'll add more as we need them.


Check the wiki for documentation!

# Development
1. Install [Virtualbox](https://www.virtualbox.org/). Works with 5.1.
2. Install [Vagrant](https://vagrantup.com/). Works with 1.9.8.
3. Clone the repo: `git clone https://github.com/comp413-2017/RDFS.git`
4. `cd HDFS`
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

cmake ..
make
```
You will see a sample executable placed in `build/rice-namenode/namenode.` The
compiled protocols are in `build/proto`.

Note: if "cmake .." fails with a "can't find GTest" error, install it manually using the same commands from the vagrant provisioning file. i.e. :
```
apt-get install -y libgtest-dev
cd /usr/src/gtest
cmake CMakeLists.txt
make
cp *.a /usr/lib
cd /home/vagrant
```

# Testing

The Google Test framework is now included in the development environment.
Tests should be placed in the ~/rdfs/test directory.
After creating a new test file, you can modify the CMakeLists.txt file to create an executable
to run those tests.
There is a file, tests/run-all/run-all-tests.cc, that creates an executable running all tests.
If you create a new test executable, modify this to add yours.

You can run the tests by running the executables (e.g. ./runAllTests) in ~/rdfs/build/test after following the build instructions above

A beginner's guide to using Google Test is located [here](https://github.com/google/googletest/blob/master/googletest/docs/Primer.md)

A githook has been added at rdfs/test/pre-commit.  It's a shell script that will build and run 
the unit tests.  To use it, copy the file to rdfs/.git/hooks.  Then, before each commit is made
the tests will run, and a failure will halt the commit.  If this is too restrictive, renaming
the file to pre-push will do the same thing only when you try to push.

Namenode:
Run the namenode executable from build/rice-namenode. 
Then run something like `hdfs dfs -fs hdfs://localhost:port/ -mkdir foo`
where port is the port used by the namenode (it will print the port used)

Datanode:
Run the datanode executable from build/rice-datanode. 
Then run something like `hdfs dfsadmin -shutdownDatanode hdfs://localhost:port/`
where port is the port used by the datanode (it will print the port used)

If you want to do a quick end-to-end test, try the following to cat the file:

1. Pull the code and build (as explained above).
2. Run zookeeper (from ~, it’s `sudo zookeeper/bin/zkServer.sh start`). This will run in the background.
3. Run namenode (`rdfs/build/rice-namenode/namenode`). This will run in the foreground.
4. Run datanode (`rdfs/build/rice-datanode/datanode`). This will run in the foreground.
5. Create a file with `hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal localFile /filename`
6. Try to cat that file with `hdfs dfs -fs hdfs://localhost:5351 -cat /filename`

# Mocking

Whether you use Google Mock in conjunction with Google Test is up to you.

Google Mock should be used in conjunction with Google Test.

Google Mock is not a testing framework, but a framework for writing C++ mock   
classes. A mock class is simplified version of a real class that can be 
created to aid with testing. However, Google Mock does  do an automatic
verification of expectations.      

The typical flow is:
1. Import the Google Mock names you need to use. All Google Mock names are
in the `testing` namespace unless they are macros or otherwise noted.

2. Create the mock objects.

3. Optionally, set the default actions of the mock objects.

4. Set your expectations on the mock objects (How will they be called? What
will they do?).

5. Exercise code that uses the mock objects; if necessary, check the result
using [Google Test](../../googletest/) assertions.

6. When a mock objects is destructed, Google Mock automatically verifies
that all expectations on it have been satisfied.

You should read through all of the Google Mock documentation located 
at (/googletest/googlemock/docs/) before using it:
   - [ForDummies](ForDummies.md) -- start here if you are new to Google Mock.
   - [CheatSheet](CheatSheet.md) -- a quick reference.
   - [CookBook](CookBook.md) -- recipes for doing various tasks using Google 
     Mock.
