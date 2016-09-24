# Rice-HDFS

The current plan is to store all our code in this one repo, with separate directories for nameNode, dataNode, and code is needed by both (such as various protocals). We'll add more as we need them.


Check the wiki for documentation!

# Development
1. Install [Virtualbox](https://www.virtualbox.org/). Works with 5.1.
2. Install [Vagrant](https://vagrantup.com/). Works with 1.8.5.
3. `vagrant up` (takes 17 minutes from scratch for me)
4. `vagrant ssh`.
   The folder /home/vagrant/rdfs is synced from here, meaning that all edits
   you make to files under the project are immediately reflected in the dev
   machine.
