#include <cstdlib>

// Run all tests in folder and subfolders
int main(int argc, char **argv) {
  system("~/rdfs/build/test/DeleteTest");
  system("~/rdfs/build/test/ReplicationTest");
  system("~/rdfs/build/test/NameNodeTest");
  system("~/rdfs/build/test/NativeFsTest");
  system("~/rdfs/build/test/ReadWriteTest");
  system("~/rdfs/build/test/ZKDNClientTest");
  system("~/rdfs/build/test/ZKLockTest");
  system("~/rdfs/build/test/ZKWrapperTest");
  return 1;
}
