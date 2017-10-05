#include <iostream>
#include <unordered_map>

#pragma once

namespace config_reader {

/**
 * This is for reading config files. 
 * The methods will need to be more specified to pick which config
 * file you are reading from. Right now it only reads in the HDFS
 * default xml file.
 *
 * If you want to add files, then just add a function in the constructor
 * which parses the file. This assumes all key names are unique across ALL
 * files
 */
class ConfigReader {
 public:
  ConfigReader();

  /**
   * Get a string value associated with key
   * @param key the key in the config file
   */
  std::string getString(std::string key);

  /**
   * Get an int value associated with key
   * @param key the key in the config file
   */
  int getInt(std::string key);

  /**
   * Get a bool value associated with key
   * @param key the key in the config file
   */
  bool getBool(std::string key);

 private:
  // list of config files to read
  static const char *HDFS_DEFAULTS_CONFIG;

  // map of strings to strings, ints, and bools
  std::unordered_map<std::string, std::string> conf_strings;
  std::unordered_map<std::string, int> conf_ints;
  std::unordered_map<std::string, bool> conf_bools;

  // create the HDFS_DEFULATS_CONFIG mapping
  void InitHDFSDefaults();

  static const std::string CLASS_NAME;
};
}	
