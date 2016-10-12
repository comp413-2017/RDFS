#include <iostream>
#include <unordered_map>

#include "ConfigReader.h"
#include <pugixml.hpp>
#include <easylogging++.h>

namespace config_reader {

using namespace pugi;

const char* ConfigReader::HDFS_DEFAULTS_CONFIG = "hdfs-default.xml";

ConfigReader::ConfigReader() {
	InitHDFSDefaults();
}

int ConfigReader::getInt(std::string key) {
	return conf_ints[key];
}

std::string ConfigReader::getString(std::string key) {
	return conf_strings[key];
}

bool ConfigReader::getBool(std::string key) {
	return conf_bools[key];
}

/**
 * Init the hdfs defaults xml file
 */
void ConfigReader::InitHDFSDefaults() {
	xml_document doc;
	xml_parse_result result = doc.load_file(HDFS_DEFAULTS_CONFIG);
	if (!result) {
		LOG(ERROR) << "XML [" << HDFS_DEFAULTS_CONFIG << "] parsed with errors, attr value: [" <<
			doc.child("node").attribute("attr").value() << "]\n";
			LOG(ERROR) << "Error description: " << result.description() << "\n";
	}
	xml_node properties = doc.child("configuration");
	for (xml_node child : properties.children()) {
		// the name and value nodes in the xml 
		xml_node name = child.first_child();
		xml_node value = name.next_sibling();
		const char* name_str = name.first_child().text().get();
		int value_int = value.first_child().text().as_int();
		conf_ints[name_str] = value_int;
		std::string value_str = value.first_child().text().as_string();
		conf_strings[name_str] = value_str;
		bool value_bool = value.first_child().text().as_bool();
		conf_bools[name_str] = value_bool;
	}
	LOG(INFO) << "Configured namenode (but not really!)";
}
} //namespace
