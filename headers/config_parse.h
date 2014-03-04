#ifndef _CONFIG_PARSE_
#define _CONFIG_PARSE_
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

boost::property_tree::ptree pt;

static void init_graph_desc(const std::string& graph_name)
{
  try {
    boost::property_tree::ini_parser::read_ini(graph_name, pt);
  }
  catch(...) {
    exit(-1);
  }
}
#endif
