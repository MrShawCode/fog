//! Utilities for options
#ifndef _OPTIONS_UTILS_
#define _OPTIONS_UTILS_

#include<boost/program_options.hpp>
#include<iostream>

extern boost::program_options::options_description desc;
extern boost::program_options::variables_map vm;

static void setup_options(int argc, const char* argv[])
{
  desc.add_options()
	( "help,h", "Produce help message")
	( "graph,g", boost::program_options::value<std::string>()->required(), "Name of the graph in snap (edgelist) format")
	( "destination,d",  boost::program_options::value<std::string>()->required(), "Destination folder that contains the type2 file and index file");
    
  try {
    boost::program_options::store(boost::program_options::parse_command_line(argc,
									     argv,
									     desc),
				  vm);
    boost::program_options::notify(vm);
  }
  catch (boost::program_options::error &e) {
    if(vm.count("help") || argc ==1) {
      std::cerr << desc << "\n";
    }
    std::cerr << "Error:" << e.what() << std::endl;
    std::cerr << "Try: " << argv[0] << " --help" << std::endl;
    exit(-1);
  }
}

#endif
