//! Utilities for options
#ifndef _OPTIONS_UTILS_
#define _OPTIONS_UTILS_

#include <boost/program_options.hpp>
#include <iostream>

boost::program_options::options_description desc;
boost::program_options::variables_map vm;

static void setup_options_convert(int argc, const char* argv[])
{
  desc.add_options()
	( "help,h", "Display help message")
	( "type,t", boost::program_options::value<std::string>()->required(), "Type of the snap file(edgelist or adjlist)")
	( "graph,g", boost::program_options::value<std::string>()->required(), "Name of the graph in snap format")
	( "destination,d",  boost::program_options::value<std::string>()->required(), "Destination folder that will contain the index and edge file");
    
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

static void setup_options_fog(int argc, const char* argv[])
{
  desc.add_options()
	( "help,h", "Display help message")
	( "graph,g", boost::program_options::value<std::string>()->required(), "The name of the graph to be processed (the .desc file)")
	( "application,a", boost::program_options::value<std::string>()->required(), "The name of the application (e.g., pagerank)")
	( "parameter,p", boost::program_options::value<std::string>()->required(), "Parameter to the application (e.g., iter times for pagerank, start vid for sssp");
    
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
