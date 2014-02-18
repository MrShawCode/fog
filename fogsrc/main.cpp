#include "options_utils.h"
#include <cassert>
#include <fstream>

#include "application/sssp.hpp"

boost::program_options::options_description desc;
boost::program_options::variables_map vm; 


int main( int argc, const char**argv)
{
	std::string	prog_name;

    setup_options_convert( argc, argv );


}


