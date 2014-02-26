#include "options_utils.h"
#include "config_parse.h"
#include <cassert>
#include <fstream>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "fogengine.hpp"

#include "../application/sssp.hpp"

boost::property_tree::ptree pt;
boost::program_options::options_description desc;
boost::program_options::variables_map vm; 
//class config sys_config;

unsigned int sssp_program::start_vid = 0;

u32_t config::min_vertex_id;
u32_t config::max_vertex_id;
u64_t config::num_edges;
u32_t config::num_processors;
u64_t config::memory_size;
std::string config::graph_path;
std::string config::vertex_file_name;
std::string config::edge_file_name;
std::string config::attr_file_name;

int main( int argc, const char**argv)
{
	std::string	prog_name;
	std::string parameter;
	std::string desc_name;

    setup_options_fog( argc, argv );
	prog_name = vm["application"].as<std::string>();
	parameter = vm["parameter"].as<std::string>();
	desc_name = vm["graph"].as<std::string>();

	init_graph_desc( desc_name );

	config::min_vertex_id = pt.get<u32_t>("description.min_vertex_id");
	config::max_vertex_id = pt.get<u32_t>("description.max_vertex_id");
	config::num_edges = pt.get<u64_t>("description.num_of_edges");
	config::graph_path = desc_name.substr(0, desc_name.find_last_of("/") );
	config::vertex_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".index";
	config::edge_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".edge";
	config::attr_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".attr";

	printf( "Graph name: %s\nApplication name:%s, with parameter:%s\n", 
		desc_name.c_str(), prog_name.c_str(), parameter.c_str() );

	printf( "Configurations:\n" );
	printf( "sys_config.min_vertex_id = %d\n", config::min_vertex_id );
	printf( "sys_config.max_vertex_id = %d\n", config::max_vertex_id );
	printf( "sys_config.num_edges = %lld\n", config::num_edges );
	printf( "sys_config.vertex_file_name = %s\n", config::vertex_file_name.c_str() );
	printf( "sys_config.edge_file_name = %s\n", config::edge_file_name.c_str() );
	printf( "sys_config.attr_file_name(WRITE ONLY) = %s\n", config::attr_file_name.c_str() );

	if( prog_name == "sssp" ){
		sssp_program::start_vid = atoi(parameter.c_str());
		printf( "sssp_program start_vid = %d\n", sssp_program::start_vid );
		//initialize fog engine
		(*(new fogengine<sssp_program, sssp_vert_attr>))();
	}
}


