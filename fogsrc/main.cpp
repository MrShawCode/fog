#include "options_utils.h"
#include "config_parse.h"
#include <cassert>
#include <fstream>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "fog_engine.hpp"
#include "fog_engine_target.hpp"

#include "../application/sssp.hpp"
#include "../application/pagerank.hpp"

//boost::property_tree::ptree pt;
//boost::program_options::options_description desc;
//boost::program_options::variables_map vm; 

struct general_config gen_config;

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

	//config subjected to change.
	gen_config.num_processors = 4;
	gen_config.memory_size = (u64_t)4*1024*1024*1024;

	gen_config.min_vert_id = pt.get<u32_t>("description.min_vertex_id");
	gen_config.max_vert_id = pt.get<u32_t>("description.max_vertex_id");
	gen_config.num_edges = pt.get<u64_t>("description.num_of_edges");
	gen_config.max_out_edges = pt.get<u64_t>("description.max_out_edges");
	gen_config.graph_path = desc_name.substr(0, desc_name.find_last_of("/") );
	gen_config.vert_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".index";
	gen_config.edge_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".edge";
	gen_config.attr_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".attr";

	printf( "Graph name: %s\nApplication name:%s, with parameter:%s\n", 
		desc_name.c_str(), prog_name.c_str(), parameter.c_str() );

	printf( "Configurations:\n" );
	printf( "gen_config.memory_size = 0x%llx\n", gen_config.memory_size );
	printf( "gen_config.min_vert_id = %d\n", gen_config.min_vert_id );
	printf( "gen_config.max_vert_id = %d\n", gen_config.max_vert_id );
	printf( "gen_config.num_edges = %lld\n", gen_config.num_edges );
	printf( "gen_config.vert_file_name = %s\n", gen_config.vert_file_name.c_str() );
	printf( "gen_config.edge_file_name = %s\n", gen_config.edge_file_name.c_str() );
	printf( "gen_config.attr_file_name(WRITE ONLY) = %s\n", gen_config.attr_file_name.c_str() );

	if( prog_name == "sssp" ){
/*		segment_config<sssp_vert_attr> seg_config;

		sssp_program::start_vid = atoi(parameter.c_str());
		printf( "sssp_program start_vid = %d\n", sssp_program::start_vid );
		//ready and run
		(*(new fog_engine_target<sssp_program, sssp_vert_attr>(&seg_config)))();
*/
	}else if( prog_name == "pagerank" ){
		fog_engine<pagerank_program, pagerank_vert_attr> * eng;

		pagerank_program::iteration_times = atoi( parameter.c_str() );
		printf( "pagerank_program iteration_times = %d\n", pagerank_program::iteration_times );
		//ready and run
		(*(eng = new fog_engine<pagerank_program, pagerank_vert_attr>()))();
		delete eng;
	}
}


