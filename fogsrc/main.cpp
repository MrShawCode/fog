#include "options_utils.h"
#include "config_parse.h"
#include <cassert>
#include <fstream>

#include "config.hpp"
#include "vert_array.hpp"
#include "fogengine.hpp"

#include "../application/sssp.hpp"

boost::property_tree::ptree pt;
boost::program_options::options_description desc;
boost::program_options::variables_map vm; 
class config sys_config;

int main( int argc, const char**argv)
{
	std::string	prog_name;
	std::string parameter;
	std::string desc_name, temp;

    setup_options_fog( argc, argv );
	prog_name = vm["application"].as<std::string>();
	parameter = vm["parameter"].as<std::string>();
	desc_name = vm["graph"].as<std::string>();

	init_graph_desc( desc_name );

	sys_config.min_vertex_id = pt.get<u32_t>("description.min_vertex_id");
	sys_config.max_vertex_id = pt.get<u32_t>("description.max_vertex_id");
	sys_config.num_edges = pt.get<u64_t>("description.num_of_edges");
	sys_config.graph_path = desc_name.substr(0, desc_name.find_last_of("/") );
	sys_config.vertex_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".index";
	sys_config.edge_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".edge";

	if( prog_name == "sssp" ){
		printf( "Will process graph:%s will sssp start from:%d\n", desc_name.c_str(), atoi(parameter.c_str()) );
		printf( "Show configurations below:\n" );
		printf( "sys_config.min_vertex_id = %d\n", sys_config.min_vertex_id );
		printf( "sys_config.max_vertex_id = %d\n", sys_config.max_vertex_id );
		printf( "sys_config.num_edges = %lld\n", sys_config.num_edges );
		printf( "sys_config.vertex_file_name = %s\n", sys_config.vertex_file_name.c_str() );
		printf( "sys_config.edge_file_name = %s\n", sys_config.edge_file_name.c_str() );
	
	}

	vert_array new_array;
	edge * t_edge;
	u32_t vid = 4;
	printf( "number of edge of %d is %d\n", vid, new_array.num_out_edges(vid) );
	for( u32_t i = 0; i<new_array.num_out_edges(vid); i++ ){
		t_edge = new_array.out_edge( vid, i );
		printf( "the %d-th out edge of vid:%d is %d, value is:%f\n",
			i, vid, t_edge->dst_vert, t_edge->edge_weight );
		delete t_edge;
	}
	vid = 27;
	printf( "number of edge of %d is %d\n", vid, new_array.num_out_edges(vid) );
	for( u32_t i = 0; i<new_array.num_out_edges(vid); i++ ){
		t_edge = new_array.out_edge( vid, i );
		printf( "the %d-th out edge of vid:%d is %d, value is:%f\n",
			i, vid, t_edge->dst_vert, t_edge->edge_weight );
		delete t_edge;
	}
	vid = 1413511389;
	printf( "number of edge of %d is %d\n", vid, new_array.num_out_edges(vid) );
	for( u32_t i = 0; i<new_array.num_out_edges(vid); i++ ){
		t_edge = new_array.out_edge( vid, i );
		printf( "the %d-th out edge of vid:%d is %d, value is:%f\n",
			i, vid, t_edge->dst_vert, t_edge->edge_weight );
		delete t_edge;
	}
	vid = 1413511393;
	printf( "number of edge of %d is %d\n", vid, new_array.num_out_edges(vid) );
}


