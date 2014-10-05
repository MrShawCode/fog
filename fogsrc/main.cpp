#include "options_utils.h"
#include "config_parse.h"
#include <cassert>
#include <fstream>
#include <stdlib.h>

#include "config.hpp"
#include "index_vert_array.hpp"
//#include "fog_engine_scc.hpp"
#include "fog_engine.hpp"
//#include "fog_engine_target.hpp"
#include "print_debug.hpp"

#include "../application/sssp.hpp"
#include "../application/pagerank.hpp"
#include "../application/scc.hpp"
#include "../application/spmv.hpp"
#include "../application/cc.hpp"
#include "../application/bfs.hpp"
#include "bitmap.hpp"

//boost::property_tree::ptree pt;
//boost::program_options::options_description desc;
//boost::program_options::variables_map vm; 

struct general_config gen_config;
extern FILE * log_file;

template <typename T>
void start_engine(std::string prog_name)
{
    
    std::cout << "sizeof T = " << sizeof(T) << std::endl;
    if( prog_name == "sssp" ){
		sssp_program<T>::start_vid = vm["sssp::source"].as<unsigned long>();
		PRINT_DEBUG( "sssp_program start_vid = %d\n", sssp_program<T>::start_vid );
		//ready and run
        fog_engine<sssp_program<T>, sssp_vert_attr, sssp_vert_attr, T> *eng;
        (*(eng = new fog_engine<sssp_program<T>, sssp_vert_attr, sssp_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
	}else if( prog_name == "bfs" ){
		bfs_program<T>::bfs_root = vm["bfs::bfs_root"].as<unsigned long>();
		PRINT_DEBUG( "bfs_program bfs_root = %d\n", bfs_program<T>::bfs_root);
		//ready and run
        fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T> *eng;
        (*(eng = new fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
	}else if( prog_name == "pagerank" ){

		pagerank_program<T>::iteration_times = vm["pagerank::niters"].as<unsigned long>();
		PRINT_DEBUG( "pagerank_program iteration_times = %d\n", pagerank_program<T>::iteration_times );
		//ready and run
        fog_engine<pagerank_program<T>, pagerank_vert_attr, pagerank_vert_attr, T> * eng;
        (*(eng = new fog_engine<pagerank_program<T>, pagerank_vert_attr, pagerank_vert_attr, T>(GLOBAL_ENGINE)))();
        delete eng;
	}else if(prog_name == "scc"){
        //set FIRST_INIT to init the attr_buf
        PRINT_DEBUG("scc starts!\n");

        fog_engine<scc_program<T>, scc_vert_attr, scc_update, T> * eng;
        (*(eng = new fog_engine<scc_program<T>, scc_vert_attr, scc_update, T>(TARGET_ENGINE)))();
        delete eng;
    }else if (prog_name == "spmv"){
        PRINT_DEBUG("spmv starts!\n");
        fog_engine<spmv_program<T>, spmv_vert_attr, spmv_update, T> * eng;
        (*(eng = new fog_engine<spmv_program<T>, spmv_vert_attr, spmv_update, T>(GLOBAL_ENGINE)))();
        delete eng;
    }else if (prog_name == "cc"){
        PRINT_DEBUG("cc starts!\n");
        fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T> *eng;
        (*(eng = new fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
    }
}

int main( int argc, const char**argv)
{
	std::string	prog_name_app;
	std::string desc_name;

    setup_options_fog( argc, argv );
	prog_name_app = vm["application"].as<std::string>();
	desc_name = vm["graph"].as<std::string>();
    

	init_graph_desc( desc_name );

	//config subjected to change.
	gen_config.num_processors = vm["processors"].as<unsigned long>();
	gen_config.num_io_threads = vm["diskthreads"].as<unsigned long>();
	//the unit of memory is MB
	gen_config.memory_size = (u64_t)(vm["memory"].as<unsigned long>())*1024*1024;

    std::cout << "sizeof type1_edge = " << sizeof(type1_edge) << std::endl;
    std::cout << "sizeof type2_edge = " << sizeof(type2_edge) << std::endl;
    //add by  hejian
    if (!(log_file = fopen(LOG_FILE, "w"))) //open file for mode
    {
        printf("failed to open %s.\n", LOG_FILE);
        exit(666);
    }

	gen_config.min_vert_id = pt.get<u32_t>("description.min_vertex_id");
	gen_config.max_vert_id = pt.get<u32_t>("description.max_vertex_id");
	gen_config.num_edges = pt.get<u64_t>("description.num_of_edges");
	gen_config.max_out_edges = pt.get<u64_t>("description.max_out_edges");
	gen_config.graph_path = desc_name.substr(0, desc_name.find_last_of("/") );
	gen_config.vert_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".index";
	gen_config.edge_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".edge";
	gen_config.attr_file_name = desc_name.substr(0, desc_name.find_last_of(".") )+".attr";

    unsigned int type1_or_type2 = pt.get<u32_t>("description.edge_type");
    bool with_inedge = pt.get<bool>("description.with_in_edge");
    if (with_inedge)
    {
        gen_config.with_in_edge = true;
        gen_config.in_edge_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-edge";
        gen_config.in_vert_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-index";
    }

	PRINT_DEBUG( "Graph name: %s\nApplication name:%s\n", 
		desc_name.c_str(), prog_name_app.c_str() );

	PRINT_DEBUG( "Configurations:\n" );
	PRINT_DEBUG( "gen_config.memory_size = 0x%llx\n", gen_config.memory_size );
	PRINT_DEBUG( "gen_config.min_vert_id = %d\n", gen_config.min_vert_id );
	PRINT_DEBUG( "gen_config.max_vert_id = %d\n", gen_config.max_vert_id );
	PRINT_DEBUG( "gen_config.num_edges = %lld\n", gen_config.num_edges );
	PRINT_DEBUG( "gen_config.vert_file_name = %s\n", gen_config.vert_file_name.c_str() );
	PRINT_DEBUG( "gen_config.edge_file_name = %s\n", gen_config.edge_file_name.c_str() );
	PRINT_DEBUG( "gen_config.attr_file_name(WRITE ONLY) = %s\n", gen_config.attr_file_name.c_str() );

    if (type1_or_type2 == 1)
    {
        std::cout << "the edge type is type1" << std::endl;
        start_engine<type1_edge>(prog_name_app);
    }
    else
    {
        assert(type1_or_type2 == 2);
        std::cout << "the edge type is type2" << std::endl;
        start_engine<type2_edge>(prog_name_app);
    }

}


