/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He, Huiming Lv 
 *
 * Routines:
 *   Entrance of the program.
 *
 * Notes:
 *   1.modified log_file_name and print the user command to the log file, notifying the user
 *     when he use the defult value(sssp::source,bfs::root,pagerank::niters)
 *     modified by Huiming Lv  2014/12/21
 *************************************************************************************************/

#include <cassert>
#include <fstream>
#include <stdlib.h>

#include "fog_engine.hpp"
#include "fog_engine.cpp"
#include "print_debug.hpp"
#include "options_utils.h"
#include "config_parse.h"
#include "config.hpp"
#include "index_vert_array.hpp"
#include "index_vert_array.cpp"
#include "bitmap.hpp"
//#include "fog_engine_scc.hpp"
//#include "fog_engine_target.hpp"

#include "../application/sssp.hpp"
#include "../application/pagerank.hpp"
#include "../application/scc.hpp"
#include "../application/spmv.hpp"
#include "../application/cc.hpp"
#include "../application/bfs.hpp"

#include "./lpa.cpp"
#include "./lpa_async.cpp"
#include "./greedy_coloring.cpp"
#include "./pagerank_simple.cpp"
#include "./pagerank_matrix.cpp"
//boost::property_tree::ptree pt;
//boost::program_options::options_description desc;
//boost::program_options::variables_map vm; 



struct general_config gen_config;
FILE * log_file;

#ifdef EXPERIMENT
FILE * test_log_file;  
FILE * cv_log_file;
#endif

template <typename T>
void start_engine(std::string prog_name)
{
    //std::cout << "sizeof T = " << sizeof(T) << std::endl;
    if( prog_name == "sssp" ){
		sssp_program<T>::start_vid = vm["sssp::source"].as<unsigned long>();
        unsigned int start_vid = sssp_program<T>::start_vid;
        if(0 == start_vid)
        {
            std::cout<<"You didn't input the sssp::source or you chose the default value:0, the program will start at start_vid=0."<<std::endl;
        }
        if(8 != sizeof(T))
        {
            PRINT_ERROR("This algorithm need 'type1' edge file!\n");
        }
		PRINT_DEBUG( "sssp_program start_vid = %d\n", sssp_program<T>::start_vid );
		//ready and run
        fog_engine<sssp_program<T>, sssp_vert_attr, sssp_vert_attr, T> *eng;
        (*(eng = new fog_engine<sssp_program<T>, sssp_vert_attr, sssp_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
	}else if( prog_name == "bfs" ){
        bfs_program<T>::bfs_root = vm["bfs::bfs-root"].as<unsigned long>();
        unsigned int root_vid = bfs_program<T>::bfs_root;
        if(0 == root_vid)
        {
            std::cout<<"You didn't input the bfs::bfs_root or you chose the default value:0, the program will start at bfs_root=0."<<std::endl;
        }
        PRINT_DEBUG( "bfs_program bfs_root = %d\n", bfs_program<T>::bfs_root);
        fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T> *eng;
        (*(eng = new fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
    }else if( prog_name == "pagerank" ){

		pagerank_program<T>::iteration_times = vm["pagerank::niters"].as<unsigned long>();
        unsigned int iteration_times = pagerank_program<T>::iteration_times;
        if(10 == iteration_times)
        {
            std::cout<<"You didn't input the pagerank::niters or you chose the default value:10, the algorithm will run 10 iterations."<<std::endl;
        }
		PRINT_DEBUG( "pagerank_program iteration_times = %d\n", pagerank_program<T>::iteration_times );
		//ready and run
        fog_engine<pagerank_program<T>, pagerank_vert_attr, pagerank_vert_attr, T> * eng;
        (*(eng = new fog_engine<pagerank_program<T>, pagerank_vert_attr, pagerank_vert_attr, T>(GLOBAL_ENGINE)))();
        delete eng;
	}else if(prog_name == "scc"){
        //set FIRST_INIT to init the attr_buf
        PRINT_DEBUG("scc starts!\n");
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }

        fog_engine<scc_program<T>, scc_vert_attr, scc_update, T> * eng;
        (*(eng = new fog_engine<scc_program<T>, scc_vert_attr, scc_update, T>(TARGET_ENGINE)))();
        delete eng;
    }else if (prog_name == "spmv"){
        PRINT_DEBUG("spmv starts!\n");
        if(8 != sizeof(T))
        {
            PRINT_ERROR("This algorithm need 'type1' edge file!\n");
        }
        fog_engine<spmv_program<T>, spmv_vert_attr, spmv_update, T> * eng;
        (*(eng = new fog_engine<spmv_program<T>, spmv_vert_attr, spmv_update, T>(GLOBAL_ENGINE)))();
        delete eng;
    }else if (prog_name == "cc"){
        PRINT_DEBUG("cc starts!\n");
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
        fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T> *eng;
        (*(eng = new fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T>(TARGET_ENGINE)))();
        delete eng;
    }else if (prog_name == "lpa"){
        PRINT_DEBUG("lpa starts!\n");
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
       
        LPA_program<T> * lpa = new LPA_program<T>;
        lpa->run();
    }else if (prog_name == "lpa_async"){
        PRINT_DEBUG("lpa_async starts!\n");
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
       
        LPA_async_program<T> * lpa_async = new LPA_async_program<T>;
        lpa_async->run();
    }else if (prog_name == "greedy_coloring"){
        PRINT_DEBUG("greedy coloring starts!\n");
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
       
        Greedy_coloring<T> * g_c = new Greedy_coloring<T>;
        g_c->run();
    }else if (prog_name == "pagerank_s"){
        PRINT_DEBUG("pagerank_simple starts!\n");
		Pagerank_simple<T>::iteration_times = vm["pagerank_s::niters"].as<unsigned long>();
        unsigned int iteration_times = Pagerank_simple<T>::iteration_times;
        if(10 == iteration_times)
        {
            std::cout<<"You didn't input the pagerank_s::niters or you chose the default value:10, the algorithm will run 10 iterations."<<std::endl;
        }
        
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
       
        Pagerank_simple<T> * p_s = new Pagerank_simple<T>;
        p_s->run();
    }else if (prog_name == "pagerank_m"){
        PRINT_DEBUG("pagerank_matrix starts!\n");
		Pagerank_matrix<T>::iteration_times = vm["pagerank_m::niters"].as<unsigned long>();
        unsigned int iteration_times = Pagerank_matrix<T>::iteration_times;
        if(10 == iteration_times)
        {
            std::cout<<"You didn't input the pagerank_m::niters or you chose the default value:10, the algorithm will run 10 iterations."<<std::endl;
        }
        
        int check = access(gen_config.in_edge_file_name.c_str(), F_OK);
        if(-1 ==check )
        {
            PRINT_ERROR("in_edge file doesn't exit or '-i' is false!\n");
        }
       
        Pagerank_matrix<T> * p_m = new Pagerank_matrix<T>;
        p_m->run();
    }else if (prog_name == "demo"){
        PRINT_DEBUG("demo starts!\n");

        //...

        //fog_engine<demo_program<T>, demo_vert_attr, demo_update, T> *eng;
        //(*(eng = new fog_engine<demo_program<T>, demo_vert_attr, demo_update, T>(GLOBAL_ENGINE OR TARGET_ENGINE)))();
        //delete eng;
    }
}

int main( int argc, const char**argv)
{

    std::string user_command;
    for(int i = 0; i < argc; i++)
    {
        user_command += argv[i];
        user_command += " ";
    }

	std::string	prog_name_app;
	std::string desc_name;
    std::string log_file_name;
    std::string test_log_file_name; 
    std::string cv_log_file_name;

    setup_options_fog( argc, argv );
	prog_name_app = vm["application"].as<std::string>();
	desc_name = vm["graph"].as<std::string>();
    
    time_t timep;
    time(&timep);
    struct tm *tm_p = localtime(&timep);
    //std::string start_time;
    char temp[100];
    sprintf(temp, "%d.%d.%d-%dh%dm%ds", tm_p->tm_year+1900, tm_p->tm_mon+1,
            tm_p->tm_mday, tm_p->tm_hour, tm_p->tm_min, tm_p->tm_sec);

    log_file_name = "print-" + prog_name_app + "-" + std::string(temp) + "-.log";
    test_log_file_name = "test-" + prog_name_app + "-" + std::string(temp) + "-.LOG";
    cv_log_file_name = "cv-" + prog_name_app + "-" + std::string(temp) + "-.LOG";
    
	init_graph_desc( desc_name );

	//config subjected to change.
	gen_config.num_processors = vm["processors"].as<unsigned long>();
	gen_config.num_io_threads = vm["diskthreads"].as<unsigned long>();
	//the unit of memory is MB
	gen_config.memory_size = (u64_t)(vm["memory"].as<unsigned long>())*1024*1024;

    //std::cout << "sizeof type1_edge = " << sizeof(type1_edge) << std::endl;
    //std::cout << "sizeof type2_edge = " << sizeof(type2_edge) << std::endl;
    //add by  hejian
    if (!(log_file = fopen(log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", log_file_name.c_str());
        exit(666);
    }
#ifdef EXPERIMENT
    if (!(test_log_file = fopen(test_log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", test_log_file_name.c_str());
        exit(666);
    }
#endif
    /*
    if (!(cv_log_file = fopen(cv_log_file_name.c_str(), "w"))) //open file for mode
    {
        printf("failed to open %s.\n", cv_log_file_name.c_str());
        exit(666);
    }
    */

    PRINT_DEBUG("Your command is: %s\n", user_command.c_str());

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
    bool i_in_edge = vm["in-edge"].as<bool>();
    if(i_in_edge)
    {
        if (with_inedge)
        {
            gen_config.with_in_edge = true;
            gen_config.in_edge_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-edge";
            gen_config.in_vert_file_name = desc_name.substr(0, desc_name.find_last_of(".")) + ".in-index";
        }
        else
        {
            PRINT_ERROR("in_edge file doesn't exit!\n");
        }
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
        //std::cout << "the edge type is type1" << std::endl;
        start_engine<type1_edge>(prog_name_app);
    }
    else
    {
        assert(type1_or_type2 == 2);
        //std::cout << "the edge type is type2" << std::endl;
        start_engine<type2_edge>(prog_name_app);
    }
}

