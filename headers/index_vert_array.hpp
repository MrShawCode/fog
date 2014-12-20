//definition of index_vert_array, which is the object that manipulate the mmapped files
#ifndef __VERT_ARRAY_H__
#define __VERT_ARRAY_H__

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.hpp"
#define THRESHOLD_GRAPH_SIZE 20*1024*1024*1024

enum get_edge_state
{
    OUT_EDGE = 1, IN_EDGE
};
template <typename T>
class index_vert_array{
	private:
		std::string mmapped_vert_file;
		std::string mmapped_edge_file;
		int vert_index_file_fd;
		int edge_file_fd;
		unsigned long long vert_index_file_length;
		unsigned long long edge_file_length;
		vert_index* vert_array_header;
		T * edge_array_header;

        //if necessary
        std::string mmapped_in_vert_file;
        std::string mmapped_in_edge_file;
        int in_vert_index_file_fd;
        int in_edge_file_fd;
        unsigned long long in_vert_index_file_length;
        unsigned long long in_edge_file_length;
        vert_index * in_vert_array_header;
        in_edge * in_edge_array_header;
	
	public:
		index_vert_array();
		~index_vert_array();
		//return the number of out edges of vid
		unsigned int num_out_edges( unsigned int vid);
        //mode: OUT_EDGE(1) or IN_EDGE(2)
        unsigned int num_edges(unsigned int vid, int mode);
		//return the "which"-th out edge of vid
		T * get_out_edge( unsigned int vid, unsigned int which );
        in_edge * get_in_edge(unsigned int vid, unsigned int which);
};
#endif
