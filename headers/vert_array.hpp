//definition of vert_array, which is the object that manipulate the mmapped files
#ifndef __VERT_ARRAY_H__
#define __VERT_ARRAY_H__

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.hpp"

extern class config sys_config;

class vert_array{
	private:
		std::string mmapped_vert_file;
		std::string mmapped_edge_file;
		int vert_index_file_fd;
		int edge_file_fd;
		unsigned long long vert_index_file_length;
		unsigned long long edge_file_length;
		vertex_index* vertex_array_header;
		edge* edge_array_header;
	
	public:
		vert_array();
		~vert_array();
		//return the number of out edges of vid
		unsigned int num_out_edges( unsigned int vid);
		//return the "which"-th out edge of vid
		edge* out_edge( unsigned int vid, unsigned int which );

};

vert_array::vert_array()
{
	struct stat st;
	char * memblock;

	mmapped_vert_file = sys_config.vertex_file_name;
	mmapped_edge_file = sys_config.edge_file_name;

	vert_index_file_fd = open( mmapped_vert_file.c_str(), O_RDONLY );
	if( vert_index_file_fd < 0 ){
		std::cout << "Cannot open index file!\n";
		exit( -1 );
	}

	edge_file_fd = open( mmapped_edge_file.c_str(), O_RDONLY );
	if( edge_file_fd < 0 ){
		std::cout << "Cannot open edge file!\n";
		exit( -1 );
	}

	//map index files to vertex_array_header
    fstat(vert_index_file_fd, &st);
    vert_index_file_length = (u64_t) st.st_size;

    printf( "vertex list file size:%lld(MBytes)\n", vert_index_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, vert_index_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        printf( "index file mapping failed!\n" );
		exit( -1 );
	}
    printf( "index array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
    vertex_array_header = (struct vertex_index *) memblock;

	//map edge files to edge_array_header
    fstat(edge_file_fd, &st);
    edge_file_length = (u64_t) st.st_size;

    printf( "edge list file size:%lld(MBytes)\n", edge_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, edge_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        printf( "edge file mapping failed!\n" );
		exit( -1 );
	}
    printf( "edge array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
    edge_array_header = (struct edge *) memblock;
}

vert_array::~vert_array()
{
	munmap( vertex_array_header, vert_index_file_length );
	munmap( edge_array_header, edge_file_length );
}

unsigned int vert_array::num_out_edges( unsigned int vid )
{
	unsigned long long start_edge, end_edge=0L;
	
	start_edge = vertex_array_header[vid].offset;
	if ( start_edge == 0L ) return 0;

    if ( vid == sys_config.max_vertex_id )
        end_edge = sys_config.num_edges;
    else{
        for( u32_t i=vid+1; i<=sys_config.max_vertex_id; i++ ){
            if( vertex_array_header[i].offset != 0L ){
                end_edge = vertex_array_header[i].offset -1;
                break;
            }
        }
    }
	if( end_edge < start_edge ){
		printf( "edge disorder detected!\n" );
		return 0;
	}
	return (end_edge - start_edge + 1);
}

edge* vert_array::out_edge( unsigned int vid, unsigned int which )
{
	edge* ret = new edge;
	if( which > vert_array::num_out_edges( vid ) ) return NULL;

	*ret = (edge)edge_array_header[ vertex_array_header[vid].offset + which ];

	return ret;
}

#endif