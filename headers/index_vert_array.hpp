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
class index_vert_array{
	private:
		std::string mmapped_vert_file;
		std::string mmapped_edge_file;
		int vert_index_file_fd;
		int edge_file_fd;
		unsigned long long vert_index_file_length;
		unsigned long long edge_file_length;
		vert_index* vert_array_header;
		edge* edge_array_header;
	
	public:
		index_vert_array();
		~index_vert_array();
		//return the number of out edges of vid
		unsigned int num_out_edges( unsigned int vid);
		//return the "which"-th out edge of vid
		edge* out_edge( unsigned int vid, unsigned int which );

};

index_vert_array::index_vert_array()
{
	struct stat st;
	char * memblock;

	mmapped_vert_file = gen_config.vert_file_name;
	mmapped_edge_file = gen_config.edge_file_name;

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

    PRINT_DEBUG( "vertex list file size:%lld(MBytes)\n", vert_index_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, vert_index_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        PRINT_ERROR( "index file mapping failed!\n" );
		exit( -1 );
	}
    PRINT_DEBUG( "index array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
    vert_array_header = (struct vert_index *) memblock;

	//map edge files to edge_array_header
    fstat(edge_file_fd, &st);
    edge_file_length = (u64_t) st.st_size;

    if (edge_file_length >= (u64_t)THRESHOLD_GRAPH_SIZE)
        gen_config.prev_update = false;
    else
        gen_config.prev_update = true;

    PRINT_DEBUG( "edge list file size:%lld(MBytes)\n", edge_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, edge_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        PRINT_ERROR( "edge file mapping failed!\n" );
		exit( -1 );
	}
    PRINT_DEBUG( "edge array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
    edge_array_header = (struct edge *) memblock;
}

index_vert_array::~index_vert_array()
{
	PRINT_DEBUG( "vertex index array unmapped!\n" );
	munmap( (void*)vert_array_header, vert_index_file_length );
	munmap( (void*)edge_array_header, edge_file_length );
	close( vert_index_file_fd );
	close( edge_file_fd );
}

unsigned int index_vert_array::num_out_edges( unsigned int vid )
{
	unsigned long long start_edge=0L, end_edge=0L;
	
	start_edge = vert_array_header[vid].offset;
    //if (vid == 152)
    //    PRINT_DEBUG("start_edge = %lld\n", start_edge);
    //if (vid == 152)
    //    PRINT_DEBUG("vert_array_head[156].offset = %lld\n", vert_array_header[156].offset);
	if ( start_edge == 0L && vid != 0 ) return 0;

	if ( vid > gen_config.max_vert_id ) return 0;

    if ( vid == gen_config.max_vert_id )
        end_edge = gen_config.num_edges;
    else{
        for( u32_t i=vid+1; i<=gen_config.max_vert_id; i++ ){
            if( vert_array_header[i].offset != 0L ){
                end_edge = vert_array_header[i].offset -1;
                break;
            }
        }
    }
	if( end_edge < start_edge ){
		PRINT_ERROR( "edge disorder detected!\n" );
		return 0;
	}
	return (end_edge - start_edge + 1);
}

edge* index_vert_array::out_edge( unsigned int vid, unsigned int which )
{
	edge* ret = new edge;
    //PRINT_DEBUG("which = %d, num_out_edges = %d\n", which, index_vert_array::num_out_edges(vid));
	if( which > index_vert_array::num_out_edges( vid ) ) return NULL;
    //PRINT_DEBUG("vert_array_header[%d].offset = %lld\n",vid, vert_array_header[vid].offset);

	*ret = (edge)edge_array_header[ vert_array_header[vid].offset + which ];
    //PRINT_DEBUG("vert_array->dest_vert = %d, weight = %f\n",edge_array_header[vert_array_header[vid].offset+which].dest_vert, edge_array_header[vert_array_header[vid].offset+which].edge_weight);
    //PRINT_DEBUG("vid = %d, ret->dest_vert = %d, weight = %f\n",vid, ret->dest_vert, ret->edge_weight);

	return ret;
}

#endif
