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

template <typename T>
index_vert_array<T>::index_vert_array()
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
    //edge_array_header = (struct T *) memblock;
    edge_array_header = (T *) memblock;

    if (gen_config.with_in_edge)
    {
        mmapped_in_vert_file = gen_config.in_vert_file_name;
        mmapped_in_edge_file = gen_config.in_edge_file_name;

        in_vert_index_file_fd = open( mmapped_in_vert_file.c_str(), O_RDONLY );
        if( in_vert_index_file_fd < 0 ){
            std::cout << "Cannot open in_index file!\n";
            exit( -1 );
        }
        in_edge_file_fd = open( mmapped_in_edge_file.c_str(), O_RDONLY );
        if( in_edge_file_fd < 0 ){
            std::cout << "Cannot open edge file!\n";
            exit( -1 );
        }

        //map index files to vertex_array_header
        fstat(in_vert_index_file_fd, &st);
        in_vert_index_file_length = (u64_t) st.st_size;

        PRINT_DEBUG( "in-vertex list file size:%lld(MBytes)\n", in_vert_index_file_length/(1024*1024) );
        memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, in_vert_index_file_fd, 0 );
        if( memblock == MAP_FAILED ){
            PRINT_ERROR( "in-index file mapping failed!\n" );
            exit( -1 );
        }
        PRINT_DEBUG( "in-index array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
        in_vert_array_header = (struct vert_index *) memblock;

        //map edge files to edge_array_header
        fstat(in_edge_file_fd, &st);
        in_edge_file_length = (u64_t) st.st_size;

        PRINT_DEBUG( "in_edge list file size:%lld(MBytes)\n", in_edge_file_length/(1024*1024) );
        memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, in_edge_file_fd, 0 );
        if( memblock == MAP_FAILED ){
            PRINT_ERROR( "in_edge file mapping failed!\n" );
            exit( -1 );
        }
        PRINT_DEBUG( "in_edge array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
        in_edge_array_header = (struct in_edge *) memblock;
    }

}

template <typename T>
index_vert_array<T>::~index_vert_array()
{
	PRINT_DEBUG( "vertex index array unmapped!\n" );
	munmap( (void*)vert_array_header, vert_index_file_length );
	munmap( (void*)edge_array_header, edge_file_length );
	close( vert_index_file_fd );
	close( edge_file_fd );

    if (gen_config.with_in_edge)
    {
        munmap( (void*)in_vert_array_header, in_vert_index_file_length );
        munmap( (void*)in_edge_array_header, in_edge_file_length );
        close( in_vert_index_file_fd );
        close( in_edge_file_fd );
    }
}

template <typename T>
unsigned int index_vert_array<T>::num_out_edges( unsigned int vid )
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
        PRINT_DEBUG("vid = %d, start_edge = %llu, end_edge = %llu\n", 
                vid, start_edge, end_edge);
		PRINT_ERROR( "edge disorder detected!\n" );
		return 0;
	}
	return (end_edge - start_edge + 1);
}

template <typename T>
unsigned int index_vert_array<T>::num_edges( unsigned int vid, int mode )
{
	unsigned long long start_edge=0L, end_edge=0L;
	
    if (mode == OUT_EDGE)
    {
        start_edge = vert_array_header[vid].offset;

        if ( start_edge == 0L && vid != 0 ) return 0;

        if ( vid > gen_config.max_vert_id ) return 0;

        if ( vid == gen_config.max_vert_id )
            end_edge = gen_config.num_edges;
        else{
            for( u32_t i=vid+1; i<=gen_config.max_vert_id; i++ ){
                if (i == gen_config.max_vert_id)
                {
                    end_edge = gen_config.num_edges;
                    break;
                }
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
    else
    {
        assert(mode == IN_EDGE);
        start_edge = in_vert_array_header[vid].offset;

        if ( start_edge == 0L && vid != 0 ) return 0;

        if ( vid > gen_config.max_vert_id ) return 0;

        if ( vid == gen_config.max_vert_id )
            end_edge = gen_config.num_edges;
        else{
            for( u32_t i=vid+1; i<=gen_config.max_vert_id; i++ ){
                if (i == gen_config.max_vert_id)
                {
                    end_edge = gen_config.num_edges;
                    break;
                }
                if( in_vert_array_header[i].offset != 0L ){
                    end_edge = in_vert_array_header[i].offset -1;
                    break;
                }
            }
        }
        if( end_edge < start_edge ){
            PRINT_ERROR( "in_edge disorder detected!\n" );
            return 0;
        }
        return (end_edge - start_edge + 1);

    }
}

template <typename T>
T * index_vert_array<T>::get_out_edge( unsigned int vid, unsigned int which )
{
	T * ret = new T;
	if( which > index_vert_array<T>::num_edges( vid, OUT_EDGE) ) return NULL;

	*ret = (T)edge_array_header[ vert_array_header[vid].offset + which ];

	return ret;
}

template <typename T>
in_edge * index_vert_array<T>::get_in_edge( unsigned int vid, unsigned int which)
{
	in_edge * ret = new in_edge;
	if( which > index_vert_array<T>::num_edges( vid, IN_EDGE) ) return NULL;

	*ret = (in_edge)in_edge_array_header[ in_vert_array_header[vid].offset + which ];

	return ret;
}

#endif
