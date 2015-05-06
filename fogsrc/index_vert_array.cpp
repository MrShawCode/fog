/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao
 *
 * Routines:
 *   Manipulate the mmapped files
 * 
 * Notes:
 *   1.fix bug(function:num_edges())
 *     modified by Huiming Lv   2015/5/5 
 *************************************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.hpp"
#include "index_vert_array.hpp"
#include <errno.h>
extern int errno;
template <typename T>
index_vert_array<T>::index_vert_array()
{
	struct stat st;
	char * memblock = NULL;

	mmapped_vert_file = gen_config.vert_file_name;
	mmapped_edge_file = gen_config.edge_file_name;

	vert_index_file_fd = open( mmapped_vert_file.c_str(), O_RDONLY, S_IRUSR | S_IRGRP | S_IROTH  );
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

    //in-memory
    /*
    memblock = (char *)malloc(vert_index_file_length);
    u64_t read_in_byte = (u64_t)read(vert_index_file_fd, memblock, vert_index_file_length); 
    if(read_in_byte != vert_index_file_length)
    {
        std::cout<<read_in_byte<<std::endl;
        std::cout<<vert_index_file_length<<std::endl;
        PRINT_ERROR("read vert_index fail!\n");
    }
    vert_array_header = (struct vert_index *) memblock;
    */

    //PRINT_DEBUG( "vertex list file size:%lld(MBytes)\n", vert_index_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, vert_index_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        PRINT_ERROR( "index file mapping failed!\n" );
		exit( -1 );
	}
    //PRINT_DEBUG( "index array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
    vert_array_header = (struct vert_index *) memblock;

	//map edge files to edge_array_header
    fstat(edge_file_fd, &st);
    edge_file_length = (u64_t) st.st_size;

    if (edge_file_length >= (u64_t)THRESHOLD_GRAPH_SIZE)
        gen_config.prev_update = false;
    else
        gen_config.prev_update = true;

    //in-memory
    /*
    memblock = (char *)malloc(edge_file_length);
    read_in_byte = read(edge_file_fd, memblock, edge_file_length); 
    if(read_in_byte != edge_file_length)
    {
        PRINT_ERROR("read edge fail!\n");
    }
    edge_array_header = (T *) memblock;
    */

    //PRINT_DEBUG( "edge list file size:%lld(MBytes)\n", edge_file_length/(1024*1024) );
    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, edge_file_fd, 0 );
    if( memblock == MAP_FAILED ){
        PRINT_ERROR( "edge file mapping failed!\n" );
		exit( -1 );
	}
    //PRINT_DEBUG( "edge array mmapped at virtual address:0x%llx\n", (u64_t)memblock );
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

        //in-memory
        /*
        memblock = (char *)malloc(in_vert_index_file_length);
        u64_t read_in_byte = read(in_vert_index_file_fd, memblock, in_vert_index_file_length); 
        if(read_in_byte != in_vert_index_file_length)
        {
            PRINT_ERROR("read in_vert_index fail!\n");
        }
        in_vert_array_header = (struct vert_index *) memblock;
        */

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
        
        //in-memory
        /*
        memblock = (char *)malloc(in_edge_file_length);
        read_in_byte = read(in_edge_file_fd, memblock, in_edge_file_length); 
        if(read_in_byte != in_edge_file_length)
        {
            PRINT_ERROR("read in_edge fail!\n");
        }
        in_edge_array_header = (struct in_edge *) memblock;
        */

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
                if( vert_array_header[i].offset != 0L ){
                    end_edge = vert_array_header[i].offset -1;
                    break;
                }
                if (i == gen_config.max_vert_id)    //means this vertex is the last vertex which has out_edge 
                {
                    end_edge = gen_config.num_edges;
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
                if( in_vert_array_header[i].offset != 0L ){
                    end_edge = in_vert_array_header[i].offset -1;
                    break;
                }
                if (i == gen_config.max_vert_id)     //means this vertex is the last vertex which has in_edge 
                {
                    end_edge = gen_config.num_edges;
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

//the explicit instantiation part
//template class index_vert_array<type1_edge>;
//template class index_vert_array<type2_edge>;
