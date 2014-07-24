#include <iostream>
#include <cassert>
#include <limits>

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "convert.h"

#define LINE_FORMAT		"%d\t%d\n"

char line_buffer[MAX_LINE_LEN];
FILE * in;
FILE * out_txt;
int edge_file, vert_index_file;
//hejian-debug
int old_edge_file, old_vert_index_file;
unsigned long long line_no=0;
unsigned int src_vert, dst_vert;

//when buffer is filled, write to the output index/edge file
// and then, continue reading and populating.
struct edge edge_buffer[EDGE_BUFFER_LEN];
struct vert_index vert_buffer[VERT_BUFFER_LEN];

//hejian-debug
struct old_edge old_edge_buffer[EDGE_BUFFER_LEN];
struct old_vert_index old_vert_buffer[VERT_BUFFER_LEN];

/*
 * Regarding the vertex indexing:
 * We will assume the FIRST VERTEX ID SHOULD be 0!
 * Although in real cases, this will not necessarily be true,
 * (in many real graphs, the minimal vertex id may not be 0)
 * the assumption we made will ease the organization of the vertex
 * indexing! 
 * Since with this assumption, the out-edge offset of vertices
 * with vertex_ID can be easily accessed by using the suffix:
 * index_map[vertex_ID]
 */

void process_edgelist( const char* input_file_name,
		const char* edge_file_name, 
		const char* vert_index_file_name,
        const char * old_edge_file_name,
        const char * old_vert_index_file_name,
        const char * out_txt_file_name)
{
	unsigned int recent_src_vert=0;
	unsigned int vert_buffer_offset=0;
	unsigned int edge_buffer_offset=0;
	unsigned int edge_suffix=0;
	unsigned int vert_suffix=0;
    unsigned long long prev_out = 0;
    
    printf( "Start Processing %s.\nWill generate %s and %s in destination folder.\n", 
        input_file_name, edge_file_name, vert_index_file_name );
    //hejian-debug
    printf( "Start Processing %s.\nWill generate %s and %s in destination folder.\n", 
        input_file_name, old_edge_file_name, old_vert_index_file_name );

    srand((unsigned int)time(NULL));

	in = fopen( input_file_name, "r" );
	if( in == NULL ){
		printf( "Cannot open the input graph file!\n" );
		exit(1);
	}

    out_txt = fopen(out_txt_file_name, "wt+");
    if (out_txt == NULL)
    {
        printf("Cannot open the output txt file!\n");
        exit(-1);
    }

	edge_file = open( edge_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( edge_file == -1 ){
		printf( "Cannot create edge list file:%s\nAborted..\n",
			edge_file_name );
		exit( -1 );
	}
	
	old_edge_file = open( old_edge_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( old_edge_file == -1 ){
		printf( "Cannot create edge list file:%s\nAborted..\n",
			old_edge_file_name );
		exit( -1 );
    }

	vert_index_file = open( vert_index_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( vert_index_file == -1 ){
		printf( "Cannot create vertex index file:%s\nAborted..\n",
			vert_index_file_name );
		exit( -1 );
	}

	old_vert_index_file = open( old_vert_index_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( old_vert_index_file == -1 ){
		printf( "Cannot create vertex index file:%s\nAborted..\n",
			old_vert_index_file_name );
		exit( -1 );
    }

	memset( (char*)edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct edge) );
	memset( (char*)vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );

    //hejian-debug
    memset((char *)old_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct old_edge));
    memset((char *)old_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct old_vert_index));

	//parsing input file now.
	while ( read_one_edge() != CUSTOM_EOF ){
		//trace the vertex ids
		if( src_vert < min_vertex_id ) min_vertex_id = src_vert;
		if( dst_vert < min_vertex_id ) min_vertex_id = dst_vert;

		if( src_vert > max_vertex_id ) max_vertex_id = src_vert;
		if( dst_vert > max_vertex_id ) max_vertex_id = dst_vert;

		//vertex id disorder.
		if( src_vert < recent_src_vert ){
			printf( "Edge order is not correct at line:%lld. Edge prcessing terminated.\n", line_no );
			fclose( in );
			exit(1);
		}

		//HANDLE THE EDGES
		//fill in the edge buffer, as well as the vertex id buffer
		edge_suffix = num_edges - (edge_buffer_offset * EDGE_BUFFER_LEN);
		edge_buffer[edge_suffix].dest_vert = dst_vert;
		edge_buffer[edge_suffix].edge_weight = produce_random_weight();
        //write to out_txt file
        fprintf(out_txt, "%d\t%d\t%f\n", src_vert, dst_vert, edge_buffer[edge_suffix].edge_weight);

        //hejian-debug
		old_edge_buffer[edge_suffix].src_vert = src_vert;
		old_edge_buffer[edge_suffix].dest_vert = dst_vert;
		old_edge_buffer[edge_suffix].edge_weight = edge_buffer[edge_suffix].edge_weight;

		if (edge_suffix == (EDGE_BUFFER_LEN-1)){
			flush_buffer_to_file( edge_file, (char*)edge_buffer,
				EDGE_BUFFER_LEN*sizeof(struct edge) );
			memset( (char*)edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct edge) );

            //hejian-debug
			flush_buffer_to_file( old_edge_file, (char*)old_edge_buffer,
				EDGE_BUFFER_LEN*sizeof(struct old_edge) );
			memset( (char*)old_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct old_edge) );
			//increment the offset
			edge_buffer_offset += 1;
		}

		//is source vertex id continuous?
		if (src_vert != recent_src_vert){
            if ( max_out_edges < (num_edges - prev_out) )
                max_out_edges = num_edges - prev_out;
            prev_out = num_edges;
			//add a new record in vertex id index
			if (src_vert >= (vert_buffer_offset+1) * VERT_BUFFER_LEN ){
				vert_buffer_offset += 1;
				flush_buffer_to_file( vert_index_file, (char*)vert_buffer,
					VERT_BUFFER_LEN*sizeof(struct vert_index) );
				memset( (char*)vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );

                //hejian-debug
				flush_buffer_to_file( old_vert_index_file, (char*)old_vert_buffer,
					VERT_BUFFER_LEN*sizeof(struct old_vert_index) );
				memset( (char*)old_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct old_vert_index) );
			}
			vert_suffix = src_vert - vert_buffer_offset * VERT_BUFFER_LEN;
			vert_buffer[vert_suffix].offset = num_edges;

            //hejian-debug
            old_vert_buffer[vert_suffix].offset = num_edges;
            old_vert_buffer[vert_suffix].vert_id = src_vert;

			//update the recent src vert id
			recent_src_vert = src_vert;
		}
	}//while EOF

	//should flush the remaining data of both edge buffer and vertex index buffer to file!
	flush_buffer_to_file( edge_file, (char*)edge_buffer,
				EDGE_BUFFER_LEN*sizeof(edge) );
	flush_buffer_to_file( vert_index_file, (char*)vert_buffer,
				VERT_BUFFER_LEN*sizeof(vert_index) );

    //hejian-debug
	flush_buffer_to_file( old_edge_file, (char*)old_edge_buffer,
				EDGE_BUFFER_LEN*sizeof(old_edge) );
	flush_buffer_to_file( old_vert_index_file, (char*)old_vert_buffer,
				VERT_BUFFER_LEN*sizeof(old_vert_index) );

	//finished processing
	fclose( in );
    fclose(out_txt);
	close( edge_file );
	close( old_edge_file );
	close( vert_index_file );
	close( old_vert_index_file );
}

/*
 * this function will flush the content of buffer to file, fd
 * the length of the buffer should be "size".
 * Returns: -1 means failure
 * on success, will return the number of bytes that are actually 
 * written.
 */
int flush_buffer_to_file( int fd, char* buffer, unsigned int size )
{
    unsigned int n, offset, remaining, res;
    n = offset = 0;
    remaining = size;
    while(n<size){
        res = write( fd, buffer+offset, remaining);
		n += res;
		offset += res;
		remaining = size-n;
    }
	return n;
}


/* this function is simple: just read one line,
 * record the increased edge number, retrieve the source and destination vertex
 * leave further processing to its caller.
 * Further processing includes:
 * 1) trace the vertex ids, get the minimal and maximal vertex id.
 * 2) tell if the vertex ids are continuous or not, if not, record the id gap;
 * 3) save the edge buffer to the "type2" binary file, if the buffer is filled.
 * 4) prepare for the index file.
 */
int read_one_edge( void )
{
    char* res;

    if(( res = fgets( line_buffer, MAX_LINE_LEN, in )) == NULL )
           return CUSTOM_EOF;
	line_no++;

	//skip the comments
	if( line_buffer[0] == '#' ) return 0;
    num_edges ++; 
//	sscanf( line_buffer, "%d\t%d\n", &src_vert, &dst_vert);
	sscanf( line_buffer, LINE_FORMAT, &src_vert, &dst_vert);
    
    return 0;
}

