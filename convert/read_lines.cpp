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

#define MAX_LINE_LEN	128
#define CUSTOM_EOF	100

#define EDGE_BUFFER_LEN	1024*1024
#define VERT_BUFFER_LEN	1024*1024
#define VERTEX_INIT_BUFFER_LEN 1024*1024
#define VERTEX_STATE_BUFFER_LEN 1024*1024

struct edge{
	unsigned int src_vert;
	unsigned int dst_vert;
	float edge_weight;
}__attribute__ ((__packed__));

struct vertex_index{
	unsigned int vertex_id;
	unsigned long long offset;
}__attribute__ ((aligned(8)));

struct sssp_vertex{
    unsigned int predecessor;
    float vertex_weight;
}__attribute__ ((aligned(8)));

struct vertex_state{
    unsigned int vertex_update;
}__attribute__ ((aligned(4)));

char line_buffer[MAX_LINE_LEN];
FILE * in;
int edge_file, vert_index_file;
int vertex_init_buffer_file;
int vertex_state_buffer_file0;
int vertex_state_buffer_file1;
unsigned long long line_no=0;
unsigned int src_vert, dst_vert;
float random_weight ;

extern unsigned int min_vertex_id, max_vertex_id;
extern unsigned long long num_edges;
extern unsigned long tmp_max_out;
extern unsigned int vert_gap;

//when buffer is filled, write to the output type2 file
// and then, continue reading and populating.
struct edge edge_buffer[EDGE_BUFFER_LEN];
struct vertex_index vert_buffer[VERT_BUFFER_LEN];
struct sssp_vertex vertex_init_buffer[VERT_BUFFER_LEN];
struct vertex_state vertex_state_buffer[VERT_BUFFER_LEN];

void begin_process(const char* ,const char* ,const char*, const char * );
int read_line( void );
int flush_buffer_to_file( int fd, char* buffer, unsigned int size );

/*
 * Regarding the vertex indexing:
 * We will assume the FIRST VERTEX ID SHOULD be 0!
 * Although in real cases, this will not necessarily be true,
 * (in many real graphs, the minimal vertex id may not be 0)
 * the assumption we made will ease the organization of the vertex
 * indexing! 
 * Since with this assumption, vertices can be easily accessed by using suffix:
 * vertex_index[vertex_ID]
 */
void begin_process(const char* input_file_name,const char*edge_file_name, const char*vert_index_file_name , 
        const char * vertex_buffer_file_name, const char *vertex_state_buffer_file_name0, const char *vertex_state_buffer_file_name1 )
{
	unsigned int recent_src_vert=0;
	unsigned int vert_buffer_offset=0;
	unsigned int edge_buffer_offset=0;
    unsigned int vertex_init_offset=0;
    unsigned int vertex_state_offset=0;
	unsigned int edge_suffix=0;
	unsigned int vert_suffix=0;
    unsigned int vertex_init_suffix=0;
    unsigned int vertex_state_suffix=0;
    unsigned long tmp_out = 0 , prev_out = 0;
    
    srand((unsigned int)time(NULL));
	printf( "size of edge node:%ld, size of vertex index:%ld\n", 
		sizeof(struct edge), sizeof(struct vertex_index));
	
	in = fopen( input_file_name, "r" );
	if( in == NULL ){
		printf( "Cannot open the input graph file!\n" );
		exit(1);
	}

    //std::cout << "vertex_buffer_file_name : " << vertex_buffer_file_name << std::endl;
    vertex_init_buffer_file  = open(vertex_buffer_file_name, O_CREAT|O_WRONLY, S_IRUSR);
	if( vertex_init_buffer_file == -1 ){
		printf( "Cannot create vertex_buffer file:%s\nAborting..\n",
			vertex_buffer_file_name);
		exit( -1 );
	}

    vertex_state_buffer_file0  = open(vertex_state_buffer_file_name0, O_CREAT|O_WRONLY, S_IRUSR);
	if( vertex_state_buffer_file0 == -1 ){
		printf( "Cannot create vertex_state_buffer file:%s\nAborting..\n",
			vertex_state_buffer_file_name0);
		exit( -1 );
	}

    vertex_state_buffer_file1  = open(vertex_state_buffer_file_name1, O_CREAT|O_WRONLY, S_IRUSR);
	if( vertex_state_buffer_file1 == -1 ){
		printf( "Cannot create vertex_state_buffer file:%s\nAborting..\n",
			vertex_state_buffer_file_name1);
		exit( -1 );
	}
	//open the output files
	edge_file = open( edge_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( edge_file == -1 ){
		printf( "Cannot create edge list file:%s\nAborting..\n",
			edge_file_name );
		exit( -1 );
	}
	
	vert_index_file = open( vert_index_file_name, O_CREAT|O_WRONLY, S_IRUSR );
	if( vert_index_file == -1 ){
		printf( "Cannot create vertex index file:%s\nAborting..\n",
			vert_index_file_name );
		exit( -1 );
	}

	//parsing input file now.
	while ( read_line() != CUSTOM_EOF ){
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
		edge_buffer[edge_suffix].src_vert = src_vert;
		edge_buffer[edge_suffix].dst_vert = dst_vert;
		edge_buffer[edge_suffix].edge_weight = random_weight;
    //printf("%f\n" ,random_weight) ;

		if (edge_suffix == (EDGE_BUFFER_LEN-1)){
			//todo: flush the edge buffer to file
			flush_buffer_to_file( edge_file, (char*)edge_buffer,
				EDGE_BUFFER_LEN*sizeof(edge) );

			//increment the offset
			edge_buffer_offset += 1;
		}

		//HANDLE THE VERTEX IDs
		//is source vertex id continuous?
		if (src_vert != recent_src_vert){
            tmp_out = num_edges - prev_out;
            if (tmp_max_out < tmp_out)
                tmp_max_out = tmp_out;
            prev_out = num_edges;
            //printf("tmP_out = %ld\n", tmp_out);
            //printf("prev_out = %ld\n", prev_out);
            //printf("max_out = %ld\n", tmp_max_out);
			//add a new record in vertex id index
			if (src_vert >= (vert_buffer_offset+1) * VERT_BUFFER_LEN ){
				vert_buffer_offset += 1;
				//todo:flush the vertex id index to file
				flush_buffer_to_file( vert_index_file, (char*)vert_buffer,
					VERT_BUFFER_LEN*sizeof(vertex_index) );

			}
			vert_suffix = src_vert - vert_buffer_offset * VERT_BUFFER_LEN;
			vert_buffer[vert_suffix].vertex_id = src_vert;
			vert_buffer[vert_suffix].offset = num_edges;

			//find gap now
			if (src_vert != (recent_src_vert +1))
				//a gap is found!
				vert_gap += src_vert - recent_src_vert;
			//update the recent src vert id
			recent_src_vert = src_vert;
		}
	}//while EOF
    for (unsigned int index = min_vertex_id; index <= max_vertex_id; index++)
    {
        vertex_init_suffix = index - vertex_init_offset * VERTEX_INIT_BUFFER_LEN;
        vertex_state_suffix = index - vertex_state_offset * VERTEX_STATE_BUFFER_LEN;
        vertex_init_buffer[vertex_init_suffix].predecessor = (unsigned int )-1;
        vertex_init_buffer[vertex_init_suffix].vertex_weight = (float)(std::numeric_limits<double>::max());
        vertex_state_buffer[vertex_state_suffix].vertex_update = (unsigned int)-1;
		if (vertex_init_suffix == (VERTEX_INIT_BUFFER_LEN - 1)){
			flush_buffer_to_file( vertex_init_buffer_file, (char*)vertex_init_buffer,
				VERTEX_INIT_BUFFER_LEN * sizeof(sssp_vertex) );

			//increment the offset
			vertex_init_offset += 1;
		}

		if (vertex_state_suffix == (VERTEX_STATE_BUFFER_LEN - 1)){
			flush_buffer_to_file( vertex_state_buffer_file0, (char*)vertex_state_buffer,
				VERTEX_STATE_BUFFER_LEN * sizeof(vertex_state) );
			flush_buffer_to_file( vertex_state_buffer_file1, (char*)vertex_state_buffer,
				VERTEX_STATE_BUFFER_LEN * sizeof(vertex_state) );

			//increment the offset
			vertex_state_offset += 1;
		}
    }

	//should flush the remaining data of both edge buffer and vertex index buffer to file!
	flush_buffer_to_file( edge_file, (char*)edge_buffer,
				EDGE_BUFFER_LEN*sizeof(edge) );
	flush_buffer_to_file( vert_index_file, (char*)vert_buffer,
				VERT_BUFFER_LEN*sizeof(vertex_index) );
    flush_buffer_to_file( vertex_init_buffer_file, (char*)vertex_init_buffer,
        VERTEX_INIT_BUFFER_LEN * sizeof(sssp_vertex) );
    flush_buffer_to_file( vertex_state_buffer_file0, (char*)vertex_state_buffer,
        VERTEX_STATE_BUFFER_LEN * sizeof(vertex_state) );
    flush_buffer_to_file( vertex_state_buffer_file1, (char*)vertex_state_buffer,
        VERTEX_STATE_BUFFER_LEN * sizeof(vertex_state) );

	//close all files
	fclose( in );
	close( edge_file );
	close( vert_index_file );
	close( vertex_init_buffer_file);
	close( vertex_state_buffer_file0);
	close( vertex_state_buffer_file1);
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
int read_line( void )
{
    char* res;

    if(( res = fgets( line_buffer, MAX_LINE_LEN, in )) == NULL )
           return CUSTOM_EOF;
	line_no++;

	//skip the comments
	if( line_buffer[0] == '#' ) return 0;
        num_edges ++; 
    random_weight = 1.0 + (float)(10.0 * rand()/(RAND_MAX + 1.0));
    //printf("%f\n" ,random_weight) ;
        //parse the line buffer now
	sscanf( line_buffer, "%d\t%d\t%f\n", &src_vert, &dst_vert, &random_weight);
    
    return 0;
}

