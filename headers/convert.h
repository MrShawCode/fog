#ifndef __CONVERT_H__
#define __CONVERT_H__

#define CUSTOM_EOF  	100
#define MAX_LINE_LEN   	1024 
#define EDGE_BUFFER_LEN 2048*2048
#define VERT_BUFFER_LEN 2048*2048

extern FILE * in;
extern int edge_file, vert_index_file;
extern unsigned int src_vert, dst_vert;

extern unsigned int min_vertex_id, max_vertex_id;
extern unsigned long long  num_edges;
extern unsigned long max_out_edges;

extern struct edge edge_buffer[EDGE_BUFFER_LEN];
extern struct vert_index vert_buffer[VERT_BUFFER_LEN];

char *get_adjline();
int flush_buffer_to_file( int fd, char* buffer, unsigned int size );
void process_adjlist(const char*, const char*, const char* );
void process_edgelist(const char*, const char*,const char* );
int read_one_edge( void );
float produce_random_weight();

#endif

