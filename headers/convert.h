/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He
 *
 * Declaration:
 *   Structures and prototypes for graph format conversion.
 *************************************************************************************************/

#ifndef __CONVERT_H__
#define __CONVERT_H__

#define CUSTOM_EOF  	100
#define MAX_LINE_LEN   	1024 
#define EDGE_BUFFER_LEN 2048*2048  
#define VERT_BUFFER_LEN 2048*2048

namespace convert
{

    struct out_edge_with_weight
    {
        unsigned int dest_vert;
        float edge_weight;
    }__attribute__((aligned(8)));

    struct out_edge_without_weight
    {
        unsigned int dest_vert;
        //float edge_weight;
    }__attribute__((aligned(8)));

    struct edge
    {
        unsigned int dest_vert;
        float edge_weight;
    }__attribute__((aligned(8)));

    struct type2_edge
    {
        unsigned int dest_vert;
    }__attribute__((aligned(4)));

    struct vert_index
    {
        unsigned long long offset;
    }__attribute__((aligned(8)));

    struct old_vert_index
    {
        unsigned int vert_id;
        unsigned long long offset;
    }__attribute__((aligned(8)));

    struct old_edge
    {
        unsigned int src_vert;
        unsigned int dest_vert;
        float edge_weight;
    }__attribute__((aligned(8)));

    struct tmp_in_edge
    {
        unsigned int src_vert;
        unsigned int dest_vert;
    }__attribute__((aligned(8)));

    struct in_edge
    {
        unsigned int in_vert;
    }__attribute__((aligned(4)));
}

char *get_adjline();
int flush_buffer_to_file( int fd, char* buffer, unsigned int size );
void process_adjlist(const char*, const char *, const char *, const char *, bool, bool);
void process_edgelist(const char*, const char *, const char *, const char *, bool, bool);
void radix_sort(struct convert::tmp_in_edge * , struct convert::tmp_in_edge * , unsigned long long, unsigned int);
void process_in_edge(unsigned long long, const char *, const char *);
void insert_sort_for_buf(unsigned int, unsigned int);
void wake_up_sort(unsigned int, unsigned long long, bool);
void hook_for_merge();
void do_merge();
int read_one_edge( void );
float produce_random_weight();

extern FILE * in;
extern int edge_file, vert_index_file;
extern unsigned int src_vert, dst_vert;

extern FILE * out_txt;
extern FILE * old_edge_file;

extern unsigned int min_vertex_id, max_vertex_id;
extern unsigned long long  num_edges;
extern unsigned long max_out_edges;

extern struct convert::edge edge_buffer[EDGE_BUFFER_LEN];
extern struct convert::vert_index vert_buffer[VERT_BUFFER_LEN];
extern struct convert::type2_edge type2_edge_buffer[EDGE_BUFFER_LEN];
extern struct convert::in_edge in_edge_buffer[EDGE_BUFFER_LEN];
extern struct convert::vert_index in_vert_buffer[VERT_BUFFER_LEN];

//global vars for in_edge
extern struct convert::tmp_in_edge * buf1;
extern unsigned long long each_buf_len;
extern unsigned long long each_buf_size;
extern unsigned long long current_buf_size; //used in process_edgelist(adjlist)
extern int file_id;
extern unsigned long long * file_len;
extern unsigned int num_tmp_files;
extern const char * prev_name_tmp_file;
extern unsigned long long mem_size;
extern const char * in_name_file;

#endif

