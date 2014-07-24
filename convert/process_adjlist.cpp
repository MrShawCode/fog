#include <iostream>
#include <cassert>

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "convert.h"



float produce_random_weight()
{
    //return (1.0 + (float)(10.0 * rand()/(RAND_MAX + 1.0)));
    return ((float)(rand()/(RAND_MAX + 1.0)));
}

void process_adjlist(const char * input_file_name, 
		const char * edge_file_name, 
		const char * vert_index_file_name,
        const char * old_edge_file_name,
        const char * old_vert_index_file_name,
        const char * out_txt_file_name)
{
    char * res;
    unsigned int edge_buffer_offset = 0;
    unsigned int vert_buffer_offset = 0;
    unsigned int edge_suffix = 0;
    unsigned int vert_suffix = 0;
    unsigned int recent_src_vert = 0;

    srand((unsigned int)time(NULL));

    printf( "Start Processing %s.\nWill generate %s and %s in destination folder.\n", 
		input_file_name, edge_file_name, vert_index_file_name );

	in = fopen( input_file_name, "r" );
	if( in == NULL ){
		printf( "Cannot open the input graph file!\n" );
		exit(1);
	}

    out_txt = fopen(out_txt_file_name, "wt+");
    if (out_txt == NULL)
    {
        printf("Connnot open out_txt_file!\n");
        exit(1);
    }

   edge_file = open(edge_file_name, O_CREAT|O_WRONLY, S_IRUSR);
   if (edge_file == -1){
       printf("Cannot create edge list file:%s\nAborted..\n", edge_file_name);
       exit(-1);
   }

   vert_index_file = open( vert_index_file_name, O_CREAT|O_WRONLY, S_IRUSR );
   if (vert_index_file == -1){
       printf( "Cannot create vertex index file:%s\nAborted..\n", vert_index_file_name);
       exit(-1);
   }

    old_edge_file = open(old_edge_file_name, O_CREAT | O_WRONLY, S_IRUSR);
    if (old_edge_file == -1)
    {
       printf("Cannot create old edge list file:%s\nAborted..\n", old_edge_file_name);
       exit(-1);
    }

   old_vert_index_file = open( old_vert_index_file_name, O_CREAT|O_WRONLY, S_IRUSR );
   if (old_vert_index_file == -1){
       printf( "Cannot create old vertex index file:%s\nAborted..\n", old_vert_index_file_name);
       exit(-1);
   }

    memset((char *)vert_buffer, 0, VERT_BUFFER_LEN * sizeof(struct vert_index) );
	memset((char *)edge_buffer, 0, EDGE_BUFFER_LEN * sizeof(struct edge) );

    memset((char *)old_vert_buffer, 0, VERT_BUFFER_LEN * sizeof(struct old_vert_index) );
	memset((char *)old_edge_buffer, 0, EDGE_BUFFER_LEN * sizeof(struct old_edge) );

    while ((res = (char *) get_adjline()) != '\0')
    {
	    unsigned int index = 0;
	    unsigned int tmp_int = 0;
	    unsigned int break_signal = 0;
		unsigned int num_out_edges = 0;
        while (res[0] != '#' && res[index] != '\0')
        {
            sscanf((res + index), "%d[^ ]", &tmp_int);
            
            if (index == 0){//means this is the src_vert
                src_vert = tmp_int;

                if (src_vert < recent_src_vert){
                    printf("Edge order is not correct at line:%lld.Edge processing terminated.\n", num_edges);
                    fclose(in);
                    exit(-1);
                }
                if (src_vert < min_vertex_id) min_vertex_id = src_vert;
                if (src_vert > max_vertex_id) max_vertex_id = src_vert;
                //printf("src_vert = %d, and all these edges start from this node!\n", src_vert);
                if (src_vert >= (vert_buffer_offset + 1) * VERT_BUFFER_LEN){
                    vert_buffer_offset += 1;
                    //flush the vertex index array to file.
                    flush_buffer_to_file(vert_index_file, (char *)vert_buffer, VERT_BUFFER_LEN * sizeof(vert_index));
                    memset((char *)vert_buffer, 0, VERT_BUFFER_LEN * sizeof(struct vert_index));

                    flush_buffer_to_file(old_vert_index_file, (char *)old_vert_buffer, VERT_BUFFER_LEN * sizeof(old_vert_index));
                    memset((char *)old_vert_buffer, 0, VERT_BUFFER_LEN * sizeof(struct old_vert_index));
                }

                vert_suffix = src_vert - vert_buffer_offset * VERT_BUFFER_LEN;

                vert_buffer[vert_suffix].offset = num_edges + 1;

                old_vert_buffer[vert_suffix].offset = num_edges + 1;
                old_vert_buffer[vert_suffix].vert_id = src_vert;
            }

            if (index > 0){//means this is the dest_node of the src_node
                dst_vert = tmp_int;
                num_edges++;
                edge_suffix = num_edges - (edge_buffer_offset * EDGE_BUFFER_LEN);
                edge_buffer[edge_suffix].dest_vert = dst_vert;
                edge_buffer[edge_suffix].edge_weight = produce_random_weight();

                old_edge_buffer[edge_suffix].src_vert = src_vert;
                old_edge_buffer[edge_suffix].dest_vert = dst_vert;
                old_edge_buffer[edge_suffix].edge_weight = edge_buffer[edge_suffix].edge_weight;

                fprintf(out_txt, "%d\t%d\t%f\n", src_vert, dst_vert, edge_buffer[edge_suffix].edge_weight);

				num_out_edges ++;
                if (edge_suffix == (EDGE_BUFFER_LEN - 1)){
                    flush_buffer_to_file(edge_file, (char *)edge_buffer, EDGE_BUFFER_LEN * sizeof(edge));
					memset( edge_buffer, 0, EDGE_BUFFER_LEN * sizeof(struct edge) );

                    flush_buffer_to_file(old_edge_file, (char *)old_edge_buffer, EDGE_BUFFER_LEN * sizeof(old_edge));
					memset( old_edge_buffer, 0, EDGE_BUFFER_LEN * sizeof(struct old_edge) );

                    edge_buffer_offset += 1;
                }

                if (dst_vert < min_vertex_id)  min_vertex_id = dst_vert;
                if (dst_vert > max_vertex_id)  max_vertex_id = dst_vert;
            }

            while (res[index] != ' ')
            {
                if (res[index] == '\n')
                {
                    free(res);
                    break_signal = 1; 
                    break;
                }
                index++;
            }
            //printf("%d\n", index);
            index++;
            if (break_signal == 1)
                break;
        }//while till EOL
        
        recent_src_vert = src_vert;
		if( num_out_edges > max_out_edges ) max_out_edges = num_out_edges;
	
    }//while till EOF

	//flush the remaining data in vertex index and edge buffer to file
    flush_buffer_to_file (edge_file, (char *)edge_buffer, EDGE_BUFFER_LEN * sizeof(edge));
    flush_buffer_to_file (vert_index_file, (char *)vert_buffer, VERT_BUFFER_LEN * sizeof(vert_index));

    flush_buffer_to_file (old_edge_file, (char *)old_edge_buffer, EDGE_BUFFER_LEN * sizeof(old_edge));
    flush_buffer_to_file (old_vert_index_file, (char *)old_vert_buffer, VERT_BUFFER_LEN * sizeof(old_vert_index));

	if (res != NULL)
		free(res);
	fclose( in );
    fclose(out_txt);
    close(edge_file);
    close(vert_index_file);
    close(old_edge_file);
    close(old_vert_index_file);
}

char *get_adjline()
{  
    char *zLine;  
    int nLine;  
    int n;  
    int eol;  
  
    nLine = MAX_LINE_LEN;
    zLine = (char *)malloc( nLine );  
    if( zLine==0 ) return 0;  
    n = 0;  
    eol = 0;  
    while( !eol ){  
        if( (n+10) > nLine ){  
            nLine = nLine*2 + MAX_LINE_LEN/2;  
            zLine = (char *)realloc(zLine, nLine);  
            if( zLine == NULL ){
                printf("get_adjline: memory allocation error!\n");
                return NULL;
            }
        }  
        if( fgets(&zLine[n], nLine - n, in)==0 ){  
            if( n == 0 ){ //empty line?
                free(zLine);  
                return NULL;
            }  
            zLine[n] = 0;  
            eol = 1;  
            break;  
        }  
        //printf("%s\n",zLine);
        //printf("first_n = %d\n", n);
        while( zLine[n] ){ n++; }  
        //printf("second_n = %d\n", n);
        if( n>0 && zLine[n-1]=='\n' ){  
            //n--;  
            //zLine[n] = 0;  
            eol = 1;  
            //printf("%s", zLine);
            //zLine = (char *)realloc( zLine, n+1 );  
            return zLine;
        }  
    }  
    zLine = (char *)realloc( zLine, n+1 );  
    printf("%s\n", zLine);
    return zLine;  
} 

