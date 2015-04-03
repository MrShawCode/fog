/**************************************************************************************************
 * Authors: 
 *   Jian He,
 *
 * Routines:
 *   process in-edge
 *   
 *************************************************************************************************/

#include <iostream>
#include <sstream>
#include <cassert>

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

#include "convert.h"
using namespace convert;

typedef unsigned long long u64_t;
typedef unsigned int u32_t;
struct in_edge in_edge_buffer[EDGE_BUFFER_LEN];
struct vert_index in_vert_buffer[VERT_BUFFER_LEN];

FILE *in_edge_fd;
struct tmp_in_edge * buf1, *buf2;
u64_t each_buf_len;
u64_t each_buf_size; //How many edges can be stored in this buf
u32_t num_parts; //init to 0, add by bufs
u64_t *file_len;
struct tmp_in_edge * edge_buf_for_sort;
char * buf_for_sort;
char * tmp_out_dir;
char * origin_edge_file;
u32_t num_tmp_files;
const char * prev_name_tmp_file;
const char * in_name_file;

u64_t current_buf_size;
u64_t total_buf_size;
u64_t total_buf_len;
u32_t current_file_id;

enum
{
    READ_FILE = 0,
    WRITE_FILE
};

void *map_anon_memory( u64_t size,
        bool mlocked,
        bool zero = false)
{
    void *space = mmap(NULL, size > 0 ? size:4096,
            PROT_READ|PROT_WRITE,
            MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    printf( "Engine::map_anon_memory had allocated 0x%llx bytes at %llx\n", size, (u64_t)space);
    if(space == MAP_FAILED) {
        std::cerr << "mmap_anon_mem -- allocation " << "Error!\n";
        exit(-1);
    }
    if(mlocked) {
        if(mlock(space, size) < 0) {
            std::cerr << "mmap_anon_mem -- mlock " << "Error!\n";
        }
    }
    if(zero) {
        memset(space, 0, size);
    }
    return space;
}

void do_io_work(const char *file_name_in, u32_t operation, char* buf, u64_t offset_in, u64_t size_in)
{
    int fd;
    switch(operation)
    {
        case READ_FILE:
            {
                int read_finished = 0, remain = size_in, res;
                fd = open(file_name_in, O_RDWR, S_IRUSR | S_IRGRP | S_IROTH); 
                if (fd < 0)
                {
                    printf( "Cannot open attribute file for writing!\n");
                    exit(-1);
                }
                if (lseek(fd, offset_in, SEEK_SET) < 0)
                {
                    printf( "Cannot seek the attribute file!\n");
                    exit(-1);
                }
                while (read_finished < (int)size_in)
                {
                    if( (res = read(fd, buf, remain)) < 0 )
                    {
                        printf( "Cannot seek the attribute file!\n");
                        exit(-1);
                    }
                    read_finished += res;
                    remain -= res;
                }
                close(fd);
                break;
            }
        case WRITE_FILE:
            {
                int written = 0, remain = size_in, res;
                fd = open(file_name_in, O_RDWR, S_IRUSR | S_IRGRP | S_IROTH); 
                if (fd < 0)
                {
                    printf( "Cannot open attribute file for writing!\n");
                    exit(-1);
                }
                if (lseek(fd, offset_in, SEEK_SET) < 0)
                {
                    printf( "Cannot seek the attribute file!\n");
                    exit(-1);
                }
                while (written < (int)size_in)
                {
                    if( (res = write(fd, buf, remain)) < 0 )
                    {
                        printf( "Cannot seek the attribute file!\n");
                        exit(-1);
                    }
                    written += res;
                    remain -= res;
                }
                close(fd);
                break;
            }
    }
}

void process_in_edge(u64_t mem_size,
        const char * edge_file_name,
        const char * out_dir)
{
    /*struct stat st;
      u64_t edge_file_size;
    //open the edge file
    in_edge_fd = fopen(edge_file_name, "r");
    if (in_edge_fd < 0)
    {
    printf("Cannot open edge_file : %s\n", edge_file_name);
    exit(-1);
    }
    //fstat(in_edge_fd, &st);
    stat(edge_file_name, &st);
    edge_file_size = (u64_t)st.st_size;
    fclose(in_edge_fd);
    printf( "edge file size:%lld(MBytes)\n", edge_file_size/(1024*1024) );
    printf( "edge file size:%lld\n", edge_file_size );
    exit(-1);*/
    tmp_out_dir = new char[strlen(out_dir)+1];
    strcpy(tmp_out_dir, out_dir);

    origin_edge_file = new char[strlen(edge_file_name)+1];
    strcpy(origin_edge_file, edge_file_name);
    //determine how many files to sort
    /*u64_t per_file_size;
      if (mem_size >= (2*edge_file_size))
      {
      num_parts = 1;
      per_file_size = edge_file_size;
      }
      else
      {
      num_parts = ((edge_file_size)%(mem_size)) == 0 ? 
      (u32_t)(edge_file_size/mem_size)
      :(u32_t)(edge_file_size/mem_size + 1);
      per_file_size = mem_size/2;
      }*/

    num_parts = 0;
    each_buf_len = mem_size/2;
    each_buf_size = (u64_t)mem_size/(sizeof(struct tmp_in_edge)*2);
    current_buf_size = 0;
    current_file_id = 0;
    //std::cout << "each_buf_len = " << total_buf_len << std::endl;
    //std::cout << "each_buf_size = " << total_buf_size << std::endl;
    //std::cout << "current_buf_size = " << current_buf_size << std::endl;

    /*for (u32_t i = 0; i < num_parts; i++)
      {
      if (i == num_parts - 1 && (edge_file_size%(mem_size) != 0))
      file_len[i] = edge_file_size%mem_size;
      else
      file_len[i] = per_file_size;

      std::cout << "Init for each file:" << file_len[i] << std::endl;
      }*/

    buf_for_sort = (char *)map_anon_memory(mem_size, true, true );
    edge_buf_for_sort = (struct tmp_in_edge *)buf_for_sort;
    buf1 = (struct tmp_in_edge *)buf_for_sort;
    buf2 = (struct tmp_in_edge *)(buf_for_sort + each_buf_len);
    //printf("the address of edge_buf_for_sort is %llx\n", (u64_t)edge_buf_for_sort);
    //printf("the address of buf1 is %llx\n", (u64_t)buf1);
    //printf("the address of buf2 is %llx\n", (u64_t)buf2);
}

void wake_up_sort(u32_t file_id, u64_t buf_size, bool final_call)
{
    //std::cout << "in wakeup_sort, file_id = " << file_id;
    //std::cout <<", buf_size = " << buf_size << std::endl;



    //start sort for this buffer
    radix_sort(buf1, buf2, buf_size, max_vertex_id);
    //exit(-1);
    //for (u64_t i = 0; i < buf_size; i++)
    //{
    //    std::cout << "src_vert->dest_vert = " << (*(buf1+i)).src_vert << "->" << (*(buf1+i)).dest_vert <<std::endl;
    //}
    //std::cout << "buf_size = " << buf_size << std::endl;
    //std::cout << "max_vert_id = " << max_vertex_id << std::endl;
    if (final_call && file_id == 0)
    {
        //means there are enough memory for this sorting
        //Write the data back to file
        u32_t vert_buffer_offset = 0;
        u32_t edge_buffer_offset = 0;
        u32_t edge_suffix = 0;
        u32_t vert_suffix = 0;
        u32_t recent_src_vert = UINT_MAX;
        u64_t tmp_num_edges = 0;

        //open the in_edge_file and in_index file
        std::string tmp_out_in_edge (tmp_out_dir);
        std::string tmp_out_in_index(tmp_out_dir);
        tmp_out_in_edge += origin_edge_file;
        tmp_out_in_index += origin_edge_file;
        tmp_out_in_edge += ".in-edge";
        tmp_out_in_index += ".in-index";

        int tmp_out_in_edge_file = open( tmp_out_in_edge.c_str(), O_CREAT|O_WRONLY, S_IRUSR );
        if( tmp_out_in_edge_file == -1 )
        {
            printf( "Cannot create edge list file:%s\nAborted..\n",tmp_out_in_edge.c_str());
            exit( -1 );
        }
        int tmp_out_in_index_file = open( tmp_out_in_index.c_str(), O_CREAT|O_WRONLY, S_IRUSR );
        if( tmp_out_in_index_file == -1 )
        {
            printf( "Cannot create vertex index file:%s\nAborted..\n",tmp_out_in_index.c_str());
            exit( -1 );
        }

        memset( (char*)in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct in_edge) );
        memset( (char*)in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );
        //std::cout << "hrereer!" << std::endl;

        //read every edge
        for (u64_t i = 0; i < buf_size; i++)
        {
            //get an edge
            u32_t src_vert = (*(buf1+i)).src_vert;
            u32_t dest_vert = (*(buf1+i)).dest_vert;
            //std::cout << "src->dst = " << src_vert << "->" << dest_vert << std::endl;
            tmp_num_edges ++;

            //set the type2_edge_buffer
            edge_suffix = tmp_num_edges - (edge_buffer_offset * EDGE_BUFFER_LEN);
            in_edge_buffer[edge_suffix].in_vert = src_vert;

            //write back if necessary
            if (edge_suffix == (EDGE_BUFFER_LEN - 1))
            {
                flush_buffer_to_file( tmp_out_in_edge_file, (char*)in_edge_buffer,
                        EDGE_BUFFER_LEN*sizeof(struct in_edge) );
                memset( (char*)in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct in_edge) );

                edge_buffer_offset += 1;
            }

            //is source vertex id continuous?
            if (dest_vert != recent_src_vert)
            {
                if (dest_vert >= (vert_buffer_offset + 1)*VERT_BUFFER_LEN)
                {
                    vert_buffer_offset += 1;
                    flush_buffer_to_file( tmp_out_in_index_file, (char*)in_vert_buffer,
                            VERT_BUFFER_LEN*sizeof(struct vert_index) );
                    memset( (char*)in_vert_buffer , 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );
                }
                vert_suffix = dest_vert - vert_buffer_offset * VERT_BUFFER_LEN;
                in_vert_buffer[vert_suffix].offset = tmp_num_edges;

                recent_src_vert = dest_vert;
            }
        }

        flush_buffer_to_file( tmp_out_in_edge_file, (char*)in_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(in_edge) );
        flush_buffer_to_file( tmp_out_in_index_file, (char*)in_vert_buffer,
                VERT_BUFFER_LEN*sizeof(vert_index) );

        close(tmp_out_in_edge_file);
        close(tmp_out_in_index_file);

        /*std::string tmp_out_txt(tmp_out_dir);
          tmp_out_txt += origin_edge_file;
          tmp_out_txt += "-out.txt";
          FILE *tmp_out_file = fopen(tmp_out_txt.c_str(), "wt+");
          if (tmp_out_file == NULL)
          {
          assert(false);
          }

          for (u64_t i = 0; i < buf_size; i++)
          {
          fprintf(tmp_out_file, "%d\t%d\t\n", (*(buf1+i)).src_vert, (*(buf1+i)).dest_vert);
          }
          fclose(tmp_out_file);*/

    }
    else
    {
        std::stringstream str_file_id;
        str_file_id << file_id;
        std::string tmp_in_file_name(tmp_out_dir) ;
        tmp_in_file_name += origin_edge_file;
        tmp_in_file_name += "-tmp_in_edge_file_" + str_file_id.str();
        //std::cout << "file " << tmp_in_file_name << std::endl;
        int tmp_in_file = open( tmp_in_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR );
        if( tmp_in_file == -1 )
        {
            printf( "Cannot tmp_in_file: %s\nAborted..\n", tmp_in_file_name.c_str());
            exit( -1 );
        }
        flush_buffer_to_file( tmp_in_file, (char*)buf1, (buf_size*sizeof(tmp_in_edge)));
        //memset( (char*)buf1, 0,  total_buf_len);
        close(tmp_in_file);

        if (final_call)
        {
            num_tmp_files = file_id + 1;
            file_len = new u64_t[file_id + 1];
            for (u32_t i = 0; i <= file_id; i++)
            {
                if (i == file_id)
                    file_len[i] = buf_size*sizeof(tmp_in_edge);
                else
                    file_len[i] = each_buf_len;
            }

            for (u32_t j = 0; j <= file_id; j++)
                std::cout << "the size of tmp file[" <<j << "] is:" <<  file_len[j] << std::endl;
            std::string prev_tmp(tmp_out_dir) ;
            std::string tmp_out(tmp_out_dir) ;
            prev_tmp += origin_edge_file;
            tmp_out += origin_edge_file;
            in_name_file = tmp_out.c_str();
            prev_tmp += "-tmp_in_edge_file_";
            prev_name_tmp_file = prev_tmp.c_str();

            //munlock( buf_for_sort, mem_size);
            //munmap( buf_for_sort, mem_size);
            //char * buf_new = (char *)map_anon_memory(mem_size, true, true );
            //buf1 = (struct tmp_in_edge *)buf_new;
            //hook_for_merge();
            do_merge();
            //munlock( buf_new, mem_size);
            //munmap( buf_new, mem_size);
            munlock( buf_for_sort, mem_size);
            munmap( buf_for_sort, mem_size);
        }
    }
}

void hook_for_merge()
{
    for (u32_t i = 0; i < num_tmp_files; i++)
    {
        std::stringstream current_file_id;
        current_file_id << i;
        std::string current_file_name = std::string(prev_name_tmp_file) + current_file_id.str();
        //std::cout << current_file_name << std::endl;
    }
}

