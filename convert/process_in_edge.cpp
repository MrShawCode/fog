#include <iostream>
#include <cassert>

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "convert.h"

int in_edge_fd;
typedef unsigned long long u64_t;
typedef unsigned int u32_t;

enum
{
    READ_FILE = 0,
    WRITE_FILE
};

void *map_anon_memory( unsigned long long size,
        bool mlocked,
        bool zero = false)
{
    void *space = mmap(NULL, size > 0 ? size:4096,
        PROT_READ|PROT_WRITE,
        MAP_ANONYMOUS|MAP_SHARED, -1, 0);
    printf( "Engine::map_anon_memory had allocated 0x%llx bytes at %llx\n", size, (unsigned long long)space);
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

void process_in_edge(unsigned long long mem_size,
        const char * edge_file_name)
{
    struct stat st;
    char * buf_for_sort;
    unsigned long long edge_file_size;
    //std::cout << "mem_size = (MB)" << mem_size/(1024) << std::endl;
    //open the edge file
    in_edge_fd = open(edge_file_name, O_RDONLY);
    if (in_edge_fd < 0)
    {
        printf("Cannot open edge_file : %s\n", edge_file_name);
        exit(-1);
    }
    fstat(in_edge_fd, &st);
    edge_file_size = (unsigned long long)st.st_size;
    close(in_edge_fd);
    printf( "edge file size:%lld(MBytes)\n", edge_file_size/(1024*1024) );
    
    //determine how many files to sort
    unsigned int num_parts;
    num_parts = ((edge_file_size)%(mem_size)) == 0 ? 
       (unsigned int)(edge_file_size/mem_size)
        :(unsigned int)(edge_file_size/mem_size + 1);
    unsigned long long per_file_size = mem_size;

    buf_for_sort = (char *)map_anon_memory(mem_size, true, true );
    std::cout << "address of buf_for_sort is " << buf_for_sort << std::endl;



    for (unsigned int i = 0; i < num_parts; i++)
    {
        unsigned int offset = i * per_file_size;
        unsigned int read_size = 0;
        if (i == num_parts - 1 && (edge_file_size%(mem_size) != 0))
            read_size = edge_file_size%mem_size;
        else
            read_size = per_file_size;

        std::cout << "do_io_work for offset:" << offset << ", read_size = " << read_size << std::endl;

        do_io_work(edge_file_name, READ_FILE, buf_for_sort, offset, read_size);

        //io_work * convert_io_work = NULL;
         //convert_io_work = new io_work(edge_file_name, FILE_READ, 
        //        buf_for_sort, offset, read_size);
        //fog_io_queue->add_io_task(convert_io_work);
        //fog_io_queue->wait_for_io_task(convert_io_work);
        //fog_io_queue->del_io_task(convert_io_work);
        //convert_io_work = NULL;
    }

    
}


