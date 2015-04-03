/**************************************************************************************************
 * Authors: 
 *   Jian He, Huiming Lv
 *
 * Routines:
 *   This method is borrowed from GraphChi.
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
#include <vector>

typedef unsigned int u32_t;
typedef unsigned long long u64_t;

int in_edge_file, in_vert_index_file;

u32_t vert_buffer_offset = 0;
u32_t edge_buffer_offset = 0;
u32_t edge_suffix = 0;
u32_t vert_suffix = 0;
u32_t recent_src_vert = UINT_MAX;
u64_t tmp_num_edges = 0;

template <typename T>
class minheap
{
    T * nodes;
    int max_size;
    int curr_size;
    public:
    minheap(int max_size)
    {
        this->max_size = max_size;
        this->nodes = (T*)calloc(this->max_size, sizeof(T));
        this->curr_size = 0;
    }
    ~minheap()
    {
        delete nodes;
    }

    int parent(int i)
    {
        return (i+1)/2-1;
    }
    int left(int i)
    {
        return 2*i+1;
    }
    int right(int i)
    {
        return 2*i+2;
    }

    void inc_size()
    {
        curr_size++;
        assert(curr_size<=max_size);
    }
    void dec_size()
    {
        curr_size--;
        assert(curr_size>=0);
    }
    bool isempty()
    {
        return curr_size==0;
    }

    void insert(T element)
    {
        inc_size();
        int pos = curr_size-1;
        for(; pos>0&&element<nodes[parent(pos)]; pos=parent(pos))
        {
            nodes[pos] = nodes[parent(pos)];
        }

        nodes[pos] = element;

    }
    T get_min()
    {
        return nodes[0];
    }
    void pop_min()
    {
        nodes[0] = nodes[curr_size-1];
        dec_size();
        modify(0);
    }
    void modify(int i)
    {
        int l = left(i);
        int r = right(i);
        int small_pos = i;
        if(l<curr_size && nodes[l]<nodes[i])
        {
            small_pos = l;
        }
        if(r<curr_size && nodes[r]<nodes[small_pos])
        {
            small_pos = r;
        }
        if(small_pos != i)
        {
            T temp = nodes[i];
            nodes[i] = nodes[small_pos];
            nodes[small_pos] = temp;
            modify(small_pos);
        }
    }
};


struct merge_source
{
    unsigned long long buf_edges;
    std::string file_name;
    unsigned long long idx;
    unsigned long long buf_idx;
    tmp_in_edge * buffer;
    int f;
    unsigned long long file_edges;

    merge_source(tmp_in_edge * buf, unsigned long long buffer_edges, std::string filename, unsigned long long file_size) 
    {
        buffer = buf;
        //printf("init add of buffer is %llx\n", (u64_t)buffer);
        buf_edges = buffer_edges;
        file_name = filename;
        idx = 0;
        buf_idx = 0;
        //std::cout<<"file_name = "<<file_name<<std::endl;
        //std::cout<<"file_size = "<<file_size<<std::endl;
        file_edges = (unsigned long long)(file_size / sizeof(tmp_in_edge)); 
        //f = open(file_name.c_str(), O_RDONLY);
        //if (f < 0)
        //{
        //    assert(false);
        //}
        load_next();
    }

    ~merge_source()
    {
        //if (buffer != NULL) free(buffer);
        //buffer = NULL;
    }

    void finish()
    {
        //close(f);
        //free(buffer);
       // buffer = NULL;
    }

    void read_data(/*int f,*/ tmp_in_edge* tbuf, u64_t nbytes, u64_t off )
    {
        char * buf = (char*)tbuf;
        int read_finished = 0, remain = nbytes, res;
        //printf("buf address : %llx\n", (u64_t)buf);
        //printf("end buf address : %llx\n", (u64_t)((char*)buf1+mem_size));
        int fd = open(file_name.c_str(), O_RDWR, S_IRUSR | S_IRGRP | S_IROTH);
        if (fd < 0)
        {
            assert(false);
        }

        if (lseek(fd, off, SEEK_SET) < 0)
        {
            printf( "Cannot seek the attribute file!\n");
            exit(-1);
        }

        while(read_finished < (int)nbytes)
        {
            //hejian debug
            //std::cout << "nbytes = " << nbytes << std::endl;
            //std::cout << "read_finished = " << read_finished<< std::endl;
            //std::cout << "off = " << off << std::endl;

            //std::cout << "remain = " << remain << std::endl;

            if( (res = read(fd, buf, remain)) < 0 )
            {
                //std::cout << "res = " << res << std::endl;
                printf( "Cannot seek the attribute file!\n");
                exit(-1);
            }
            //std::cout<<"res = "<< res <<std::endl; 
            read_finished += res;
            remain -= res;

            //std::cout<<"nread: "<<read_finished <<" nbytes: "<<nbytes<<std::endl;
        }
        //std::cout<<"read_data ok"<<std::endl;
        assert(read_finished <= (int)nbytes);
        close(fd);
    }
        
    void load_next()
    {
        //std::cout<<"load_next()     buf_edges:"<<buf_edges<<std::endl;
        //std::cout<<"buf_size = "<<buf_edges*sizeof(tmp_in_edge)<<std::endl;
        //std::cout<<"un_read_size = "<<(file_edges-idx)*sizeof(tmp_in_edge)<<std::endl;
        unsigned long long len = std::min(buf_edges*sizeof(tmp_in_edge), (file_edges - idx)*sizeof(tmp_in_edge));
        //std::cout<<"len = "<<len<<std::endl;
        read_data(/*f,*/ buffer, (u64_t)len, (u64_t)(idx * sizeof(tmp_in_edge)));
        buf_idx = 0;
    }

    bool has_more()
    {
        return idx < file_edges;
    }

    tmp_in_edge get_next()
    {
        if (buf_idx == buf_edges)
        {
            load_next();
        }
        idx++;
        if (idx == file_edges)
        {
            tmp_in_edge x = buffer[buf_idx++];
            finish();
            return x;
        }
        return buffer[buf_idx++];
    }
};

struct merge_sink
{
    u64_t bufsize_bytes;
    u64_t buf_edges;
    std::string file_name;
    tmp_in_edge * buffer;
    u64_t buf_idx;
    int f;
    FILE * tmp_txt_file;

    //lvhuiming debug
    unsigned int last_one;
    u64_t idx; 
    //debug end 

    merge_sink(u64_t bufsize_bytes, std::string file_name)
    {
        bufsize_bytes = bufsize_bytes;
        file_name = file_name;
        assert(bufsize_bytes % sizeof(tmp_in_edge) == 0);
        //tmp_txt_file = fopen(file_name.c_str(), "wt+");
        //if (tmp_txt_file == NULL)
        //{
        //    assert(false);
        //}

        buffer = (tmp_in_edge *) malloc(bufsize_bytes);
        buf_edges = bufsize_bytes / sizeof(tmp_in_edge);
        buf_idx = 0;

        //lvhuiming debug
        last_one = -1;
        idx = 0;
        //debug end
    }
    void Add(tmp_in_edge value)
    {
        //lvhuiming debug
        idx++;

        //final output
        u32_t src_vert = value.src_vert;
        u32_t dest_vert = value.dest_vert;
        //std::cout << "src->dst = " << src_vert << "->" << dest_vert << std::endl;
        tmp_num_edges ++;
        //set the type2_edge_buffer
        edge_suffix = tmp_num_edges - (edge_buffer_offset * EDGE_BUFFER_LEN);
        in_edge_buffer[edge_suffix].in_vert = src_vert;
        //write back if necessary
        if (edge_suffix == (EDGE_BUFFER_LEN - 1))
        {
            flush_buffer_to_file( in_edge_file, (char*)in_edge_buffer,
                    EDGE_BUFFER_LEN*sizeof(struct in_edge) );
            memset( (char*)in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct in_edge) );
            edge_buffer_offset += 1;
        }

        //fprintf(tmp_txt_file, "%d\t%d\t\n", value.src_vert, value.dest_vert);

        //is source vertex id continuous?
        if (dest_vert != recent_src_vert)
        {
            if (dest_vert >= (vert_buffer_offset + 1)*VERT_BUFFER_LEN)
            {
                vert_buffer_offset += 1;
                flush_buffer_to_file( in_vert_index_file, (char*)in_vert_buffer,
                        VERT_BUFFER_LEN*sizeof(struct vert_index) );
                memset( (char*)in_vert_buffer , 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );
            }
            vert_suffix = dest_vert - vert_buffer_offset * VERT_BUFFER_LEN;
            in_vert_buffer[vert_suffix].offset = tmp_num_edges;
            recent_src_vert = dest_vert;
        }
        //debug end
        buf_idx++; 
        if(buf_idx<=buf_edges) 
        {
            //buffer[buf_idx] = value;
            
            //lvhuiming debug
            if(value.dest_vert != last_one)
            {
                last_one = value.dest_vert;
                //std::cout<<value.dest_vert<<" is ok"<<std::endl;
            }
            //debug end

        }
        else
        {
            write_data();
            buf_idx = 0;
        }
    }
    void write_data()
    {

    }

    void finish()
    {
        //fclose(tmp_txt_file);
        free(buffer);
        flush_buffer_to_file( in_edge_file, (char*)in_edge_buffer,
                EDGE_BUFFER_LEN*sizeof(in_edge) );
        flush_buffer_to_file( in_vert_index_file, (char*)in_vert_buffer,
                VERT_BUFFER_LEN*sizeof(vert_index) );
        close(in_vert_index_file);
        close(in_edge_file);
        //buffer = NULL;
    }

};

struct value_source
{
    int source_id;
    tmp_in_edge value;
    value_source(int id, tmp_in_edge val) : source_id(id), value(val){}
    bool operator< (value_source &obj2)
    {
        return (this->value.dest_vert < obj2.value.dest_vert);
    }
};

class kway_merge
{
    std::vector<merge_source*> sources;
    merge_sink * sink;
    minheap<value_source> m_heap;
    int merge_num;
    public:
    kway_merge(std::vector<merge_source*> sources, merge_sink* sink) : sources(sources),sink(sink),m_heap(int(sources.size()))
    {
        this->merge_num = int(sources.size());
    }

    ~kway_merge()
    {
        sink = NULL;
    }

    void merge()
    {
        int active_sources = (int)sources.size();
        for(int i=0; i<active_sources; i++)
        {
            m_heap.insert(value_source(i, sources[i]->get_next()));
        }

        while(active_sources>0 || !m_heap.isempty())
        {
            value_source v = m_heap.get_min();
            m_heap.pop_min();
            //std::cout<<"pop_min: "<<v.value.dest_vert<<std::endl;
            if(sources[v.source_id]->has_more())
            {
                m_heap.insert(value_source(v.source_id, sources[v.source_id]->get_next()));
            }
            else
            {
                active_sources--;
            }
            sink->Add(v.value);
        }
        sink->finish();
    }
};

void do_merge()
{
    /*
    while(0 != (mem_size % sizeof(tmp_in_edge)))
    {
        mem_size--;
    }
    */
    //exit(-1);
    unsigned long long source_buf_size = ((mem_size/num_tmp_files)/sizeof(tmp_in_edge)) * sizeof(tmp_in_edge);
    unsigned long long buf_edges = source_buf_size/sizeof(tmp_in_edge);
    //std::cout<<"source_buf_size:"<<source_buf_size << std::endl;
    //std::cout<<"sizeof tmp_in_edge:"<<sizeof(tmp_in_edge)<<std::endl;
    //std::cout<<"buf_edges:"<<buf_edges<<std::endl;  
    unsigned long long sink_bufsize = 1024*1024;
    memset( (char*)in_edge_buffer, 0, EDGE_BUFFER_LEN*sizeof(struct in_edge) );
    memset( (char*)in_vert_buffer, 0, VERT_BUFFER_LEN*sizeof(struct vert_index) );
    std::vector<merge_source* > sources;
    for (unsigned int i = 0; i < num_tmp_files; i++)
    {
        //std::cout<<"source_"<<i<<std::endl;
        std::stringstream current_file_id;
        current_file_id << i;
        std::string current_file_name = std::string(prev_name_tmp_file) + current_file_id.str();
        //std::cout << "source_buf_size = " << source_buf_size;
        tmp_in_edge * buf = (tmp_in_edge *)((char *)buf1 + i*source_buf_size);
        sources.push_back(new merge_source(buf, buf_edges, current_file_name, file_len[i]) );
        //std::cout << "the size of sources :" << sources.size() << std::endl;

    }

    std::string in_edge_file_name = std::string(in_name_file);
    std::string in_index_file_name = std::string(in_name_file);
    in_edge_file_name += ".in-edge";
    in_index_file_name += ".in-index";
    
    //std::cout << "in_name_file = " << in_name_file << std::endl;
    //std::cout << "in_edge_file_name = " << in_edge_file_name << std::endl;
    //std::cout << "in_index_file_name = " << in_index_file_name << std::endl;

    in_edge_file = open( in_edge_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR );
    if( in_edge_file == -1 )
    {
        printf( "Cannot create edge list file:%s\nAborted..\n",
                in_edge_file_name.c_str() );
        exit( -1 );
    }

    in_vert_index_file = open( in_index_file_name.c_str(), O_CREAT|O_WRONLY, S_IRUSR );
    if( in_vert_index_file == -1 )
    {
        printf( "Cannot create vertex index file:%s\nAborted..\n",
                in_index_file_name.c_str() );
        exit( -1 );
    }
    
    std::string sink_file_name = std::string(prev_name_tmp_file);
    sink_file_name += "out.txt";
    merge_sink* sink = new merge_sink((u64_t)sink_bufsize, sink_file_name);

    kway_merge k_merger(sources, sink);
    k_merger.merge();
    //lvhuiming debug
    u64_t all_idx = 0;
    std::vector<merge_source* >::iterator iter;
    for (iter=sources.begin(); iter!=sources.end(); iter++)
    {
        all_idx += (*iter)->idx;
    }    
    std::cout<<"all in_file have a sum of "<<all_idx<<" edges."<<std::endl;
    std::cout<<"out_file has a sum of "<<sink->idx<<" edges."<<std::endl;
    //debug end
    for (unsigned int i = 0; i < num_tmp_files; i++)
    {
        std::stringstream delete_current_file_id;
        delete_current_file_id << i;
        std::string delete_current_file_name = std::string(prev_name_tmp_file) + delete_current_file_id.str();

        std::cout << "delete tmp file "  << delete_current_file_name << std::endl;
        char tmp[1024];
        sprintf(tmp,"rm -rf %s", delete_current_file_name.c_str());
        int ret = system(tmp);
        if (ret < 0)
            assert(false);
    }
}
