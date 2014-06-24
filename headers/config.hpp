#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "types.hpp"
#include "print_debug.hpp"


//general config stores the general configuration information, such as number of processors,
// description of the original graph.
struct general_config{
		//description
		u32_t min_vert_id;
		u32_t max_vert_id;
		u64_t num_edges; 
		u32_t max_out_edges;

		//sysconfig
		u32_t num_io_threads;
		u32_t num_processors;
		u64_t memory_size;

        //u32_t scope_of_attr;//add by hejian. In order to get the scope of the attribute 
        //0 -> small graph 
        //1-> big graph

		//input & output
		std::string graph_path;
		std::string vert_file_name;
		std::string edge_file_name;
		std::string attr_file_name;
};

extern general_config gen_config;

//=========== following definitions are for buffer partitioning and management ==========
#define DISK_THREAD_ID_BEGIN_WITH	1024
#define MIN_VERT_TO_PROCESS		1024

#define VID_TO_SEGMENT(_vid) \
(_vid/seg_config->segment_cap)

#define VID_TO_PARTITION(_vid) \
    (_vid%gen_config.num_processors)
//((_vid%seg_config->segment_cap)/seg_config->partition_cap)

#define START_VID(_seg, _cpu)\
(_seg*seg_config->segment_cap + _cpu*seg_config->partition_cap)

#define TERM_VID(_seg, _cpu)\
(_seg*seg_config->segment_cap + (_cpu+1)*seg_config->partition_cap - 1)

#define ROUND_DOWN(x, y)	((x/y)*y)
#define ROUND_UP(x, y)		(((x+(y-1))/y)*y)

//per-cpu data, arranged by address-increasing order
template <typename VA, typename S>
struct per_cpu_data{
	char* buf_head;
	u64_t buf_size;
	S * sched_manager;
	update_map_manager* update_manager;
	aux_update_buf_manager<VA>* aux_manager;
	char* aux_update_buf_head;
	char* strip_buf_head;
	u64_t strip_buf_len;
	u32_t strip_cap;		//how many updates will a (whole) strip store?
}__attribute__ ((aligned(8)));

template <typename VA, typename S>
class segment_config{
	public:
		//in theory, fog will divide the buffer into 5 pieces, however, 
		//	such division is subjected to change according to the actual graph size.
		//	theory_per_slice_size is used to conduct further judgement.
		u64_t theory_per_slice_size; 

		//segments and partitions
		u32_t num_segments;		//how many segments it will have?
		u32_t segment_cap;		//how many vertices can be stored in one segment?
		u32_t partition_cap;	//how many vertices will be treated as a partition?

		//we will divide the whole (write) buffer into 3 parts:
		//	---------------------------------
		//  |	sched_update (for each CPU)	|
		//	---------------------------------
		//	|	aux_update_buf(for each CPU)|
		//	---------------------------------
		//	|	attr_buf 					|
		//	---------------------------------
		char* sched_update_buf;
		u64_t sched_update_buf_len;

        //modified by hejian
		char* aux_update_buf ;
		u64_t aux_update_buf_len ;

		//possible value for num_attr_buf: 1 or 2
		//there are possibly two buffers (especially for large graph)
		//	if num_attr_buf==1, only attr_buf0 is used, 
		//		and attr_buf_len denotes the size of the buffer
		//	if num_attr_buf==2, attr_buf0 and attr_buf1 are used, 
		//		and attr_buf_len denotes the size of one buffer.
		u32_t num_attr_buf;
		char* attr_buf0;
		char* attr_buf1;
		u64_t attr_buf_len;

		//per-cpu data, a list with gen_config.num_processors elements.
		per_cpu_data<VA, S>** per_cpu_info_list;

        //Note:
        // Should show this configuration information at each run, especially when
        //  it is not debugging.
        // TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.	
		void show_config(const char* buf_head)
		{
            PRINT_DEBUG( "==========\tBegin of segment configuration info\t============\n" );
            PRINT_DEBUG( "(Whole) Buffer Head:0x%llx, (Whole) Buffer Size:0x%llx\n",
				(u64_t)buf_head, gen_config.memory_size );
            PRINT_DEBUG( "Number of Vertices:%u, Sizeof vertex attribute:%lu\n", gen_config.max_vert_id + 1, sizeof(VA) );
            PRINT_DEBUG( "Number of segments:%u\n", num_segments );
            PRINT_DEBUG( "Segment capacity:%u(vertices)\n", segment_cap );
            PRINT_DEBUG( "Number of Partitions:%u\n", gen_config.num_processors );
            PRINT_DEBUG( "Partition capacity:%u(vertices)\n", partition_cap );

            PRINT_DEBUG( "----------\tAddressing (ordered by logical address)\t-------------\n" );
            PRINT_DEBUG( "Sched_update buffer:0x%llx, size:0x%llx\n", (u64_t)sched_update_buf, sched_update_buf_len );
            PRINT_DEBUG( "PerCPU Buffer:\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ ){
                PRINT_DEBUG( "CPU:%d, sched_update buffer begins:0x%llx, size:0x%llx\n", 
                    i, (u64_t)per_cpu_info_list[i]->buf_head, per_cpu_info_list[i]->buf_size );
            }

			//PRINT_DEBUG( "Auxiliary update buffer:0x%llx, size:0x%llx\n", (u64_t)aux_update_buf, aux_update_buf_len );

			PRINT_DEBUG( "There are %u attribute buffer(s)\n", num_attr_buf );
			switch( num_attr_buf ){
				case 1:{
					PRINT_DEBUG( "Attribute buffer:0x%llx, size:0x%llx\n", (u64_t)attr_buf0, attr_buf_len );
					break;
				}
				case 2:{
					PRINT_DEBUG( "Attribute buffer0:0x%llx, size:0x%llx\n", (u64_t)attr_buf0, attr_buf_len );
					PRINT_DEBUG( "Attribute buffer1:0x%llx, size:0x%llx\n", (u64_t)attr_buf1, attr_buf_len );
					break;
				}
			}	
            PRINT_DEBUG( "==========\tEnd of segment configuration info\t============\n" );
		}

		//calculate the configration for segments and partitions
		segment_config(const char* buf_head)
		{
			//still assume the vertex id starts from 0
			u32_t num_vertices = gen_config.max_vert_id + 1;

			//fog will divide the whole (write) buffer into 5 pieces (in theory):
			//	following figure explains why there are 5 pieces
			//	---------------------------------		---
			//  |	sched_update (for each CPU)	|
			//	---------------------------------		sched_update_buf_len
			//  |	sched_update (for each CPU)	|
			//	---------------------------------		---
			//	|	aux_update_buf(for each CPU)|		aux_update_buf_len
			//	---------------------------------  		---
			//	|	attr_buf0					|
			//	---------------------------------  		graph_attr_size
			//	|	attr_buf1					|
			//	---------------------------------  		---
			// Note (in theory):
			//	1) sched_update occupies 2/5 of the whole buffer area
			//	2) aux_update_buf occupies the next 1/5 area
			//	3) attr_buf0 and attr_buf1 divide the remaining area equally, and thus
			//		organized as dual buffer to loading the vertex attribute data
			//	4) sched_update and aux_update_buf will be further divided among 
			//		all online CPUs, which will be done in fog_engine::init_sched_update_buffer()
			
			theory_per_slice_size = gen_config.memory_size / 5; 

			//make adjustments according to the actual size of the inputed graph and its attr file
			//round up num_vertices, according to the number of processors
			//	since the vertices will eventually be divided and processed by the CPUs
			u64_t graph_attr_size = (u64_t)(ROUND_UP( num_vertices, gen_config.num_processors ) * sizeof(VA));

			if( graph_attr_size < (2*theory_per_slice_size) ){	//small graph, only give one attr_buffer
				num_attr_buf = 1;
                //gen_config.scope_of_attr = 0;
                PRINT_DEBUG("This is a small graph!\n");
				attr_buf_len = graph_attr_size;
				attr_buf0 = (char*)((u64_t)buf_head + (gen_config.memory_size - graph_attr_size ) );

				num_segments = 1;
				segment_cap = graph_attr_size / sizeof(VA);
				partition_cap = segment_cap / (gen_config.num_processors);
			}else{	//big graph, will have two attribute buffers
				//the objective is to find a proper size for attribute buffer to store the segments
				// however, it is not to find an exact division for the whole attribute data! since
				// this can possibly make the attribute buffer very small.
				u64_t segment_size = ROUND_DOWN(theory_per_slice_size, (sizeof(VA)*gen_config.num_processors));
				u64_t old_remain, new_remain;

				old_remain = graph_attr_size % segment_size;
				if( old_remain != 0L ){
					PRINT_DEBUG( "segment_size=%llu, remain=%llu\n", segment_size, old_remain );
					for( segment_size -= sizeof(VA)*gen_config.num_processors; 
						 segment_size > 0; 
						 segment_size -= sizeof(VA)*gen_config.num_processors ){
						new_remain = graph_attr_size % segment_size;
						if( new_remain > old_remain){
							old_remain = new_remain;
							continue;
						}else{
							break;
						}
					}
					segment_size += sizeof(VA)*gen_config.num_processors;
				}

				num_attr_buf = 2;
                //gen_config.scope_of_attr = 1;
				attr_buf_len = segment_size;
				attr_buf0 = (char*)((u64_t)buf_head + (gen_config.memory_size - 2*segment_size ) );
				attr_buf1 = (char*)((u64_t)attr_buf0 + segment_size );

				num_segments = (graph_attr_size%segment_size)?(graph_attr_size/segment_size+1):graph_attr_size/segment_size ;
				segment_cap = segment_size / sizeof(VA);
				partition_cap = segment_cap / (gen_config.num_processors);
			}

			//the auxiliary update buffer, occupies one slice (i.e., 1/5 of whole buffer)
			//aux_update_buf_len = ROUND_DOWN( theory_per_slice_size, (sizeof(update<VA>)*gen_config.num_processors) );
			//aux_update_buf = (char*)((u64_t)buf_head + 
			//	(gen_config.memory_size - segment_cap*sizeof(VA)*num_attr_buf - aux_update_buf_len) );
			aux_update_buf = (char*)NULL;
            aux_update_buf_len = 0;

			//sched_udate buffer, its length should be the same as the size of remaining buffer
			sched_update_buf = (char*)buf_head;
			sched_update_buf_len = gen_config.memory_size - segment_cap*sizeof(VA)*num_attr_buf /*- aux_update_buf_len*/;

			//divide sched_update to each processor
			//make sure per_cpu_buf_size is aligned to 8 bytes, since the future
			//	headers will be stored here, and they are aligned by 8 bytes
			u64_t per_cpu_buf_size = ROUND_UP( (sched_update_buf_len / gen_config.num_processors), 8 );

			per_cpu_info_list = new per_cpu_data<VA, S>*[gen_config.num_processors];
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				per_cpu_info_list[i] = new per_cpu_data<VA, S>;
				per_cpu_info_list[i]->buf_head = (char*)((u64_t)buf_head+i*per_cpu_buf_size);
				per_cpu_info_list[i]->buf_size = per_cpu_buf_size;
			}
			show_config(buf_head);
		}
};

#endif
