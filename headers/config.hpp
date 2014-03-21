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
		u32_t num_processors;
		u64_t memory_size;

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
((_vid%seg_config->segment_cap)/seg_config->partition_cap)

#define START_VID(_seg, _cpu)\
(_seg*seg_config->segment_cap + _cpu*seg_config->partition_cap)

#define TERM_VID(_seg, _cpu)\
(_seg*seg_config->segment_cap + (_cpu+1)*seg_config->partition_cap - 1)

//per-cpu data, arranged by address-increasing order
struct per_cpu_data{
	char* buffer_head;
	u64_t buffer_size;
	sched_list_manager* sched_manager;
	update_map_manager* update_manager;
	char* update_buffer_head;
	u64_t strip_len;
	u64_t strip_cap;	//how many updates will a strip store?
}__attribute__ ((aligned(8)));

template <typename VA>
class segment_config{
	public:
		//segments & partitions
		u32_t num_segments;
		u32_t segment_cap;
		u32_t partition_cap;

		//per-cpu data, a list with gen_config.num_processors elements.
		per_cpu_data** per_cpu_info_list;
		char* attr_buf0;
		char* attr_buf1;
	
        //show configuration data
		void show_config(const char* buffer_header)
		{
            PRINT_DEBUG( "==========Begin of config info============" );
            PRINT_DEBUG( "Fog_engine is created with following segment configurations:" );
            PRINT_DEBUG( "Sizeof vertex attribute:%lu", sizeof(VA) );
            PRINT_DEBUG( "Number of segments=%uSegment capacity:%u(vertices)", 
                num_segments, segment_cap );
            PRINT_DEBUG( "Partition capacity:%u", partition_cap );
            PRINT_DEBUG( "----------Addressing-------------" );
            PRINT_DEBUG( "Write buffer: buffer_header addr:0x%llx, size:0x%llx", (u64_t)buffer_header, gen_config.memory_size );
            PRINT_DEBUG( "attribute buffer0:0x%llx, size:0x%llx", (u64_t)attr_buf0 );
            PRINT_DEBUG( "attribute buffer1:0x%llx, size:0x%llx", (u64_t)attr_buf1 );
            PRINT_DEBUG( "PerCPU Buffer:" );
            for( u32_t i=0; i<gen_config.num_processors; i++ ){
                PRINT_DEBUG( "CPU:%d, Sched and Update buffer begins:0x%llx, size:0x%llx", 
                    i, (u64_t)per_cpu_info_list[i]->buffer_head, per_cpu_info_list[i]->buffer_size );
            }
            PRINT_DEBUG( "==========End of config info============" );
		}

		//calculate the configration for segments and partitions
		segment_config(const char* buffer_header)
		{
			//still assume the vertex id starts from 0
			u32_t num_vertices = gen_config.max_vert_id + 1;
			//we will divide the whole (write) buffer into 4 pieces
			// note: we will have dual buffer for attributes
			u64_t attr_buffer_size = gen_config.memory_size / 4; 
			//alignment according to the sizeof VA.
			attr_buffer_size = (attr_buffer_size + (sizeof(VA)-1)) & ~(sizeof(VA)-1);

			segment_cap = attr_buffer_size / sizeof(VA);
			partition_cap = segment_cap / gen_config.num_processors;
			num_segments = (num_vertices%segment_cap)?num_vertices/segment_cap+1:num_vertices/segment_cap;

			//arrange the buffer
			//the rear part of the buffer will be allocated to the attribute array
			attr_buf0 = (char*)((u64_t)buffer_header + gen_config.memory_size/2 );
			attr_buf1 = (char*)((u64_t)attr_buf0 + attr_buffer_size);

			u64_t per_cpu_buf_size = gen_config.memory_size /(2 * gen_config.num_processors);

			per_cpu_info_list = new per_cpu_data*[gen_config.num_processors];
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				per_cpu_info_list[i] = new per_cpu_data;
				per_cpu_info_list[i]->buffer_head = (char*)((u64_t)buffer_header+i*per_cpu_buf_size);
				per_cpu_info_list[i]->buffer_size = per_cpu_buf_size;
			}
			//show_config(buffer_header);
		}
};

#endif
