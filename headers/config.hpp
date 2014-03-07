#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "types.hpp"

#define DISK_THREAD_ID_BEGIN_WITH	1024
#define MIN_VERT_TO_PROCESS		1024

struct general_config{
		//description
		u32_t min_vertex_id;
		u32_t max_vertex_id;
		u64_t num_edges; 

		//sysconfig
		u32_t num_processors;
		u64_t memory_size;

		//input & output
		std::string graph_path;
		std::string vertex_file_name;
		std::string edge_file_name;
		std::string attr_file_name;
};

extern general_config gen_config;

template <typename VA>
class segment_config{
	public:
		//segments & partitions
		u32_t num_segments;
		u32_t segment_cap;
		u32_t partition_cap;

		//calculate the configration for segments and partitions
		segment_config()
		{
			//still assume the vertex id starts from 0
			u32_t num_vertices = gen_config.max_vertex_id + 1;
			//we will divide the whole (write) buffer into 4 pieces
			u64_t attr_buffer_size = gen_config.memory_size / 4;
			//alignment according to the sizeof VA.
			attr_buffer_size = (attr_buffer_size + (sizeof(VA)-1)) & ~(sizeof(VA)-1);

			segment_cap = attr_buffer_size / sizeof(VA);
			partition_cap = segment_cap / gen_config.num_processors;
			num_segments = (num_vertices%segment_cap)?num_vertices/segment_cap+1:num_vertices/segment_cap;
		}
};

#endif
