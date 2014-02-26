#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "type.hpp"

#define NUM_PROCESSORS	4
#define DISK_THREAD_BEGIN_WITH	1024
#define MIN_VERT_TO_PROCESS		1024

class config{
	public:
		static u32_t min_vertex_id;
		static u32_t max_vertex_id;
		static u64_t num_edges; 

		static u32_t num_processors;
		static u64_t memory_size;

		static std::string graph_path;
		static std::string vertex_file_name;
		static std::string edge_file_name;
		static std::string attr_file_name;
};

#endif
