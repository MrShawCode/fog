#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "type.hpp"

class config{
	public:
		u32_t min_vertex_id;
		u32_t max_vertex_id;
		u64_t num_edges; 

		u32_t num_processors;
		u64_t memory_size;

		std::string graph_path;
		std::string vertex_file_name;
		std::string edge_file_name;
};

#endif
