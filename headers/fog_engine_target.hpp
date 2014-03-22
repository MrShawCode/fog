//fog_engine_target is defined for targeted queries, such as SSSP.
// characters of fog_engine_target:
// 1) Schedule list with dynamic size
// 2) need to consider merging and (possibly) the scheduled tasks
// 3) As the schedule list may grow dramatically, the system may need to consider dump 
//	(partial) of the list to disk (to alieviate pressure on buffer).

#ifndef __FOG_ENGINE_TARGET_H__
#define __FOG_ENGINE_TARGET_H__

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fog_engine_target{
		index_vert_array* vert_index;

		segment_config<VA> *seg_config;

	public:

		fog_engine_target(segment_config<VA> *seg_config_in)
			:seg_config( seg_config_in)
		{}
			
		~fog_engine_target()
		{}

		void operator() ()
		{}
};

#endif
