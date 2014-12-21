/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He
 *
 * Declaration:
 *   The object for FOG engine
 *************************************************************************************************/

#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>

#include <time.h>

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "bitmap.hpp"
#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"
#include "print_debug.hpp"
#include "../fogsrc/cpu_thread.cpp"

#define THRESHOLD 0.8
#define MMAP_THRESHOLD 0.02

enum global_target
{
    GLOBAL_ENGINE = 0,
    TARGET_ENGINE,
    BACKWARD_ENGINE
};
//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA, typename U, typename T>
class fog_engine{

        //global variables
		static index_vert_array<T>* vert_index;

		static segment_config<VA> *seg_config;
        char * buf_for_write;

        static u32_t scatter_fog_engine_state;
        static u32_t gather_fog_engine_state;
        static u32_t init_fog_engine_state;
        static u32_t current_attr_segment;

        //io work queue
        static io_queue * fog_io_queue;

        cpu_thread<A,VA,U,T> ** pcpu_threads;
        boost::thread ** boost_pcpu_threads;

        u32_t * p_strip_count;

        //The reasons to use another mmaped file to access the attribute file (in SCATTER phase):
        //  It is really hard if not possible to arrange the attribute buffer by repeatively reading
        //  the attribute file in/replace, since there may be different status among the cpu threads.
        //  For ex., cpu0 may need to access segment 1, while other cpu threads need to access the 
        //  segment 2. 
        //  The other reason is that, since file reading and buffer replacing is done intermediatively,
        //  there will be (and must be) a waste at the last step. 
        //  Think about the case that cpu threads filled up their update buffer, and ready to finish
        //  their current SCATTER phase. But remember, at this time, there is another file reading 
        //  conducting on the background, which is useless and the following steps (i.e., GATHER)
        //  must wait till the completion of this background operation.
        int attr_fd;
        u64_t attr_file_length;
        VA *attr_array_header;

        int signal_of_partition_gather;
        u32_t global_or_target;

        time_t start_time;
        time_t end_time;

    public:
        fog_engine(u32_t global_target);
		~fog_engine();
        void operator() ();
        void print_attr_result();
        u32_t cal_true_bits_size(u32_t CONTEXT_PHASE);
        void show_all_sched_tasks();
		void init_phase(int global_loop);
        void set_signal_to_scatter(u32_t signal, u32_t processor_id, u32_t CONTEXT_PHASE);
        void set_signal_to_gather(u32_t signal, u32_t processor_id, u32_t CONTEXT_PHASE);
        int scatter_updates(u32_t CONTEXT_PHASE);
        void reset_target_manager(u32_t CONTEXT_PHASE);
        void reset_global_manager(u32_t CONTEXT_PHASE);
        u32_t rebalance_sched_bitmap(u32_t cpu_not_finished_id, u32_t CONTEXT_PHASE);
        void rebalance_sched_tasks(u32_t cpu_unfinished_id, u32_t CONTEXT_PHASE);
        void gather_updates(u32_t CONTEXT_PHASE, int phase);
        int lru_hit_target(int strip_id);
        u32_t get_free_buf_num();
        u32_t get_free_buf_id();
        char * get_target_buf_addr(int strip_id);
        void do_io_work(int strip_id, u32_t operation, char * io_buf, io_work* one_io_work);
        u32_t cal_strip_size(int strip_id, u32_t util_rate_signal, u32_t signal_threshold);
        u32_t global_return();
        u32_t cal_threshold();
        void show_update_map(int processor_id, u32_t * map_head);
        int map_attr_file();
        int unmap_attr_file();
        int map_write_attr_file();
        int remap_write_attr_file();
        int remap_attr_file();
        static void add_schedule(u32_t task_vid, u32_t CONTEXT_PHASE);
		void reclaim_everything();
		void show_target_sched_update_buf();
		void target_init_sched_update_buf();
        void show_global_sched_update_buf();
        void global_init_sched_update_buf();
        void add_sched_task_to_processor( u32_t processor_id, sched_task *task, u32_t task_len );
        void add_all_task_to_cpu( sched_task * task );
		static void *map_anon_memory( u64_t size,bool mlocked,bool zero = false);
};
template <typename A, typename VA, typename U, typename T>
index_vert_array<T> * fog_engine<A, VA, U, T>::vert_index;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::scatter_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::init_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::gather_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::current_attr_segment;

template <typename A, typename VA, typename U, typename T>
segment_config<VA> * fog_engine<A, VA, U, T>::seg_config;

template <typename A, typename VA, typename U, typename T>
io_queue * fog_engine<A, VA, U, T>::fog_io_queue;

#endif
		
