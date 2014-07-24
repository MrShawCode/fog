#ifndef __TYPES_H__
#define __TYPES_H__

#include "bitmap.hpp"

typedef unsigned int u32_t;
typedef unsigned long long u64_t;

//element in edge array
struct edge{
    u32_t dest_vert;                                         
    float edge_weight;
}__attribute__ ((aligned(8)));                                     

//vertex indexing (element in vertex array)
struct vert_index{
    u64_t  offset;                                    
}__attribute__ ((aligned(8)));   

//update
template<typename VA>
struct update{
	u32_t dest_vert;
	VA vert_attr;
}__attribute__ ((__packed__));

//defined to schedule tasks
struct sched_task{
	u32_t start;
	u32_t term;
};

//manage the ring buffer containing all scheduled tasks
struct sched_list_manager{
	//fields that define the buffer
	sched_task* sched_buf_head;
    u32_t sched_buf_size;    //the size of this buffer (unit is "sched_task")

	//fields that define the list
    u32_t sched_task_counter;   //remember the number of tasks.
    sched_task *head, *tail, *current;
}__attribute__ ((aligned(8)));

struct sched_list_context_data
{
    //to be continued
}__attribute__((aligned(8)));

//manage the bitmap buffer, add by hejian
struct sched_bitmap_manager{
    struct context_data * p_context_data0;
    struct context_data * p_context_data1;
}__attribute__ ((aligned(8)));

struct context_data{
    bitmap * p_bitmap;
    bitmap * p_bitmap_steal;
    char * per_bitmap_buf_head;
    u32_t per_bitmap_buf_size;
    u32_t per_bits_true_size;
    u32_t per_min_vert_id;
    u32_t per_max_vert_id;
    u32_t per_num_edges;
    u32_t signal_to_scatter;// 0 stands for normal , 1 stands for the situation when update-buf is full, 2 stands for stealing from other processors 

    //data for stealed-cpus
    u32_t steal_max_vert_id;
    u32_t steal_min_vert_id;
    u32_t steal_context_edge_id;
    u32_t steal_virt_cpu_id;
    u32_t steal_num_virt_cpus;
    u32_t steal_bits_true_size;

    //data for partition-gather!
    int partition_gather_strip_id;
    int partition_gather_signal;
}__attribute__ ((aligned(8)));

//manage the update buffer.
struct update_map_manager{
	u32_t* update_map_head;	//points to the beginning of the map
	u32_t update_map_size;	//the size of update map: num_of_segments*num_of_processors. IN BYTES!!!
}__attribute__ ((aligned(8)));

//manage the auxiliary update buffer
template <typename VA>
struct aux_update_buf_manager{
	char* buf_head;
	u64_t buf_size;
	update<VA>* update_head;
	u32_t buf_cap;
	u32_t num_updates;
}__attribute__ ((aligned(8)));

#endif
