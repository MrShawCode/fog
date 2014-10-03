#ifndef __TYPES_H__
#define __TYPES_H__

#include "bitmap.hpp"

typedef unsigned int u32_t;
typedef unsigned long long u64_t;

const float EPSILON = 1.00e-03;

#define FLOAT_EQ(x, v)(((v - EPSILON) < x) && (x < (v + EPSILON)))

//element in edge array
struct edge{
    u32_t dest_vert;                                         
    float edge_weight;
}__attribute__ ((aligned(8)));                                     

struct in_edge
{
    u32_t src_vert;
    u32_t get_dest_value(){return (u32_t)-1;}
    u32_t get_src_value(){return src_vert;}
    float get_edge_value(){return (float)-1;}
}__attribute__((aligned(4)));

struct type1_edge
{
    u32_t dest_vert;
    float edge_weight;
    u32_t get_dest_value()
    {
        return dest_vert;
    }
    float get_edge_value()
    {
        return edge_weight;
    }
    u32_t get_src_value(){return (u32_t)-1;}
}__attribute__((aligned(8)));

struct type2_edge
{
    u32_t dest_vert;
    u32_t get_dest_value()
    {
        return dest_vert;
    }
    float get_edge_value()
    {
        return (float)-1;
    }
    u32_t get_src_value(){return (u32_t)-1;}
}__attribute__((aligned(4)));

//vertex indexing (element in vertex array)
struct vert_index{
    u64_t  offset;                                    
}__attribute__ ((aligned(8)));   

//update
template<typename VA>
struct update{
	u32_t dest_vert;
	VA vert_attr;
    //modified by hejian
    //VA vert_attr_msg;
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

    //context data
    struct sched_list_context_data * p_context_data;
}__attribute__ ((aligned(8)));

struct sched_list_context_data
{
    //normal-sched-vert-id
    u32_t normal_sched_min_vert;
    u32_t normal_sched_max_vert;
    u32_t normal_sched_vert_to_scatter;
    u32_t num_vert_to_scatter;
    
    //verte_id to store
    u32_t context_vert_id;
    //edge id to store
    u32_t context_edge_id;

    u32_t context_steal_max_vert;
    u32_t context_steal_min_vert;
    u32_t context_steal_num_vert;
    u32_t context_steal_edge_id;
    //to be continued
    u32_t signal_to_scatter;

    //gather param
    int partition_gather_strip_id;

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
    bool steal_special_signal;

    //data for partition-gather!
    int partition_gather_strip_id;
    int partition_gather_signal;

    //context-data for scc-usign
    bool will_be_updated;
    bitmap * next_p_bitmap_steal;
    u32_t next_steal_bits_true_size;
    u32_t next_steal_max_vert_id;
    u32_t next_steal_min_vert_id;

    u32_t signal_to_gather;

    //for backward-algorithm
    u32_t alg_per_bits_true_size;
    u32_t alg_per_min_vert_id;
    u32_t alg_per_max_vert_id;
}__attribute__ ((aligned(8)));

//manage the update buffer.
struct update_map_manager{
	u32_t* update_map_head;	//points to the beginning of the map
	u32_t update_map_size;	//the size of update map: num_of_segments*num_of_processors. IN BYTES!!!
}__attribute__ ((aligned(8)));

enum algorithm_phase
{
    FORWARD_TRAVERSAL = 0,
    BACKWARD_TRAVERSAL,
    FORWARD_BACKWARD_TRAVERSAL
};

enum iteration_control
{
    ITERATION_STOP = 0,
    ITERATION_CONTINUE
};
enum engine_control
{
    ENGINE_STOP = 0,
    ENGINE_CONTINUE
};
#endif
