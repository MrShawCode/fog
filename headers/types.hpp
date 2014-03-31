#ifndef __TYPES_H__
#define __TYPES_H__

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
