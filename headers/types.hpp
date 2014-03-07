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

//the data structure that manages the ring buffer containing all scheduled tasks
struct sched_list_manager{
    u32_t sched_buffer_size;    //the size of this buffer (unit is "sched_task")
    u32_t sched_task_counter;   //remember the number of tasks.
    sched_task *head, *tail, *current;
}__attribute__ ((aligned(8)));

//the data structure that manages the update buffer.
struct update_map_manager{
    u32_t max_margin_value; //remember how many updates can be stored in (which strip)
}__attribute__ ((aligned(8)));

#endif
