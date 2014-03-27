#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include "config.hpp"
#include "print_debug.hpp"

enum fog_engine_state{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM
};

//denotes the different status of cpu threads after they finished the given tasks.
// Note: these status are for scatter phase ONLY!
enum cpu_thread_status{
	UPDATE_BUF_FULL = 100,		//Cannot scatter more updates, since my update buffer is full
	NO_MORE_SCHED	//I have no more sched tasks, I finished the whole scatter phase
};

#define SCHED_BUFFER_LEN    1024

template <typename A, typename VA>
class cpu_thread;

class barrier {
    volatile unsigned long count[2];
    volatile unsigned long sense;
    unsigned long expected;
    public:
    barrier(unsigned long expected_in)
        :sense(0), expected(expected_in)
    {
        count[0] = 0;
        count[1] = 0;
    }

    void wait()
    {
        unsigned long sense_used = sense;
        unsigned long arrived =
            __sync_fetch_and_add(&count[sense_used], 1);
        if(arrived == (expected - 1)) {
            sense = 1 - sense_used; // Reverse sense
            count[sense_used] = 0;
        }
        while(count[sense_used] != 0);
        __sync_synchronize(); // Also clobber memory
    }
//    friend class cpu_thread<A,VA>;
};

struct init_param{
	char* attr_buffer_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
};

struct scatter_param{
	void* attr_array_head;
};

struct gather_param{

};

template <typename A, typename VA>
struct cpu_work{
	u32_t engine_state;
	void* state_param;

	cpu_work( u32_t state, void* state_param_in )
		:engine_state(state), state_param(state_param_in)
	{}
	
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index, 
		segment_config<VA>* seg_config, int *status )
	{
		u32_t local_start_vert_off, local_term_vert_off;
        sync->wait();
		
		switch( engine_state ){
			case INIT:
			{	//add {} to prevent "error: jump to case label" error. Cann't believe that!
				init_param* p_init_param = (init_param*) state_param;

				if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

				//compute loca_start_vert_id and local_term_vert_id
				local_start_vert_off = processor_id*(seg_config->partition_cap);

				if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
					local_term_vert_off = p_init_param->num_of_vertices - 1;
				else
					local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;
			
//				PRINT_DEBUG( "processor:%d, vert start from %u, number:%u local start from vertex %u to %u", 
//					processor_id, 
//					p_init_param->start_vert_id, 
//					p_init_param->num_of_vertices, 
//					local_start_vert_off, 
//					local_term_vert_off );

				//Note: for A::init, the vertex id and VA* address does not mean the same offset!
				for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
					A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buffer_head) + i );

				break;
			}
			case SCATTER:
			{
				scatter_param* p_scatter_param = (scatter_param*) state_param;
				sched_list_manager* my_sched_list_manager;
				update_map_manager* my_update_map_manager;
				u32_t my_strip_cap, per_cpu_strip_cap, min_laxity;
				u32_t* my_update_map_head;

				VA* attr_array_head;
				update<VA>* my_update_buffer_head;

				sched_task *p_task;
				edge* t_edge;
				update<VA> *t_update;
				u32_t num_out_edges, i;
				u32_t strip_num, cpu_offset, map_value, update_map_offset, update_buffer_offset;

//				PRINT_DEBUG( "processor:%d, parameter address:%llx", processor_id, (u64_t)p_scatter_param );

				my_sched_list_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;
				my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;
				my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
				per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
				min_laxity = per_cpu_strip_cap; //this is the maximum laxity
				my_update_map_head = my_update_map_manager->update_map_head;

				attr_array_head = (VA*) p_scatter_param->attr_array_head;
				my_update_buffer_head = (update<VA>*) seg_config->per_cpu_info_list[processor_id]->update_buffer_head;

				while( 1 ){
					p_task = get_sched_task( my_sched_list_manager );

					if( p_task == NULL ){
						*status = NO_MORE_SCHED;
						return;
					}

					PRINT_DEBUG( "processor:%d, task start:%u, task term:%u, remain task:%u\n", 
						processor_id, p_task->start, p_task->term, my_sched_list_manager->sched_task_counter );

					for( i=p_task->start; i<p_task->term; i++ ){
						//how many edges does "i" have?
						num_out_edges = vert_index->num_out_edges( i );
						if( num_out_edges == 0 ) continue;

						//should tell if the remaining space in update buffer is enough to store the updates?
						// if buffer full, should return.
						if( my_update_map_manager->max_margin_value < (num_out_edges) ){
							PRINT_DEBUG( "Processor %d: update full with max laxity=%u, current out edgs=%u. i=%u\n",
								processor_id, my_update_map_manager->max_margin_value, num_out_edges, i );
							*status = UPDATE_BUF_FULL;
							p_task->start = i;
							return;
						}
					
						for( u32_t j=0; j<num_out_edges; j++ ){
							t_edge = vert_index->out_edge( i, j );
							assert( t_edge ); //make sure the edge exists!

							t_update = A::scatter_one_edge( i, (VA*)&attr_array_head[i], num_out_edges, t_edge );
							assert( t_update );//make sure the update is not NULL

							//save t_update to update buffer;
				
//							strip_num = t_update->dest_vert/seg_config->segment_cap;
//							cpu_offset = (t_update->dest_vert%seg_config->segment_cap)/seg_config->partition_cap;
							strip_num = VID_TO_SEGMENT( t_update->dest_vert );
							cpu_offset = VID_TO_PARTITION( t_update->dest_vert );

							assert( strip_num < seg_config->num_segments );
							assert( cpu_offset < gen_config.num_processors );
							//update map layout
							//			cpu0				cpu1				.....
							//strip 0-> map_value(s0,c0)	map_value(s0, c1)	.....
							//strip 1-> map_value(s1,c0)	map_value(s1, c1)	.....
							//strip 2-> map_value(s2,c0)	map_value(s2, c1)	.....
							//		... ...
	
							map_value = *(my_update_map_head + strip_num*gen_config.num_processors + cpu_offset);

							if( map_value < per_cpu_strip_cap ){
								update_buffer_offset = strip_num*my_strip_cap + map_value*gen_config.num_processors + cpu_offset;

								*(my_update_buffer_head + update_buffer_offset) = *t_update;

								//update the map
								map_value ++;
								*(my_update_map_head + strip_num*gen_config.num_processors + cpu_offset) = map_value;

								//compute the laxity
								if( min_laxity > (per_cpu_strip_cap - map_value) )
									min_laxity = per_cpu_strip_cap - map_value;
							}else{
								PRINT_DEBUG( "Losing update to vertex %u at processor %d: max_laxity:%u, num of out edge:%u!\n", 
									t_update->dest_vert, processor_id, 
									my_update_map_manager->max_margin_value, num_out_edges );
							}

							//drop t_edge and t_update
							delete t_edge;
							delete t_update;
						}
						//update laxity
						if (my_update_map_manager->max_margin_value > min_laxity )
							my_update_map_manager->max_margin_value = min_laxity;
					}
					//tell if this task is finished or not, if not, update it to make it start from i
					if( i >= p_task->term )
						del_sched_task( my_sched_list_manager );
				}
				*status = NO_MORE_SCHED;
				break;
			}
			default:
				printf( "Unknow fog engine state is encountered\n" );
		}

        sync->wait();
	}

	//return current sched_task
	//return NULL when there is no more tasks
	sched_task * get_sched_task( sched_list_manager* sched_manager )
	{
		if( sched_manager->sched_task_counter == 0 ) return NULL;

		return sched_manager->current;
	}

	//delete current sched_task
	//note: Must be the current sched_task!
	// After deletion, current pointer should move forward by one
	void del_sched_task( sched_list_manager* sched_manager )
	{
		assert( sched_manager->sched_task_counter > 0 );

		sched_manager->sched_task_counter --;

		//move forward current pointer
        if( sched_manager->current >= (sched_manager->sched_buffer_head
                    + sched_manager->sched_buffer_size) )
                sched_manager->current = sched_manager->sched_buffer_head;
        else
                sched_manager->current++;

		//move forward head
		sched_manager->head = sched_manager->current;
	}
};

template <typename A, typename VA>
class cpu_thread {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	segment_config<VA>* seg_config;
	int status;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work<A,VA> * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA>* seg_config_in )
    :processor_id(processor_id_in), vert_index(vert_index_in), seg_config(seg_config_in)
    {   
        if(sync == NULL) { //as it is shared, be created for one time
	        sync = new barrier(gen_config.num_processors);
        }
    }   

    void operator() ()
    {
        do{
            sync->wait();
            if(terminate) {
	            break;
            }
            else {
	            (*work_to_do)(processor_id, sync, vert_index, seg_config, &status );

            sync->wait(); // Must synchronize before p0 exits (object is on stack)
            }
        }while(processor_id != 0);
    }

	sched_task* get_sched_task()
	{return NULL;}

	void browse_sched_list()
	{}

};

template <typename A, typename VA>
barrier * cpu_thread<A, VA>::sync;

template <typename A, typename VA>
volatile bool cpu_thread<A, VA>::terminate;

template <typename A, typename VA>
cpu_work<A,VA> * volatile cpu_thread<A, VA>::work_to_do;

#endif
