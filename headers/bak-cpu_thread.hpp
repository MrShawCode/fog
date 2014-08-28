#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include "config.hpp"
#include "print_debug.hpp"

enum fog_engine_state{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM,
    SCC_GLOBAL_INIT,
    SCC_TARGET_INIT,
    GLOBAL_SCATTER,
    TARGET_SCATTER,
    GLOBAL_GATHER,
    TARGET_GATHER,
    SCC_INIT,
    SCC_FORWARD_SCATTER,
    SCC_BACKWARD_SCATTER,
    SCC_FORWARD_IN_BACKWARD_SCATTER,
    SCC_FORWARD_GATHER,
    SCC_BACKWARD_GATHER,
    SCC_FORWARD_IN_BACKWARD_GATHER
};

//denotes the different status of cpu threads after they finished the given tasks.
// Note: these status are for scatter phase ONLY!
enum cpu_thread_status{
	UPDATE_BUF_FULL = 100,	//Cannot scatter more updates, since my update buffer is full
	NO_MORE_SCHED,			//I have no more sched tasks, but have updates in the auxiliary update buffer
	FINISHED_SCATTER		//I have no more sched tasks, and no updates in auxiliary update buffer.
	                        //	But still have updates in my strip update buffer. 
};

enum scatter_signal
{
    NORMAL_SCATTER = 0,
    CONTEXT_SCATTER,
    STEAL_SCATTER,
    SPECIAL_STEAL_SCATTER
};

enum gather_signal
{
    NORMAL_GATHER = 0,
    CONTEXT_GATHER,
    STEAL_GATHER
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
	char* attr_buf_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
    u32_t PHASE;//for target && scc algorithm
    u32_t scc_phase;
    //reset in future
    u32_t loop_counter;
};

struct scatter_param{
	void* attr_array_head;
    u32_t PHASE;
};

struct gather_param{
    void * attr_array_head;
    int strip_id;
    u32_t threshold;
    u32_t PHASE;
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
		//segment_config<VA, sched_list_context_data>* seg_config, int *status )
		//segment_config<VA, sched_list_manager>* seg_config, int *status )
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
			
//				PRINT_DEBUG( "processor:%d, vert start from %u, number:%u local start from vertex %u to %u\n", 
//					processor_id, 
//					p_init_param->start_vert_id, 
//					p_init_param->num_of_vertices, 
//					local_start_vert_off, 
//					local_term_vert_off );

				//Note: for A::init, the vertex id and VA* address does not mean the same offset!
				for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
					A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buf_head) + i );

				break;
			}
			case SCATTER:
			{
                *status = FINISHED_SCATTER;
				scatter_param* p_scatter_param = (scatter_param*) state_param;
				sched_list_context_data* my_sched_list_manager;
				update_map_manager* my_update_map_manager;
				u32_t my_strip_cap, per_cpu_strip_cap;
				u32_t* my_update_map_head;

				VA* attr_array_head;
				update<VA>* my_update_buf_head;

				edge* t_edge;
				update<VA> *t_update;
				u32_t num_out_edges;
				u32_t strip_num, cpu_offset, map_value, update_buf_offset;

				my_sched_list_manager = seg_config->per_cpu_info_list[processor_id]->global_sched_manager;
				my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;

				my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
				per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
				my_update_map_head = my_update_map_manager->update_map_head;

				attr_array_head = (VA*) p_scatter_param->attr_array_head;
				my_update_buf_head = 
					(update<VA>*)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

                u32_t signal_to_scatter = my_sched_list_manager->signal_to_scatter;
                u32_t min_vert = 0, max_vert = 0;
                u32_t old_edge_id;
                if (signal_to_scatter == NORMAL_SCATTER)
                {
                    min_vert = my_sched_list_manager->normal_sched_min_vert;
                    max_vert = my_sched_list_manager->normal_sched_max_vert;
                    PRINT_DEBUG("cpu %d normal scatter, min_vert = %d, max_vert = %d\n",
                            processor_id, min_vert, max_vert);
                }
                else if (signal_to_scatter == CONTEXT_SCATTER)
                {
                    min_vert = my_sched_list_manager->context_vert_id;
                    max_vert = my_sched_list_manager->normal_sched_max_vert;
                    PRINT_DEBUG("cpu %d context scatter, min_vert = %d, max_vert = %d\n",
                            processor_id, min_vert, max_vert);
                }
                else if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {
                    min_vert = my_sched_list_manager->context_steal_min_vert;
                    max_vert = my_sched_list_manager->context_steal_max_vert;
                    PRINT_DEBUG("cpu %d steal scatter, min_vert = %d, max_vert = %d\n",
                            processor_id, min_vert, max_vert);
                }

                if (my_sched_list_manager->num_vert_to_scatter == 0 
                        && signal_to_scatter != STEAL_SCATTER && signal_to_scatter != SPECIAL_STEAL_SCATTER)
                {
                    *status = FINISHED_SCATTER;
                    break;
                }
                if (my_sched_list_manager->context_steal_min_vert == 0 &&
                        my_sched_list_manager->context_steal_max_vert == 0 &&
                        (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER))
                {
                    *status = FINISHED_SCATTER;
                    break;
                }

                //for loop for every vertex in every cpu
                for (u32_t i = min_vert; i <= max_vert; i += gen_config.num_processors)
                {
                    num_out_edges = vert_index->num_out_edges(i);
                    if (num_out_edges == 0)
                    {
                        //different counter for different scatter-mode
                        if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                            my_sched_list_manager->context_steal_num_vert++;
                        else
                            my_sched_list_manager->num_vert_to_scatter--;

                        //jump to next loop
                        continue;
                    }
                    
                    //set old_edge_id for context-scatter
                    if ((signal_to_scatter == CONTEXT_SCATTER) && (i == my_sched_list_manager->context_vert_id))
                    {
                        old_edge_id = my_sched_list_manager->context_edge_id;
                    }
                    else if (signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        old_edge_id = my_sched_list_manager->context_steal_edge_id;
                    else
                        old_edge_id = 0;

                    //generating updates for each edge of this vertex
                    for (u32_t z = old_edge_id; z < num_out_edges; z++)
                    {
                        //get edge from vert_index
                        t_edge = vert_index->out_edge(i, z);
                        //Make sure this edge existd!
                        assert(t_edge);
                        t_update = A::scatter_one_edge(i, (VA*)&attr_array_head[i], num_out_edges, t_edge);
                        //Make sure this update existd!
                        assert(t_update);

                        strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                        cpu_offset = VID_TO_PARTITION(t_update->dest_vert);
                        //Check for existd!
                        assert(strip_num < seg_config->num_segments);
                        assert(cpu_offset < gen_config.num_processors);

                        //find out the corresponding value for update-buffer
                        map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);
                        if (map_value < (per_cpu_strip_cap - 1))
                        {
                            //There are enough space for the current update
                            update_buf_offset = strip_num * my_strip_cap + map_value * gen_config.num_processors + cpu_offset;
                            *(my_update_buf_head + update_buf_offset) = *t_update;
                            map_value++;
                            *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                        }
                        else
                        {
                            //There is no space for this update, need to store the context data
                            if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                            {
                                my_sched_list_manager->context_steal_min_vert = i;
                                my_sched_list_manager->context_steal_max_vert = max_vert;
                                my_sched_list_manager->context_steal_edge_id = z;
                                PRINT_DEBUG("In steal-scatter, update-buf is fulled, need to store the context data!\n");
                                PRINT_DEBUG("min_vert = %d, max_vert = %d, edge = %d\n", i, max_vert, z);
                            }
                            else
                            {
                                PRINT_DEBUG("other-scatter, update-buf is fulled, need to store the context data!\n");
                                my_sched_list_manager->context_vert_id = i;
                                my_sched_list_manager->context_edge_id = z;
                                my_sched_list_manager->partition_gather_strip_id = (int)strip_num;
                                PRINT_DEBUG("vert = %d, edge = %d, strip_num = %d\n", i, z, strip_num);
                            }
                            *status = UPDATE_BUF_FULL;
                            delete t_edge;
                            delete t_update;
                            break;
                        }
                        delete t_edge;
                        delete t_update;
                    }
                    if (*status == UPDATE_BUF_FULL)
                        break;
                    else
                    {
                        //need to set the counter
                        if (signal_to_scatter == CONTEXT_SCATTER && my_sched_list_manager->num_vert_to_scatter == 0)
                            PRINT_ERROR("i = %d\n", i);
                        if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                            my_sched_list_manager->context_steal_num_vert++;
                        else
                            my_sched_list_manager->num_vert_to_scatter--;
                    }
                }

                if (*status == UPDATE_BUF_FULL)
                {
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id, 
                                my_sched_list_manager->context_steal_num_vert);
                    else
                        PRINT_DEBUG("Processor %d has not finished scatter, has %d vertices to scatter~\n", processor_id,
                                my_sched_list_manager->num_vert_to_scatter);
                }
                else
                {
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id, 
                                my_sched_list_manager->context_steal_num_vert);
                    else
                    {
                        PRINT_DEBUG("Processor %d has finished scatter, and there is %d vertex to scatter\n", processor_id, 
                                my_sched_list_manager->num_vert_to_scatter);
                        if (my_sched_list_manager->num_vert_to_scatter != 0)
                        {
                            PRINT_ERROR("after scatter, num_vert_to_scatter != 0\n");
                        }
                    }
                    *status = FINISHED_SCATTER;
                }
				break;
			}
            case GATHER:
            {
                gather_param * p_gather_param = (gather_param *)state_param; 
                update_map_manager * my_update_map_manager;
                u32_t my_strip_cap;
                u32_t * my_update_map_head;
                VA * attr_array_head;
                update<VA> * my_update_buf_head;

                update<VA> * t_update;
                u32_t map_value, update_buf_offset;
                u32_t dest_vert;
                int strip_id;
                u32_t threshold;
                u32_t vert_index;

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                attr_array_head = (VA *)p_gather_param->attr_array_head;
                strip_id = p_gather_param->strip_id;
                threshold = p_gather_param->threshold;

                //Traversal all the buffers of each cpu to find the corresponding UPDATES
                for (u32_t buf_id = 0; buf_id < gen_config.num_processors; buf_id++)
                {
                    my_update_map_manager = seg_config->per_cpu_info_list[buf_id]->update_manager;
                    my_update_map_head = my_update_map_manager->update_map_head;
                    my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
                    map_value = *(my_update_map_head + strip_id * gen_config.num_processors + processor_id);
                    if (map_value == 0)
                        continue;

                    for (u32_t update_id = 0; update_id < map_value; update_id++)
                    {
                        update_buf_offset = strip_id * my_strip_cap + update_id * gen_config.num_processors + processor_id;
                        t_update = (my_update_buf_head + update_buf_offset);
                        assert(t_update);
                        dest_vert = t_update->dest_vert;
                        if (threshold == 1) 
                            vert_index = dest_vert%seg_config->segment_cap;
                        else
                            vert_index = dest_vert;
                            
                        A::gather_one_update(dest_vert, (VA *)&attr_array_head[vert_index], t_update);
                    }
                    map_value = 0;
                    *(my_update_map_head + strip_id * gen_config.num_processors + processor_id) = 0;
                }
                break;
            }
			default:
				printf( "Unknow fog engine state is encountered\n" );
		}

        sync->wait();
	}

/*
	//return value:
	//0: added successfully
	//-1: failed on adding
	int add_update_to_sched_update( 
				seg_config,
				map_head, 
				buf_head, 
				p_update, 
				per_cpu_strip_cap )
	{

		t_update = (update<VA>*)(my_aux_manager->update_head + i);
		strip_num = VID_TO_SEGMENT( t_update->dest_vert );
		cpu_offset = VID_TO_PARTITION( t_update->dest_vert );

//						assert( strip_num < seg_config->num_segments );
//						assert( cpu_offset < gen_config.num_processors );

		//update map layout
		//			cpu0				cpu1				.....
		//strip 0-> map_value(s0,c0)	map_value(s0, c1)	.....
		//strip 1-> map_value(s1,c0)	map_value(s1, c1)	.....
		//strip 2-> map_value(s2,c0)	map_value(s2, c1)	.....
		//		... ...
		map_value = *(my_update_map_head + strip_num*gen_config.num_processors + cpu_offset);

		if( map_value < per_cpu_strip_cap ){
			update_buf_offset = strip_num*per_cpu_strip_cap*gen_config.num_processors
				+ map_value*gen_config.num_processors 
				+ cpu_offset;

		*(my_update_buf_head + update_buf_offset) = *t_update;

		//update the map
		map_value ++;
	}
*/

    //void show_update_map( int processor_id, segment_config<VA, sched_list_manager>* seg_config, u32_t* map_head )
    //void show_update_map( int processor_id, segment_config<VA, sched_list_context_data>* seg_config, u32_t* map_head )
    void show_update_map( int processor_id, segment_config<VA>* seg_config, u32_t* map_head )
    {
        //print title
        PRINT_SHORT( "--------------- update map of CPU%d begin-----------------\n", processor_id );
        PRINT_SHORT( "\t" );
        for( u32_t i=0; i<gen_config.num_processors; i++ )
        PRINT_SHORT( "\tCPU%d", i );
        PRINT_SHORT( "\n" );

        for( u32_t i=0; i<seg_config->num_segments; i++ ){
            PRINT_SHORT( "Strip%d\t\t", i );
            for( u32_t j=0; j<gen_config.num_processors; j++ )
            PRINT_SHORT( "%d\t", *(map_head+i*(gen_config.num_processors)+j) );
            PRINT_SHORT( "\n" );
        }
        PRINT_SHORT( "--------------- update map of CPU%d end-----------------\n", processor_id );
    }

};

template <typename A, typename VA>
class cpu_thread {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	segment_config<VA>* seg_config;
	//segment_config<VA, sched_list_context_data>* seg_config;
	//segment_config<VA, sched_list_manager>* seg_config;
	int status;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work<A,VA> * volatile work_to_do;

    //cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA, sched_list_manager>* seg_config_in )
    //cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA, sched_list_context_data>* seg_config_in )
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
