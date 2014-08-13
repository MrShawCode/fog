#ifndef __CPU_THREAD_TARGET_HPP__
#define __CPU_THREAD_TARGET_HPP__

#include "cpu_thread.hpp"
#include "bitmap.hpp"
#include "disk_thread.hpp"
#include <cassert>

#include "config.hpp"
#include "print_debug.hpp"
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream> 
#include <fcntl.h>


#define SCHED_BUFFER_LEN    1024

template <typename A, typename VA>
class cpu_thread_target;

struct init_param_target{
	char* attr_buf_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
    u32_t PHASE;
};

struct scatter_param_target{
	void* attr_array_head;
    u32_t PHASE;
};

struct gather_param_target{
    void * attr_array_head;
    u32_t PHASE;
    int strip_id;
    u32_t threshold;
};

template <typename A, typename VA>
struct cpu_work_target{
	u32_t engine_state;
	void* state_param;

	cpu_work_target( u32_t state, void* state_param_in)
		:engine_state(state), state_param(state_param_in)
	{
    }
	
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index, 
		segment_config<VA, sched_bitmap_manager>* seg_config, int *status )
	{
		u32_t local_start_vert_off, local_term_vert_off;
        sync->wait();
		
		switch( engine_state ){
			case INIT:
			{	//add {} to prevent "error: jump to case label" error. Cann't believe that!
				init_param_target* p_init_param = (init_param_target*) state_param;

				if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

				//compute loca_start_vert_id and local_term_vert_id
				local_start_vert_off = processor_id*(seg_config->partition_cap);

				if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
					local_term_vert_off = p_init_param->num_of_vertices - 1;
				else
					local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;
			
				//Note: for A::init, the vertex id and VA* address does not mean the same offset!
				for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
                {
					A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buf_head) + i, p_init_param->PHASE);
                }

				break;
			}
			case SCATTER:
			{
                //sync->wait();
                *status = FINISHED_SCATTER;
                scatter_param_target * p_scatter_param = (scatter_param_target *)state_param; 
                sched_bitmap_manager * my_sched_bitmap_manager;
                struct context_data * my_context_data;
                update_map_manager * my_update_map_manager;
                //aux_update_buf_manager<VA> * my_aux_manager;
                u32_t my_strip_cap, per_cpu_strip_cap;
                u32_t * my_update_map_head;

                VA * attr_array_head;
                update<VA> * my_update_buf_head;

                edge * t_edge;
                update<VA> * t_update;
                u32_t num_out_edges/*,  temp_laxity*/;
                u32_t strip_num, cpu_offset, map_value, update_buf_offset;
                bitmap * current_bitmap = NULL ;
                u32_t u32_bitmap_value;
                u32_t signal_to_scatter;
                u32_t old_edge_id;
                u32_t max_vert = 0, min_vert = 0;
                //u32_t use_buf_data = 0;

                my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;
                my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;
                //my_aux_manager = seg_config->per_cpu_info_list[processor_id]->aux_manager;

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
                my_update_map_head = my_update_map_manager->update_map_head;
                my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

                attr_array_head = (VA *)p_scatter_param->attr_array_head;


                my_context_data = p_scatter_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                    my_sched_bitmap_manager->p_context_data0;
                
                signal_to_scatter = my_context_data->signal_to_scatter;
                if (signal_to_scatter == 0 || signal_to_scatter == 1)
                {
                    current_bitmap = my_context_data->p_bitmap;
                    max_vert = my_context_data->per_max_vert_id;
                    min_vert = my_context_data->per_min_vert_id;
                }
                if (signal_to_scatter == 2 || signal_to_scatter == 3)
                {
                    //PRINT_DEBUG("Steal scatter\n");
                    current_bitmap = my_context_data->p_bitmap_steal;
                    max_vert = my_context_data->steal_max_vert_id;
                    min_vert = my_context_data->steal_min_vert_id;
                }

                if (my_context_data->per_bits_true_size == 0 && signal_to_scatter != 2 && signal_to_scatter != 3)
                {
                    *status = FINISHED_SCATTER;
                    break;
                }

                for (u32_t i = min_vert; i <= max_vert; i = i + gen_config.num_processors)
                {
                    /*u32_t tmp_index = 0;
                    if (signal_to_scatter == 2)
                        tmp_index = VID_TO_BITMAP_INDEX(i, my_context_data->steal_virt_cpu_id, gen_config.num_processors);
                    else
                        tmp_index = VID_TO_BITMAP_INDEX(i, processor_id, gen_config.num_processors);
                    if ((tmp_index%(sizeof(unsigned char)*8)) == 0 && 
                            current_bitmap->get_u8_value(tmp_index) == 0)
                    {
                        i += (sizeof(unsigned char)*8 - 1) * gen_config.num_processors;//for loop
                        //if ((i+gen_config.num_processors) <= max_vert)
                            continue;
                        //else
                         //   break;
                    }*/
                    u32_bitmap_value = i;
                    if (current_bitmap->get_value(u32_bitmap_value) == 0)
                        continue;
                    /*u32_t tmp_strip_num = VID_TO_SEGMENT(u32_bitmap_value);
                    if (signal_to_scatter == 1)
                    {
                        if ((int)tmp_strip_num == seg_config->buf0_holder && seg_config->buf0_holder != -1)
                        {
                            attr_array_head = (VA *)seg_config->attr_buf0;
                            use_buf_data = 1;
                        }
                        else if ((int)tmp_strip_num == seg_config->buf1_holder && seg_config->buf1_holder != -1)
                        {
                            attr_array_head = (VA *)seg_config->attr_buf1;
                            use_buf_data = 1;
                        }
                        else
                            attr_array_head = (VA *)p_scatter_param->attr_array_head;
                    }*/
                    num_out_edges = vert_index->num_out_edges(u32_bitmap_value);
                    if (num_out_edges == 0 )
                    {
                        current_bitmap->clear_value(u32_bitmap_value);
                        if (signal_to_scatter == 2 || signal_to_scatter == 3)
                            my_context_data->steal_bits_true_size++;
                        else 
                            my_context_data->per_bits_true_size--;
                        continue;
                    }

                    if (signal_to_scatter == 0)
                        old_edge_id = 0;
                    else if ((signal_to_scatter == 1) && (i == my_context_data->per_min_vert_id))
                        old_edge_id = my_context_data->per_num_edges;
                    else if (signal_to_scatter == 3)
                        old_edge_id = my_context_data->steal_context_edge_id;
                    else
                        old_edge_id = 0;
                    
                    /*u32_t buf_index = -1;
                    if (use_buf_data == 1)
                        buf_index = u32_bitmap_value%seg_config->segment_cap;
                    if (use_buf_data == 0)
                        buf_index = u32_bitmap_value;*/
                    for (u32_t z = old_edge_id; z < num_out_edges; z++)
                    {
                        t_edge = vert_index->out_edge(u32_bitmap_value, z);
                        assert(t_edge);//Make sure this edge existd!
                        t_update = A::scatter_one_edge(u32_bitmap_value, (VA *)&attr_array_head[u32_bitmap_value], t_edge);
                        //t_update = A::scatter_one_edge(u32_bitmap_value, (VA *)&attr_array_head[buf_index], t_edge);
                        assert(t_update);

                        strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                        cpu_offset = VID_TO_PARTITION(t_update->dest_vert );
                        assert(strip_num < seg_config->num_segments);
                        assert(cpu_offset < gen_config.num_processors);
                        map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);

                        if (map_value < per_cpu_strip_cap)
                        {
                            update_buf_offset = strip_num * my_strip_cap + map_value * gen_config.num_processors + cpu_offset;
                            *(my_update_buf_head + update_buf_offset) = *t_update;
                            //update the map
                            map_value++;
                            *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                        }
                        else
                        {
                            //A new method to handle the situation when some strip_buf is FULL
                            //PRINT_DEBUG("Currently, the strip_num = %d, cpu_offset = %d\n", strip_num, cpu_offset);
                            //PRINT_DEBUG("The vert_to_scatter is %d, the edge_id is %d\n", u32_bitmap_value, z);
                            
                            if (signal_to_scatter == 2 || signal_to_scatter == 3)
                            {
                                my_context_data->steal_min_vert_id = u32_bitmap_value;
                                my_context_data->steal_context_edge_id = z;
                                PRINT_DEBUG("In steal-scatter, update_buffer is fulled, need to store the context data!\n");
                            }
                            else
                            {
                                PRINT_DEBUG("Update_buffer is fulled, need to store the context data!\n");
                                my_context_data->per_min_vert_id = u32_bitmap_value;
                                my_context_data->per_num_edges = z;
                                my_context_data->partition_gather_signal = processor_id;//just be different from origin status
                                my_context_data->partition_gather_strip_id = (int)strip_num;//record the strip_id to gather
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
                        current_bitmap->clear_value(u32_bitmap_value);
                        if (signal_to_scatter == 1 && my_context_data->per_bits_true_size == 0)
                            PRINT_DEBUG("u32_bitmap_value = %d\n", u32_bitmap_value);
                        if (signal_to_scatter == 2 || signal_to_scatter == 3)
                            my_context_data->steal_bits_true_size++;
                        else 
                            my_context_data->per_bits_true_size--;
                    }
                }

                if (*status == UPDATE_BUF_FULL)
                {
                    if (signal_to_scatter == 2 || signal_to_scatter == 3)
                    {
                        PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                    }
                    else
                        PRINT_DEBUG("Processor %d have not finished scatter,  UPDATE_BUF_FULL, has %d bits to scatter!\n", processor_id, 
                            my_context_data->per_bits_true_size);
                }
                else
                {
                    if (signal_to_scatter == 2 || signal_to_scatter == 3)
                    {
                        PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                    }
                    else
                    {
                        PRINT_DEBUG("Processor %d Finished scatter, has %d bits to scatter!\n", processor_id, 
                               my_context_data->per_bits_true_size);
                        if (my_context_data->per_bits_true_size != 0)
                        {
                            PRINT_ERROR("Error, processor %d still has %d bits to scatter!\n", processor_id, 
                                my_context_data->per_bits_true_size);
                            my_context_data->per_bits_true_size = 0;
                        }
                        my_context_data->per_max_vert_id = current_bitmap->get_start_vert();
                        my_context_data->per_min_vert_id = current_bitmap->get_term_vert();
                    }
                    *status = FINISHED_SCATTER;
                }
                break;
			}
            case GATHER:
            {                
                gather_param_target * p_gather_param = (gather_param_target *)state_param; 
                update_map_manager * my_update_map_manager;
                //aux_update_buf_manager<VA> * tmp_aux_manager;
                u32_t my_strip_cap;
                u32_t * my_update_map_head;
                //sched_bitmap_manager * my_sched_bitmap_manager;
                //struct context_data * my_context_data;

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

                //my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;

                //my_context_data = p_gather_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                  //  my_sched_bitmap_manager->p_context_data0;
                //PRINT_DEBUG("In gather, PHASE = %d, This is the processor %d, the max_vert = %d, the min_vert = %d\n", 
                 //       p_gather_param->PHASE, processor_id, 
                  //      my_context_data->per_max_vert_id, my_context_data->per_min_vert_id);
                
                //Traversal all the buffers of each cpu to find the corresponding UPDATES
                for (u32_t buf_id = 0; buf_id < gen_config.num_processors; buf_id++)
                {
                    my_update_map_manager = seg_config->per_cpu_info_list[buf_id]->update_manager;
                    my_update_map_head = my_update_map_manager->update_map_head;
                    my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
                    map_value = *(my_update_map_head + strip_id * gen_config.num_processors + processor_id);
                    if (map_value == 0)
                        continue;

                    //PRINT_DEBUG("map_value = %d\n", map_value);
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
                            
                        A::gather_one_update(dest_vert, (VA *)&attr_array_head[vert_index], t_update, p_gather_param->PHASE);
                        //PRINT_DEBUG("attr_array_head[%d].value = %f\n", vert_index, attr_array_head[vert_index].value);
                        //PRINT_DEBUG("t_update->attr.value = %f\n", (t_update->vert_attr).value);
                    }
                    map_value = 0;
                    *(my_update_map_head + strip_id * gen_config.num_processors + processor_id) = 0;
                }
                //PRINT_DEBUG("After gather!\n");
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

    void show_update_map( int processor_id, segment_config<VA, sched_bitmap_manager>* seg_config, u32_t* map_head )
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

	//return current sched_task
	//return NULL when there is no more tasks
	static sched_task * get_sched_task( sched_list_manager* sched_manager )
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
        if( sched_manager->current >= (sched_manager->sched_buf_head
                    + sched_manager->sched_buf_size) )
                sched_manager->current = sched_manager->sched_buf_head;
        else
                sched_manager->current++;

		//move forward head
		sched_manager->head = sched_manager->current;
	}
};

template <typename A, typename VA>
class cpu_thread_target {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	segment_config<VA, sched_bitmap_manager>* seg_config;
	int status;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work_target<A,VA> * volatile work_to_do;

    cpu_thread_target(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA, sched_bitmap_manager>* seg_config_in )
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
                //PRINT_DEBUG("Before operator, this is processor:%ld\n", processor_id);
                sync->wait();
	            (*work_to_do)(processor_id, sync, vert_index, seg_config, &status);

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
barrier * cpu_thread_target<A, VA>::sync;

template <typename A, typename VA>
volatile bool cpu_thread_target<A, VA>::terminate;

template <typename A, typename VA>
cpu_work_target<A,VA> * volatile cpu_thread_target<A, VA>::work_to_do;


#endif
