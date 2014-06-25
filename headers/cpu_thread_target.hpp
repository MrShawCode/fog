#ifndef __CPU_THREAD_TARGET_HPP__
#define __CPU_THREAD_TARGET_HPP__

#include "cpu_thread.hpp"
#include "bitmap.hpp"
#include "disk_thread_target.hpp"
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


/*enum fog_engine_state{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM
};*/

//denotes the different status of cpu threads after they finished the given tasks.
// Note: these status are for scatter phase ONLY!
/*enum cpu_thread_status{
	UPDATE_BUF_FULL = 100,	//Cannot scatter more updates, since my update buffer is full
	NO_MORE_SCHED,			//I have no more sched tasks, but have updates in the auxiliary update buffer
	FINISHED_SCATTER		//I have no more sched tasks, and no updates in auxiliary update buffer.
							//	But still have updates in my strip update buffer. 
};*/

#define SCHED_BUFFER_LEN    1024

template <typename A, typename VA>
class cpu_thread_target;

/*class barrier {
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
//    friend class cpu_thread_target<A,VA>;
};*/

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
    u32_t strip_id;
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
        //PRINT_DEBUG("PROCESSOR:%d\n", processor_id);
		
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
                //param for new_bitmap
                bitmap * current_bitmap = NULL ;
                //u32_t * p_bitmap_index = NULL;
                //unsigned char * p_char_bitmap_index;
                //u32_t p_bitmap_value;
                //unsigned char p_char_bitmap_value;
                u32_t u32_bitmap_value;
                //u32_t min_index, u32_min_index;
                //u32_t max_index, u32_max_index;
                u32_t signal_to_scatter;
                u32_t old_edge_id;
                //u32_t u8_min_index, tmp_u8, u8_min_value, tmp_index;
                u32_t max_vert = 0, min_vert = 0;

                PRINT_DEBUG("PHASE:%d, processor : %d\n",p_scatter_param->PHASE, processor_id);

                my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;
                my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;
                //my_aux_manager = seg_config->per_cpu_info_list[processor_id]->aux_manager;

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
                my_update_map_head = my_update_map_manager->update_map_head;
                attr_array_head = (VA *)p_scatter_param->attr_array_head;
                my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);


                my_context_data = p_scatter_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                    my_sched_bitmap_manager->p_context_data0;
                
                signal_to_scatter = my_context_data->signal_to_scatter;
                if (signal_to_scatter == 0)
                {
                    PRINT_DEBUG("Normal scatter!\n");
                    current_bitmap = my_context_data->p_bitmap;
                    max_vert = my_context_data->per_max_vert_id;
                    min_vert = my_context_data->per_min_vert_id;
                }
                else if (signal_to_scatter == 1)
                {
                    PRINT_DEBUG("Context scatter!\n");
                    current_bitmap = my_context_data->p_bitmap;
                    max_vert = my_context_data->per_max_vert_id;
                    min_vert = my_context_data->per_min_vert_id;
                }
                else if (signal_to_scatter == 2)
                {
                    PRINT_DEBUG("Steal scatter\n");
                    current_bitmap = my_context_data->p_bitmap_steal;
                    max_vert = my_context_data->steal_max_vert_id;
                    min_vert = my_context_data->steal_min_vert_id;
                }
                //current_bitmap->print_binary(0, 100);
                //if (current_bitmap->get_bits_true_size() == 0)
                if (my_context_data->per_bits_true_size == 0 && signal_to_scatter != 2)
                {
                    *status = FINISHED_SCATTER;
                    break;
                }
                //p_bitmap_index =  (u32_t *)my_context_data->per_bitmap_buf_head;
                //PRINT_DEBUG("p_bitmap_index's first u32_t is 0x%u\n", (u32_t)*p_bitmap_index);


                //max_vert = current_bitmap->get_max_vert();
                //min_vert = current_bitmap->get_min_vert();
                PRINT_DEBUG("Processor: %d ,min_vert= %d, max_vert= %d\n", processor_id, min_vert, max_vert);

                    //a funny way to get the private value
                    //u32_t max_vert = *((u32_t *)new_read_bitmap + 5);
                    //u32_t min_vert = *((u32_t *)new_read_bitmap + 6);
                    //Traversal the bitmap to find ...
                    //min_index = VID_TO_BITMAP_INDEX(min_vert, processor_id, gen_config.num_processors);
                    //max_index = VID_TO_BITMAP_INDEX(max_vert, processor_id, gen_config.num_processors);
                    //u32_min_index = min_index/(sizeof(u32_t) * 8);
                    //u32_max_index = max_index/(sizeof(u32_t) * 8);
                    //u8_min_index = (min_index%(sizeof(u32_t)*8))/(sizeof(char) * 8);
                    //u8_min_value = (min_index%(sizeof(u32_t)*8))%(sizeof(char) * 8);
                    //PRINT_DEBUG("min_index = %d, max_index = %d\n", u32_min_index, u32_max_index);
                    //p_bitmap_index = p_bitmap_index + u32_min_index;
                    //PRINT_DEBUG("after  add u32_min_index ,p_bitmap_index's u32_t is 0x%u\n", (u32_t)*p_bitmap_index);
                    for (u32_t i = min_vert; i <= max_vert; i = i + gen_config.num_processors)
                    {
                    //for (u32_t i = u32_min_index; i <= u32_max_index; i++)
                    //{
                    //    p_bitmap_value = (u32_t)*(p_bitmap_index+i);
                    //    if (p_bitmap_value == 0)
                    //    {
                            //means this u32_t dosen't contain any vertex to scatter!
                   //         continue;
                   //     }
                        //p_char_bitmap_index = (char *)p_bitmap_index;
                  //      p_char_bitmap_index = (unsigned char *)&p_bitmap_value;
                       // if (i == u32_min_index)
                          //  tmp_u8 = u8_min_index;
                        //else 
                          //  tmp_u8 = 0;
                        //for (u32_t j = tmp_u8; j < sizeof(u32_t); j++)
                   //     for (u32_t j = 0; j < sizeof(u32_t); j++)
                    //    {
                    //        p_char_bitmap_value = (unsigned char )*(p_char_bitmap_index + j);
                    //        if ((u32_t)p_char_bitmap_value == 0)
                    //        {
                    //            continue;
                    //        }

                            //if (i == u32_min_index)
                                //tmp_index = u8_min_value;
                            //else 
                                //tmp_index = 0;
                            //tmp_index = tmp_u8;
                           //tmp_u8 = tmp_index;
                            //for (u32_t k = tmp_index; k < 8; k++)
                      //      for (u32_t k = 0; k < 8; k++)
                       //     {
                     //           if ( ((1 << k) & p_char_bitmap_value) == 0 )
                      //              continue;
                      //          u32_bitmap_value = INDEX_TO_VID(i, j, k, gen_config.num_processors, processor_id);
                      //          if (current_bitmap->get_value(u32_bitmap_value) == 0)
                       //             continue;
                        u32_bitmap_value = i;
                        if (current_bitmap->get_value(u32_bitmap_value) == 0)
                            continue;

                                num_out_edges = vert_index->num_out_edges(u32_bitmap_value);
                                if (num_out_edges == 0 )
                                {
                                    current_bitmap->clear_value(u32_bitmap_value);
                                    if (signal_to_scatter == 2)
                                        my_context_data->steal_bits_true_size++;
                                    else 
                                        my_context_data->per_bits_true_size--;
                                    continue;
                                }

                                //tell if the remaining space in update buffer is enough to store the updates?
                                //since we add an auxiliary update buffer
                                //temp_laxity = my_aux_manager->buf_cap - my_aux_manager->num_updates;
                                /*temp_laxity = 0;

                                if (temp_laxity < num_out_edges)
                                {
                                    PRINT_DEBUG("Processor %d: laxity=%u, current out edges=%u, i = %u\n", processor_id, temp_laxity, num_out_edges, u32_bitmap_value);
                                    *status = UPDATE_BUF_FULL;
                                    current_bitmap->set_min_value(u32_bitmap_value);
                                    //need to supplement
                                    return;
                                }*/

                                //set the 0 to the i-th pos 
                                //if (u32_bitmap_value == 23 || u32_bitmap_value == 10)
                                  //  PRINT_DEBUG("u32_bitmap_value = %d\n", u32_bitmap_value);

                                //u32_t tmp;
                                if (signal_to_scatter == 0)
                                    old_edge_id = 0;
                                else if ((signal_to_scatter == 1) && (i == my_context_data->per_min_vert_id))
                                    old_edge_id = my_context_data->per_num_edges;
                                else 
                                    old_edge_id = 0;
                                //for (u32_t z = 0; z < num_out_edges; z++)
                                for (u32_t z = old_edge_id; z < num_out_edges; z++)
                                {
                                    t_edge = vert_index->out_edge(u32_bitmap_value, z);
                                    assert(t_edge);//Make sure this edge existd!
                                    t_update = A::scatter_one_edge(u32_bitmap_value, (VA *)&attr_array_head[u32_bitmap_value], t_edge);
                                    //if (p_scatter_param->PHASE == 1)
                                    //{
                                      //  PRINT_DEBUG("u32_bitmap_value = %d, the value is %f\n",u32_bitmap_value, 
                                        //        attr_array_head[u32_bitmap_value].value);
                                      //  PRINT_DEBUG("t_edge's weight = %f\n", t_edge->edge_weight);
                                       // PRINT_DEBUG("t_update's dest_vert = %d, value = %f\n", t_update->dest_vert, t_update->vert_attr.value);
                                   // }
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
                                        //should add it to auxiliary update buffer
                                        //*(my_aux_manager->update_head + my_aux_manager->num_updates) = *t_update;
                                        //my_aux_manager->num_updates++;
                                        
                                        //A new method to handle the situation when some strip_buf is FULL
                                        PRINT_DEBUG("Currently, the strip_num = %d, cpu_offset = %d\n", strip_num, cpu_offset);
                                        PRINT_DEBUG("The vert_to_scatter is %d, the edge_id is %d\n", u32_bitmap_value, z);
                                        //PRINT_DEBUG("map_value = %d, per_cpu_strip_cap = %d\n", map_value, per_cpu_strip_cap);

                                        //set the value of current_bitmap and current bitmap_manager

                                        my_context_data->per_min_vert_id = u32_bitmap_value;
                                        my_context_data->per_num_edges = z;

                                        *status = UPDATE_BUF_FULL;
                                        break;
                                    }

                                    if (z == (num_out_edges - 1))
                                    {
                                        current_bitmap->clear_value(u32_bitmap_value);
                                        if (signal_to_scatter == 2)
                                            my_context_data->steal_bits_true_size++;
                                        else 
                                            my_context_data->per_bits_true_size--;
                                    }
                                    delete t_edge;
                                    delete t_update;
                                }
                                //current_bitmap->clear_value(u32_bitmap_value);
                            //}
                        //}
                        if (*status == UPDATE_BUF_FULL)
                            break;
                    }

                    if (*status == UPDATE_BUF_FULL)
                    {
                        PRINT_DEBUG("UPDATE_BUF_FULL:after this scatter, Processor %d has %d bits to scatter!\n", processor_id, 
                                my_context_data->per_bits_true_size);
                    }
                    else
                    {
                        if (signal_to_scatter == 2)
                        {
                            PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                        }
                        else
                        {
                            PRINT_DEBUG("After this scatter, Processor %d has %d bits to scatter!\n", processor_id, 
                                    my_context_data->per_bits_true_size);
                            if (my_context_data->per_bits_true_size != 0)
                            {
                                PRINT_ERROR("Processor %d still has %d bits to scatter!\n", processor_id, 
                                    my_context_data->per_bits_true_size);
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
                sched_bitmap_manager * my_sched_bitmap_manager;
                struct context_data * my_context_data;

                VA * attr_array_head;
                update<VA> * my_update_buf_head;

                update<VA> * t_update;
                u32_t map_value, update_buf_offset;
                u32_t dest_vert;
                u32_t strip_id;
                u32_t threshold;
                u32_t vert_index;
                //bitmap * current_bitmap;

                //PRINT_DEBUG("processor : %d, parameter address:%llx\n", processor_id, (u64_t)p_gather_param);

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                attr_array_head = (VA *)p_gather_param->attr_array_head;
                strip_id = p_gather_param->strip_id;
                threshold = p_gather_param->threshold;

                my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;

                my_context_data = p_gather_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                    my_sched_bitmap_manager->p_context_data0;
                //current_bitmap = p_gather_param->PHASE > 0? my_sched_bitmap_manager->p_bitmap1
                  //  : my_sched_bitmap_manager->p_bitmap0;
                PRINT_DEBUG("PHASE = %d, This is the processor %d, the max_vert = %d, the min_vert = %d\n", 
                        p_gather_param->PHASE, processor_id, 
                        my_context_data->per_max_vert_id, my_context_data->per_min_vert_id);
                
                //int current_bitmap_file_fd;
                //Traversal all the buffers of each cpu to find the corresponding UPDATES
                for (u32_t buf_id = 0; buf_id < gen_config.num_processors; buf_id++)
                {
                    //PRINT_DEBUG("I(%d) am checking the %d's buffer\n", processor_id, buf_id);
                    my_update_map_manager = seg_config->per_cpu_info_list[buf_id]->update_manager;
                    my_update_map_head = my_update_map_manager->update_map_head;
                    my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
                    map_value = *(my_update_map_head + strip_id * gen_config.num_processors + processor_id);
                    if (map_value == 0)
                        continue;

                    //PRINT_DEBUG("I(%d) am checking the %d's buffer, and now in the %d strip,and the num of this strip is %d\n", 
                      //      processor_id, buf_id, strip_id, map_value);
                    for (u32_t update_id = 0; update_id < map_value; update_id++)
                    {
                        update_buf_offset = strip_id * my_strip_cap + update_id * gen_config.num_processors + processor_id;
                        //PRINT_DEBUG("update_buf_offset = %d\n", update_buf_offset);
                        t_update = (my_update_buf_head + update_buf_offset);
                        //PRINT_DEBUG("dest_vert = %d\n", my_update_buf_head->dest_vert);
                        assert(t_update);
                        dest_vert = t_update->dest_vert;
                        if (threshold == 1) 
                            vert_index = dest_vert%seg_config->segment_cap;
                        else
                            vert_index = dest_vert;
                            
                        //PRINT_DEBUG("dest_vert = %d\n", dest_vert);
                        A::gather_one_update(dest_vert, (VA *)&attr_array_head[vert_index], t_update, p_gather_param->PHASE);
                        //if (/*p_gather_param->PHASE == 1 &&*/ ( dest_vert == 23 || dest_vert == 10))
                          //  PRINT_DEBUG("dest_vert = %d\n", dest_vert);
                          //  PRINT_ERROR("here!\n");
                    }
                    //PRINT_DEBUG("This is processor:%d, my_bitmap->bits_true_size is %d:\n", processor_id,
                            //current_bitmap->get_bits_true_size());
                    map_value = 0;
                    *(my_update_map_head + strip_id * gen_config.num_processors + processor_id) = 0;
                    //now, the thread has finished its own task of the strip_id 
                    /*if (strip_id == (seg_config->num_segments - 1))
                    {
                        //handle the aux_buffer 
                        for (u32_t tmp_processor_id = 0; tmp_processor_id < gen_config.num_processors; tmp_processor_id++)
                        {
                            tmp_aux_manager = seg_config->per_cpu_info_list[tmp_processor_id]->aux_manager;
                            if (tmp_aux_manager->num_updates > 0)
                            {
                                //we need to hadle each process's buffer 
                            }
                        }
                    }*/
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
