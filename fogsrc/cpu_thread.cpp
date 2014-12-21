/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He
 *
 * Routines:
 *   CPU (computing) threads.
 *************************************************************************************************/

#include "config.hpp"
#include "print_debug.hpp"
#include "bitmap.hpp"
#include "disk_thread.hpp"
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream> 
#include <fcntl.h>
#include <index_vert_array.hpp>

#include "cpu_thread.hpp"

template <typename A, typename VA, typename U, typename T>
cpu_work<A, VA, U, T>::cpu_work( u32_t state, void* state_param_in)
    :engine_state(state), state_param(state_param_in)
{
}
	
template <typename A, typename VA, typename U, typename T>
void cpu_work<A, VA, U, T>::operator() ( u32_t processor_id, barrier *sync, index_vert_array<T> *vert_index, 
    segment_config<VA>* seg_config, int *status )
{
    u32_t local_term_vert_off, local_start_vert_off;
    sync->wait();
    
    switch( engine_state ){
        case INIT:
        {
            if (A::init_sched == false)
            {
                init_param* p_init_param = (init_param*) state_param;

                if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

                //compute loca_start_vert_id and local_term_vert_id
                local_start_vert_off = processor_id*(seg_config->partition_cap);

                if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
                    local_term_vert_off = p_init_param->num_of_vertices - 1;
                else
                    local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;

                //Note: for A::init, the vertex id and VA* address does not mean the same offset!
                for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
                    A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buf_head) + i, vert_index);
                break;
            }
            else
            {
                assert(A::init_sched == true);
                init_param * p_init_param = (init_param *)state_param;
                if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

                u32_t current_start_id = p_init_param->start_vert_id + processor_id;
                u32_t current_term_id = p_init_param->start_vert_id + p_init_param->num_of_vertices; 

                for (u32_t i=current_start_id ; i<current_term_id; i+=gen_config.num_processors)
                {
                    assert((i-processor_id)%gen_config.num_processors == 0);
                    assert(i <= gen_config.max_vert_id);
                    u32_t index = 0;
                    if (seg_config->num_segments > 1)
                        index = i%seg_config->segment_cap;
                    else
                        index = i;

                    assert(index <= seg_config->segment_cap);
                    A::init( i, (VA*)(p_init_param->attr_buf_head) + index, vert_index); 
                }
                break;

            }
        }
        case TARGET_SCATTER:
        {
            *status = FINISHED_SCATTER;
            scatter_param * p_scatter_param = (scatter_param *)state_param; 
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            update_map_manager * my_update_map_manager;
            u32_t my_strip_cap, per_cpu_strip_cap;
            u32_t * my_update_map_head;

            VA * attr_array_head;
            //update<VA> * my_update_buf_head;
            update<U> * my_update_buf_head;
            update<U> * t_update;

            //bitmap * next_bitmap = NULL;
           // context_data * next_context_data = NULL;

            T * t_edge;
            in_edge * t_in_edge;
            u32_t num_edges;
            u32_t strip_num, cpu_offset, map_value, update_buf_offset;
            bitmap * current_bitmap = NULL ;
            u32_t signal_to_scatter;
            u32_t old_edge_id;
            u32_t max_vert = 0, min_vert = 0;

            my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
            my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;

            my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
            per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
            my_update_map_head = my_update_map_manager->update_map_head;
            my_update_buf_head = (update<U> *)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

            attr_array_head = (VA *)p_scatter_param->attr_array_head;


            my_context_data = p_scatter_param->PHASE > 0 ? my_sched_bitmap_manager->p_context_data1:
                my_sched_bitmap_manager->p_context_data0;
            
            signal_to_scatter = my_context_data->signal_to_scatter;
            if (signal_to_scatter == NORMAL_SCATTER || signal_to_scatter == CONTEXT_SCATTER)
            {
                current_bitmap = my_context_data->p_bitmap;
                max_vert = my_context_data->per_max_vert_id;
                min_vert = my_context_data->per_min_vert_id;

                if (signal_to_scatter == NORMAL_SCATTER && A::set_forward_backward == true 
                        && A::forward_backward_phase == FORWARD_TRAVERSAL)
                {
                    my_context_data->alg_per_max_vert_id = my_context_data->per_max_vert_id;
                    my_context_data->alg_per_min_vert_id = my_context_data->per_min_vert_id;
                    my_context_data->alg_per_bits_true_size = my_context_data->per_bits_true_size;
                }
            }
            if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
            {
                current_bitmap = my_context_data->p_bitmap_steal;
                max_vert = my_context_data->steal_max_vert_id;
                min_vert = my_context_data->steal_min_vert_id;
                if (my_context_data->steal_special_signal == true)
                {
                    *status = FINISHED_SCATTER;
                    break;
                }
            }

            if (my_context_data->per_bits_true_size == 0 && 
                    signal_to_scatter != STEAL_SCATTER && signal_to_scatter != SPECIAL_STEAL_SCATTER)
            {
                *status = FINISHED_SCATTER;
                //PRINT_DEBUG("Processor %d Finished scatter, has %d bits to scatter!\n", processor_id, 
                //       my_context_data->per_bits_true_size);
                break;
            }

            for (u32_t i = min_vert; i <= max_vert; i = i + gen_config.num_processors)
            {
                if (current_bitmap->get_value(i) == 0)
                    continue;
                
                if (A::forward_backward_phase == FORWARD_TRAVERSAL)
                    num_edges = vert_index->num_edges(i, OUT_EDGE);
                else
                {
                    assert(A::forward_backward_phase == BACKWARD_TRAVERSAL);
                    num_edges = vert_index->num_edges(i, IN_EDGE);
                }
                ///num_out_edges = vert_index->num_out_edges(i);
                //if (num_out_edges == 0 )
                if (num_edges == 0 )
                {
                    //if (seg_config->num_segments == 1 && 
                    //        ( engine_state == SCC_BACKWARD_SCATTER || engine_state == SCC_FORWARD_SCATTER))
                    //    A::set_finish_to_vert(i, (VA*)&attr_array_head[i]);
                    if ((A::set_forward_backward == true && A::forward_backward_phase == BACKWARD_TRAVERSAL) 
                           || A::set_forward_backward == false)
                        current_bitmap->clear_value(i);

                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        my_context_data->steal_bits_true_size++;
                    else 
                        my_context_data->per_bits_true_size--;
                    continue;
                }

                if ((signal_to_scatter == CONTEXT_SCATTER) && (i == my_context_data->per_min_vert_id))
                    old_edge_id = my_context_data->per_num_edges;
                else if ((signal_to_scatter == SPECIAL_STEAL_SCATTER) && (my_context_data->steal_min_vert_id == i))
                    old_edge_id = my_context_data->steal_context_edge_id;
                else
                    old_edge_id = 0;

                //bool will_be_updated = false;
                //if (engine_state == CC_SCATTER)
                //    old_edge_id = 0;
                for (u32_t z = old_edge_id; z < num_edges; z++)
                {
                    if (A::forward_backward_phase == FORWARD_TRAVERSAL)
                    {
                        t_edge = vert_index->get_out_edge(i, z);
                        if (t_edge->get_dest_value() == i)
                        {
                            delete t_edge;
                            continue;
                        }
                        assert(t_edge);//Make sure this edge existd!
                        t_update = A::scatter_one_edge((VA *)&attr_array_head[i], t_edge, i);
                        assert(t_update);
                        delete t_edge;
                    }
                    else
                    {
                        assert(A::forward_backward_phase == BACKWARD_TRAVERSAL);
                        t_in_edge = vert_index->get_in_edge(i, z);
                        if (t_in_edge->get_src_value() == i)
                        {
                            delete t_in_edge;
                            continue;
                        }
                        assert(t_in_edge);//Make sure this edge existd!
                        t_update = A::scatter_one_edge((VA *)&attr_array_head[i], NULL, t_in_edge->get_src_value());
                        delete t_in_edge;
                    }

                    strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                    cpu_offset = VID_TO_PARTITION(t_update->dest_vert );
                    assert(strip_num < seg_config->num_segments);
                    assert(cpu_offset < gen_config.num_processors);

                    map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);
                    
                    if (map_value < per_cpu_strip_cap)
                    {

                        update_buf_offset = strip_num * my_strip_cap + 
                            map_value * gen_config.num_processors + cpu_offset;

                        *(my_update_buf_head + update_buf_offset) = *t_update;
                        map_value++;
                        *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                    }
                    else
                    {          
                        if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                        {
                            my_context_data->steal_min_vert_id = i;
                            my_context_data->steal_context_edge_id = z;
                            //PRINT_DEBUG("In steal-scatter, update_buffer is fulled, need to store the context data!\n");
                        }
                        else
                        {
                            //PRINT_DEBUG("Update_buffer is fulled, need to store the context data!\n");
                            my_context_data->per_min_vert_id = i;
                            my_context_data->per_num_edges = z;
                            my_context_data->partition_gather_signal = processor_id;//just be different from origin status
                            my_context_data->partition_gather_strip_id = (int)strip_num;//record the strip_id to gather
                        }
                        *status = UPDATE_BUF_FULL;
                        delete t_update;
                        break;
                    }
                    delete t_update;
                }
                if (*status == UPDATE_BUF_FULL)
                    break;
                else
                {
                    assert(*status == FINISHED_SCATTER);
                    if ((A::set_forward_backward == true && A::forward_backward_phase == BACKWARD_TRAVERSAL) 
                            || A::set_forward_backward == false)
                        current_bitmap->clear_value(i);
                    if (signal_to_scatter == 1 && my_context_data->per_bits_true_size == 0)
                        PRINT_ERROR("i = %d\n", i);
                    if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                    {
                        my_context_data->steal_bits_true_size++;
                    }
                    else 
                    {                            
                        my_context_data->per_bits_true_size--;
                    }
                    
                }
                
            }
            
            if (*status == UPDATE_BUF_FULL)
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                }
                else
                {}
                    //PRINT_DEBUG("Processor %d have not finished scatter,  UPDATE_BUF_FULL, has %d bits to scatter!\n", processor_id, 
                      //  my_context_data->per_bits_true_size);
            }
            else
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d bits\n", processor_id, my_context_data->steal_bits_true_size);
                }
                else
                {
                    //PRINT_DEBUG("Processor %d Finished scatter, has %d bits to scatter!\n", processor_id, 
                      //     my_context_data->per_bits_true_size);
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
        case GLOBAL_SCATTER:
        {
            *status = FINISHED_SCATTER;
            scatter_param* p_scatter_param = (scatter_param*) state_param;
            sched_list_context_data* my_sched_list_manager;
            update_map_manager* my_update_map_manager;
            u32_t my_strip_cap, per_cpu_strip_cap;
            u32_t* my_update_map_head;

            VA* attr_array_head;
            update<U>* my_update_buf_head;

            T * t_edge;
            update<U> *t_update = NULL;
            u32_t num_out_edges;
            u32_t strip_num, cpu_offset, map_value, update_buf_offset;

            my_sched_list_manager = seg_config->per_cpu_info_list[processor_id]->global_sched_manager;
            my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;

            my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
            per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
            my_update_map_head = my_update_map_manager->update_map_head;

            attr_array_head = (VA*) p_scatter_param->attr_array_head;
            my_update_buf_head = 
                (update<U>*)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);

            u32_t signal_to_scatter = my_sched_list_manager->signal_to_scatter;
            u32_t min_vert = 0, max_vert = 0;
            u32_t old_edge_id;
            if (signal_to_scatter == NORMAL_SCATTER)
            {
                min_vert = my_sched_list_manager->normal_sched_min_vert;
                max_vert = my_sched_list_manager->normal_sched_max_vert;
                //PRINT_DEBUG("cpu %d normal scatter, min_vert = %d, max_vert = %d\n",
                //        processor_id, min_vert, max_vert);
            }
            else if (signal_to_scatter == CONTEXT_SCATTER)
            {
                min_vert = my_sched_list_manager->context_vert_id;
                max_vert = my_sched_list_manager->normal_sched_max_vert;
                //PRINT_DEBUG("cpu %d context scatter, min_vert = %d, max_vert = %d\n",
                  //      processor_id, min_vert, max_vert);
            }
            else if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
            {
                min_vert = my_sched_list_manager->context_steal_min_vert;
                max_vert = my_sched_list_manager->context_steal_max_vert;
                //PRINT_DEBUG("cpu %d steal scatter, min_vert = %d, max_vert = %d\n",
                  //      processor_id, min_vert, max_vert);
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
                
                num_out_edges = vert_index->num_edges(i, OUT_EDGE);

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
                    t_edge = vert_index->get_out_edge(i, z);
                    //Make sure this edge existd!
                    assert(t_edge);
                    t_update = A::scatter_one_edge((VA*)&attr_array_head[i], t_edge, num_out_edges);
                    assert(t_update);
                    delete t_edge;

                    //Make sure this update existd!

                    strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                    cpu_offset = VID_TO_PARTITION(t_update->dest_vert);
                    //Check for existd!
                    assert(strip_num < seg_config->num_segments);
                    assert(cpu_offset < gen_config.num_processors);

                    //find out the corresponding value for update-buffer
                    map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);

                    if (map_value < (per_cpu_strip_cap - 1))
                    {

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
                            //PRINT_DEBUG("In steal-scatter, update-buf is fulled, need to store the context data!\n");
                            //PRINT_DEBUG("min_vert = %d, max_vert = %d, edge = %d\n", i, max_vert, z);
                        }
                        else
                        {
                            //PRINT_DEBUG("other-scatter, update-buf is fulled, need to store the context data!\n");
                            my_sched_list_manager->context_vert_id = i;
                            my_sched_list_manager->context_edge_id = z;
                            my_sched_list_manager->partition_gather_strip_id = (int)strip_num;
                            //PRINT_DEBUG("vert = %d, edge = %d, strip_num = %d\n", i, z, strip_num);
                        }
                        *status = UPDATE_BUF_FULL;
                        delete t_update;
                        break;
                    }
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
                    {}
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id, 
                      //      my_sched_list_manager->context_steal_num_vert);
                else{}
                    //PRINT_DEBUG("Processor %d has not finished scatter, has %d vertices to scatter~\n", processor_id,
                      //      my_sched_list_manager->num_vert_to_scatter);
            }
            else
            {
                if (signal_to_scatter == STEAL_SCATTER || signal_to_scatter == SPECIAL_STEAL_SCATTER)
                {}
                    //PRINT_DEBUG("Steal-cpu %d has scatter %d vertex\n", processor_id, 
                      //      my_sched_list_manager->context_steal_num_vert);
                else
                {
                    if (my_sched_list_manager->num_vert_to_scatter != 0)
                    {
                        PRINT_ERROR("after scatter, num_vert_to_scatter != 0\n");
                    }
                    //PRINT_DEBUG("Processor %d has finished scatter, and there is %d vertex to scatter\n", processor_id, 
                      //      my_sched_list_manager->num_vert_to_scatter);
                }
                *status = FINISHED_SCATTER;
            }
            break;
        }
        case GLOBAL_GATHER:
        case TARGET_GATHER:
        {                
            gather_param * p_gather_param = (gather_param *)state_param; 
            update_map_manager * my_update_map_manager;
            u32_t my_strip_cap;
            u32_t * my_update_map_head;
            VA * attr_array_head;
            update<U> * my_update_buf_head;

            update<U> * t_update;
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
                my_update_buf_head = (update<U> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
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

template <typename A, typename VA, typename U, typename T>
void cpu_work<A, VA, U, T>::show_update_map( int processor_id, segment_config<VA>* seg_config, u32_t* map_head )
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

//impletation of cpu_thread
template <typename A, typename VA, typename U, typename T>
cpu_thread<A, VA, U, T>::cpu_thread(u32_t processor_id_in, index_vert_array<T> * vert_index_in, segment_config<VA>* seg_config_in )
:processor_id(processor_id_in), vert_index(vert_index_in), seg_config(seg_config_in)
{   
    if(sync == NULL) { //as it is shared, be created for one time
        sync = new barrier(gen_config.num_processors);
    }
}   

template <typename A, typename VA, typename U, typename T>
void cpu_thread<A, VA, U, T>::operator() ()
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

template <typename A, typename VA, typename U, typename T>
sched_task* cpu_thread<A, VA, U, T>::get_sched_task()
{return NULL;}

template <typename A, typename VA, typename U, typename T>
void cpu_thread<A, VA, U, T>::browse_sched_list()
{}
