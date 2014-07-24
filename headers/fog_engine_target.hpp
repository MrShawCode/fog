//fog_engine_target is defined for targeted queries, such as SSSP.
// characters of fog_engine_target:
// 1) Schedule list with dynamic size
// 2) need to consider merging and (possibly) the scheduled tasks
// 3) As the schedule list may grow dramatically, the system may need to consider dump 
//	(partial) of the list to disk (to alieviate pressure on buffer).

#ifndef __FOG_ENGINE_TARGET_H__
#define __FOG_ENGINE_TARGET_H__

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "bitmap.hpp"
#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "disk_thread_target.hpp"
#include "cpu_thread.hpp"
#include "print_debug.hpp"
#include "cpu_thread_target.hpp"

#define THRESHOLD 0.8
#define MMAP_THRESHOLD 0.005

int init_current_fd;
//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fog_engine_target{
		static index_vert_array* vert_index;

		static segment_config<VA, sched_bitmap_manager> *seg_config;
        char * buf_for_write;

        static u32_t fog_engine_target_state;
        static u32_t current_attr_segment;

        //io work queue
        static io_queue_target* fog_io_queue;

        cpu_thread_target<A,VA> ** pcpu_threads;
        boost::thread ** boost_pcpu_threads;

        u32_t * p_strip_count;

        int attr_fd;
        u64_t attr_file_length;
        VA *attr_array_header;

        int signal_of_partition_gather;
        int  loop_counter;


	public:

		fog_engine_target()
		{
            //create the index array for indexing the out-going edges
            vert_index = new index_vert_array;

            //-1 means nothing
            //1 means partiton_gather, thus gather a part of segments in stead of all the segments, in ALL-BUF-FULL mode
            //2 means partition gather in steal-mode
            signal_of_partition_gather = 0;

            loop_counter = 0;


            //allocate buffer for writting
            buf_for_write = (char *)map_anon_memory(gen_config.memory_size, true, true );

            //config the buffer for writting
            seg_config = new segment_config<VA, sched_bitmap_manager>((const char *)buf_for_write);

            //create io queue
            fog_io_queue = new io_queue_target;

            //create cpu threads
            pcpu_threads = new cpu_thread_target<A,VA> *[gen_config.num_processors];
            boost_pcpu_threads = new boost::thread *[gen_config.num_processors];
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                pcpu_threads[i] = new cpu_thread_target<A,VA>(i, vert_index, seg_config);
                if(i > 0)
                    boost_pcpu_threads[i] = new boost::thread(boost::ref(*pcpu_threads[i]));
            }
            attr_fd = 0;

            init_sched_update_buf();
        }
			
		~fog_engine_target()
		{
            reclaim_everything();
        }

		void operator() ()
		{
            u32_t PHASE = 0;
            init_phase(PHASE);
            while(1)
            {
                loop_counter++;
                PRINT_WARNING("SCATTER and UPDATE loop %d\n", loop_counter);
                PHASE = (loop_counter%2);
                //PRINT_DEBUG("PHASE = %d\n", PHASE);

                PRINT_DEBUG("before normal-scatter, num_vert_of_next_phase = %d\n", cal_true_bits_size(1-PHASE));
                scatter_updates(1-PHASE, loop_counter);
                cal_threshold();
                PRINT_DEBUG("after normal-scatter and before normal-gather, num_vert_of_next_phase = %d\n", cal_true_bits_size(1-PHASE));

                gather_updates(PHASE, -1, loop_counter);
                //cal_threshold();
                PRINT_DEBUG("after normal-gather, num_vert_of_next_phase = %d\n", cal_true_bits_size(PHASE));

                if (cal_true_bits_size(PHASE) == 0)
                    break;
            }
            print_attr_result();
        }

        void print_attr_result()
        {
            u32_t i = 0;
            if (seg_config->num_attr_buf == 1)
            {
                VA * attr_array_head =  (VA *)seg_config->attr_buf0;
                for (i = 0; i < 10000; i++)
                    PRINT_DEBUG("attr[%d].predecessor = %d, value = %f\n",i, attr_array_head[i].predecessor, attr_array_head[i].value);
            }
            else
            {
                if (remap_attr_file() < 0)
                {
                    PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                }
                for (i = 0; i < 10000; i++)
                    PRINT_DEBUG("attr[%d].predecessor = %d, value = %f\n",i, attr_array_header[i].predecessor, attr_array_header[i].value);

            }
        }

        u32_t cal_true_bits_size(u32_t PHASE)
        {
            u32_t num_vert_of_next_phase = 0;
            struct context_data * my_context_data;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                my_context_data = PHASE > 0 ? seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1
                    : seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0;
                num_vert_of_next_phase += my_context_data->per_bits_true_size; 
            }
            return num_vert_of_next_phase;
        }

		void init_phase(u32_t PHASE)
		{
			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work_target<A,VA>* new_cpu_work = NULL;
			io_work_target* init_io_work = NULL;
			char * buf_to_dump = NULL;
			init_param_target * p_init_param=new init_param_target;

			PRINT_DEBUG( "fog engine operator is called, conduct init phase for %d times.\n", seg_config->num_segments );

            p_strip_count = new u32_t[seg_config->num_segments];
            memset(p_strip_count, 0, sizeof(u32_t)*seg_config->num_segments);
			current_attr_segment = 0;
			fog_engine_target_state = INIT;
			for( u32_t i=0; i < seg_config->num_segments; i++ ){
				//which attribute buffer should be dumped to disk?
				if ( current_attr_segment%2== 0 ) buf_to_dump = (char*)seg_config->attr_buf0;
				else buf_to_dump = (char*)seg_config->attr_buf1;
		
				//create cpu threads
				if( i != (seg_config->num_segments-1) ){
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = seg_config->segment_cap;
                    p_init_param->PHASE = PHASE;
					new_cpu_work = new cpu_work_target<A,VA>( INIT, 
						(void*)p_init_param);
				}else{	//the last segment, should be smaller than a full segment
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap+1;
                    p_init_param->PHASE = PHASE;
					new_cpu_work = new cpu_work_target<A,VA>( INIT, 
						(void*)p_init_param);
				}
				pcpu_threads[0]->work_to_do = new_cpu_work;
				(*pcpu_threads[0])();
				//cpu threads finished init current attr buffer
				delete new_cpu_work;
				new_cpu_work = NULL;
				
				//dump current attr buffer to disk
				if ( init_io_work != NULL ){
					//keep waiting till previous disk work is finished
					fog_io_queue->wait_for_io_task( init_io_work );
					fog_io_queue->del_io_task( init_io_work );
					init_io_work = NULL;
				}

				if( i != (seg_config->num_segments-1) ){
					init_io_work = new io_work_target( gen_config.attr_file_name.c_str(),
                        FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)seg_config->segment_cap*sizeof(VA) );
				}else{
					init_io_work = new io_work_target( gen_config.attr_file_name.c_str(),
                        FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA) );
				}

				//activate the disk thread
				fog_io_queue->add_io_task( init_io_work );

				current_attr_segment++;
			}
	
			//the INIT phase finished now, ALTHOUGH the last write is still on its way. 
			// Do NOT wait!

			//should add tasks here, when the disk is busy.

			//show_all_sched_tasks();
			
			//FOLLOWING BELONGS TO THE INIT PHASE! wait till the last write work is finished.
			fog_io_queue->wait_for_io_task( init_io_work );
			fog_io_queue->del_io_task( init_io_work );
			PRINT_DEBUG( "fog engine finished initializing attribute files!\n" );
			//ABOVE BELONGS TO THE INIT PHASE! wait till the last write work is finished.
		}


        //scatter_updates:
        //scatter updates to update buffer, till update buffer filled, or no more sched_tasks
        //return values:
        //0:no more sched tasks
        //1:update buffer full
        //2:steal work
        //-1:failure
        void set_signal_to_scatter(u32_t signal, u32_t processor_id, u32_t PHASE)
        {
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;
            my_context_data = PHASE > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;
            my_context_data->signal_to_scatter = signal; 
        }
        int scatter_updates(u32_t PHASE, u32_t loop_counter)
        {
            u32_t ret = 0, unemployed=0; 
            u32_t num_finished, num_not_finished;
            u32_t *p_finished = NULL, *p_not_finished = NULL;
            int super_phase = 0;
            cpu_work_target<A,VA>* scatter_cpu_work = NULL;
            scatter_param_target * p_scatter_param = new scatter_param_target;

			fog_engine_target_state = SCATTER;
            if (seg_config->num_attr_buf == 1)
            {
                p_scatter_param->attr_array_head = (void*)seg_config->attr_buf0;
                p_scatter_param->PHASE = PHASE;
            }
            else
            {
                if (remap_attr_file() < 0)
                {
                    PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                    return -1;
                }
                p_scatter_param->attr_array_head = (void *)attr_array_header;
                p_scatter_param->PHASE = PHASE;
            }

            do{

                PRINT_DEBUG("Super_phase = %d\n", super_phase);
                if (ret == 1)
                    PRINT_DEBUG("before context scatter, num_vert_of_next_phase = %d\n", cal_true_bits_size(PHASE));
                scatter_cpu_work = new cpu_work_target<A, VA>(SCATTER, (void *)p_scatter_param);
                pcpu_threads[0]->work_to_do = scatter_cpu_work;
                (*pcpu_threads[0])();

                delete scatter_cpu_work;
                scatter_cpu_work = NULL;
                //p_scatter_param = NULL;

                PRINT_DEBUG("After scatter computation!\n");
                ret = 0;
                unemployed = 0;
                num_not_finished = 0;
                num_finished = 0;
                p_not_finished = NULL; 
                p_finished = NULL;//remember free!!!
                for (u32_t i = 0; i < gen_config.num_processors; i++)
                {
                    PRINT_DEBUG("Processor %d status %d\n", i, pcpu_threads[i]->status);
                    if(pcpu_threads[i]->status != FINISHED_SCATTER)
                    {
                        num_not_finished++;
                        p_not_finished = (u32_t *)realloc (p_not_finished, sizeof(u32_t) * num_not_finished);
                        if (num_not_finished == 1)
                        {
                            *p_not_finished = i; //p_not_finished[0] = i;
                        }
                        else 
                        {
                            p_not_finished[num_not_finished-1] = i;
                            //insertion sort
                            u32_t tmp1 = PHASE > 0 ? seg_config->per_cpu_info_list[p_not_finished[num_not_finished-1]]->sched_manager->
                                p_context_data1->per_bits_true_size : seg_config->per_cpu_info_list[p_not_finished[num_not_finished-1]]->sched_manager->
                                p_context_data0->per_bits_true_size;
                            u32_t tmp2 = PHASE > 0 ? seg_config->per_cpu_info_list[p_not_finished[num_not_finished-2]]->sched_manager->
                                p_context_data1->per_bits_true_size : seg_config->per_cpu_info_list[p_not_finished[num_not_finished-2]]->sched_manager->
                                p_context_data0->per_bits_true_size;
                            if (tmp1 > tmp2)
                            {
                                p_not_finished[num_not_finished-1] = p_not_finished[num_not_finished-2];
                                p_not_finished[num_not_finished-2] = i;
                            }

                        }
                        ret = 1;
                        set_signal_to_scatter(1, i, PHASE);
                    }
                    if (pcpu_threads[i]->status != UPDATE_BUF_FULL )
                    {
                        num_finished++;
                        p_finished = (u32_t *)realloc (p_finished, sizeof(u32_t) * num_finished);
                        p_finished[num_finished-1] = i;
                        unemployed = 1;
                        //PRINT_DEBUG("Processor: %d has finished scatter, and the update buffer is not full!\n", i);
                        set_signal_to_scatter(0, i, PHASE);
                    }
                }
                if ((cal_true_bits_size(PHASE)) < (gen_config.num_processors * 8) && ret == 1 && unemployed == 1)
                {
                    PRINT_DEBUG("special gather happens!\n");
                    signal_of_partition_gather = 1;
                    cal_threshold();
                    gather_updates(1-PHASE, super_phase, -1);
                }

                if ((num_finished + num_not_finished) != gen_config.num_processors)
                    PRINT_ERROR("OH!NO!\n");

                if (ret == 0 && unemployed == 1)
                {
                    //All cpus have finished scatter_updates! Now we just need to go back to normal gather.
                    assert(num_finished == gen_config.num_processors);
                    signal_of_partition_gather = 0;
                    //PRINT_DEBUG("All cpus have finished scatter\n");
                    //return ret;
                }
                if (ret == 1 && unemployed == 0)
                {
                    assert(num_not_finished == gen_config.num_processors);
                    signal_of_partition_gather = 1;
                    //PRINT_DEBUG("ALL CPUS have not finished scatter beacuse the update_bufs are full!\n");
                    cal_threshold();
                    gather_updates(1-PHASE, super_phase, -1);
                }
                if (unemployed == 1 && ret == 1 && ((cal_true_bits_size(PHASE)) >= (gen_config.num_processors * 8)))
                {
                    //PRINT_DEBUG("some cpus have finished scatter! Others not!\n");
                    signal_of_partition_gather = 2;
                    cal_threshold();
                    gather_updates(1-PHASE, -1, -1);
                    cal_threshold();

                    
                    for (u32_t k = 0; k < num_not_finished; k++)
                    {
                        ret = 0;
                        u32_t ret_value = rebalance_sched_bitmap(p_not_finished[k], PHASE);
                        if (ret_value == 2)
                            PRINT_DEBUG("cpu-%d has so few bits to steal!To be continued\n", p_not_finished[k]);
                        u32_t special_signal;
                        do
                        {
                            special_signal = 0;
                            scatter_cpu_work = new cpu_work_target<A, VA>(SCATTER, (void *)p_scatter_param);
                            pcpu_threads[0]->work_to_do = scatter_cpu_work;
                            (*pcpu_threads[0])();

                            delete scatter_cpu_work;
                            scatter_cpu_work = NULL;

                            PRINT_DEBUG("After steal scatter!\n");
                            for (u32_t i = 0; i < gen_config.num_processors; i++)
                            {
                                context_data * context_data_steal = PHASE > 0 ? 
                                    seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1: 
                                    seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0;
                                context_data * context_data_not_finished = PHASE > 0 ?
                                    seg_config->per_cpu_info_list[p_not_finished[k]]->sched_manager->p_context_data1: 
                                    seg_config->per_cpu_info_list[p_not_finished[k]]->sched_manager->p_context_data0;
                                
                                context_data_not_finished->per_bits_true_size -= context_data_steal->steal_bits_true_size;
                                PRINT_DEBUG("per_bits_not_continued = %d\n", 
                                        context_data_not_finished->per_bits_true_size);
                                context_data_steal->steal_bits_true_size = 0;
                                if(pcpu_threads[i]->status != FINISHED_SCATTER)
                                {
                                    PRINT_DEBUG("In steal-mode, processor:%d has not finished scatter!Not very good!\n", i);
                                    special_signal = 1;
                                    set_signal_to_scatter(3, i, PHASE);
                                }
                                if (pcpu_threads[i]->status != UPDATE_BUF_FULL )
                                {
                                    PRINT_DEBUG("Steal round %d, Processor: %d has finished scatter, and the update buffer is not full!\n", k, i);
                                    set_signal_to_scatter(2, i, PHASE);
                                    context_data_steal->steal_min_vert_id = 0;
                                    context_data_steal->steal_max_vert_id = 0;
                                    //context_data_steal->steal_context_edge_id = 0;
                                }
                            }
                            if (special_signal == 1)
                            {
                                signal_of_partition_gather = 2;
                                cal_threshold();
                                gather_updates(1-PHASE, -1, -1);
                                cal_threshold();
                            }
                        }while(special_signal == 1);
                    }
                    PRINT_DEBUG("After steal!\n");
                    ret = 0;
                }
                super_phase++; 
            }while(ret == 1);
            //return ret;
            if (p_not_finished)
                free(p_not_finished);
            if (p_finished)
                free(p_finished);
            signal_of_partition_gather = 0;
            //seg_config->buf0_holder = seg_config->buf1_holder = -1;
            memset(p_strip_count, 0, sizeof(u32_t) * seg_config->num_segments);
            reset_manager(PHASE);
            return ret;
        }

        void reset_manager(u32_t PHASE)
        {
            context_data * my_context_data;
            for (u32_t i = 0 ; i < gen_config.num_processors; i++ )
            {
                my_context_data = PHASE > 0 ? seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1
                    : seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0;
                if(my_context_data->per_bits_true_size != 0)
                    PRINT_ERROR("Per_bits_true_size != 0, impossible!\n");
                    //PRINT_ERROR("Per_bits_true_size != 0, impossible!\n");
                my_context_data->per_bits_true_size = 0;
                my_context_data->steal_min_vert_id = 0;
                my_context_data->steal_max_vert_id = 0;
                my_context_data->signal_to_scatter = 0;
                my_context_data->p_bitmap_steal = NULL;
                my_context_data->steal_virt_cpu_id = 0;
                my_context_data->steal_num_virt_cpus = 0;
                my_context_data->steal_bits_true_size = 0;
                my_context_data->steal_context_edge_id = 0;

                my_context_data->partition_gather_signal = 0;
                my_context_data->partition_gather_strip_id = -1;

                //my_context_data->p_bitmap->memset_buffer();

            }
        }

        //return value: 
        //1:success
        //2: this (not_finished_cpu) cpu has few bits
        u32_t rebalance_sched_bitmap(u32_t cpu_not_finished_id, u32_t PHASE)
        {

            u32_t cpu_id;
            context_data * context_data_not_finished = PHASE > 0 ? seg_config->per_cpu_info_list[cpu_not_finished_id]->sched_manager->p_context_data1 : seg_config->per_cpu_info_list[cpu_not_finished_id]->sched_manager->p_context_data0;
            u32_t min_vert = context_data_not_finished->per_min_vert_id;
            u32_t max_vert = context_data_not_finished->per_max_vert_id;
            u32_t average_num = (max_vert - min_vert + 1)/(gen_config.num_processors);
            //PRINT_DEBUG("In cpu %d, min_vert = %d, max_vert = %d, average_num = %d\n",
             //               cpu_not_finished_id, min_vert, max_vert, average_num);
                
            if ((average_num/8) == 0)
                return 2;

            u32_t tmp_min_vert = 0;
            u32_t tmp_max_vert = 0;
            u32_t tmp_index = 0;
            for (cpu_id = 0; cpu_id < gen_config.num_processors; cpu_id++)
            {
                //set STEAL signal to next scatter
                set_signal_to_scatter(2, cpu_id, PHASE);
                context_data * context_data_steal = PHASE > 0 ? seg_config->per_cpu_info_list[cpu_id]->sched_manager->p_context_data1 : seg_config->per_cpu_info_list[cpu_id]->sched_manager->p_context_data0;
                context_data_steal->steal_virt_cpu_id = cpu_not_finished_id;
                context_data_steal->p_bitmap_steal = context_data_not_finished->p_bitmap;

                if (cpu_id == 0)
                {
                    tmp_min_vert = min_vert;
                }
                else
                {
                    tmp_min_vert = tmp_max_vert + gen_config.num_processors;
                }
                context_data_steal->steal_min_vert_id = tmp_min_vert; 
                PRINT_DEBUG("Processor-%d's steal_min_vert = %d\n", cpu_id,tmp_min_vert);

                if (cpu_id == gen_config.num_processors - 1)
                {
                    tmp_max_vert = max_vert;
                }
                else
                {
                    tmp_max_vert = tmp_min_vert + average_num;
                    tmp_index = (tmp_max_vert - cpu_not_finished_id)/(gen_config.num_processors);

                    if ((tmp_index % 8) == 0)
                        tmp_index--;
                    else
                    {
                        u32_t tmp_val = tmp_index%8;
                        tmp_index += 8 - tmp_val - 1;
                    }
                    tmp_max_vert = tmp_index*(gen_config.num_processors) + cpu_not_finished_id;
                }
                context_data_steal->steal_max_vert_id = tmp_max_vert;
                PRINT_DEBUG("Processor-%d's steal_max_vert = %d\n", cpu_id, tmp_max_vert);
            }
            return 1;
        }
        
        //gather all updates in the update buffer.
        void gather_updates(u32_t PHASE, int super_phase, int loop_counter)
        {
            cpu_work_target<A,VA>* gather_cpu_work = NULL;
            io_work_target * one_io_work = NULL;
            char * next_buffer = NULL, *read_buf = NULL;
            char * write_buf = NULL; /** next_write_buf = NULL;*/
            //u32_t begin_with;
            u64_t offset, read_size;
            gather_param_target * p_gather_param = new gather_param_target;
            u32_t ret = 0;
            //u32_t index_j;

			fog_engine_target_state = GATHER;

            if (seg_config->num_attr_buf == 1)
            {
                p_gather_param->attr_array_head = (void*)seg_config->attr_buf0;
                p_gather_param->PHASE = PHASE;
                p_gather_param->threshold = 0;
                p_gather_param->strip_id = 0;

                gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param);
                pcpu_threads[0]->work_to_do = gather_cpu_work;
                (*pcpu_threads[0])();

                delete gather_cpu_work;
                gather_cpu_work = NULL;
            }
            else
            {
                //if ((ret = unmap_attr_file()) < 0) 
                 //   PRINT_ERROR("unmap attr file error!\n");
                //means all cpus have finished scatter, so we need to gather
                //But, we also need to notice that some strips may be empty.
                //if (signal_of_partition_gather == 0 || signal_of_partition_gather == 2)
                if (signal_of_partition_gather == 0 || signal_of_partition_gather == 2)
                {
                    if (signal_of_partition_gather == 0)
                        PRINT_DEBUG("Normal gather starts!\n");
                    else if (signal_of_partition_gather ==2)
                        PRINT_DEBUG("Steal gather starts!\n");
                //    int continue_normal_gather = -1;
                    //mmap gather
                    //test -- not execution
                    /*if (signal_of_partition_gather == 1 && seg_config->buf0_holder == -1 && seg_config->buf1_holder == -1)
                    {
                        //We can begin mmap-gather
                        int mmap_ret = -1;
                        int signal_mmap = -1;
                        //int mmap_strip0 = -1;
                        for (u32_t i = 0; i < seg_config->num_segments; i++)
                        {
                            mmap_ret = cal_strip_size(i, 1, 1);
                            if (mmap_ret == 1 && i != 0)
                            {
                                signal_mmap = 0;
                                break;
                            }
                            //if (mmap_ret == 1 && i == 0)
                            //    mmap_strip0 = 0;
                            //else if (mmap_ret == 0 && i == 0)
                            //    mmap_strip0 = 1;
                            //signal_mmap = 1;
                        }
                        if (signal_mmap == 1)
                        {

                            PRINT_DEBUG("gather-loop:%d, need to mmap_gather\n", loop_counter);
                            u32_t mmap_index = 0;
                            if (remap_write_attr_file() < 0)
                            {
                                PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                            }
                            p_gather_param->attr_array_head = (void *)attr_array_header;
                            for(u32_t j = mmap_index; j < seg_config->num_segments; j++)
                            {
                                //PRINT_DEBUG("mmap_gather:%d\n", j);
                                p_gather_param->attr_array_head = (void *)attr_array_header;
                                p_gather_param->PHASE = PHASE;
                                p_gather_param->threshold = 0;
                                p_gather_param->strip_id = j;

                                gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param);
                                pcpu_threads[0]->work_to_do = gather_cpu_work;
                                (*pcpu_threads[0])();

                                delete gather_cpu_work;
                                gather_cpu_work = NULL;
                            }
                        }
                        else
                        {
                            //PRINT_DEBUG("no need to mmap-gather!\n");
                            continue_normal_gather = 1;
                        }
                    }*/
                    //else
                     //   continue_normal_gather = 1;

                   // if (continue_normal_gather == 1)
                    //{
                        u32_t buf_index = 0;
                        for (u32_t i = 0; i < 2; i++)
                        {
                            u32_t tmp_strip_id;

                            if (i == 0)
                            {
                                if (seg_config->buf0_holder == -1)
                                    continue;
                                else
                                {
                                    read_buf = (char *)seg_config->attr_buf0;
                                    tmp_strip_id = seg_config->buf0_holder;
                                }
                            }
                            else
                            {
                                if (seg_config->buf1_holder == -1)
                                    continue;
                                else
                                {
                                    read_buf = (char *)seg_config->attr_buf1;
                                    tmp_strip_id = seg_config->buf1_holder;
                                }
                            }
                            p_gather_param->threshold = 1;
                            p_gather_param->strip_id = (int)tmp_strip_id;
                            p_gather_param->PHASE = PHASE;

                            p_gather_param->attr_array_head = (void *)read_buf;
                            //PRINT_DEBUG("Earlier gather strip %d\n", tmp_strip_id);

                            gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param);
                            pcpu_threads[0]->work_to_do = gather_cpu_work;
                            (*pcpu_threads[0])();

                            delete gather_cpu_work;
                            gather_cpu_work = NULL;
                            //we need to write the attr_buf to attr_file 
                            if (one_io_work != NULL)
                            {
                                fog_io_queue->wait_for_io_task(one_io_work);
                                fog_io_queue->del_io_task(one_io_work);
                                one_io_work = NULL;
                            }

                            if (tmp_strip_id != (seg_config->num_segments-1) )
                            {
                                one_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                        FILE_WRITE, read_buf, 
                                        (u64_t)tmp_strip_id * seg_config->segment_cap*sizeof(VA), (u64_t)seg_config->segment_cap*sizeof(VA));
                            }
                            else
                            {
                                one_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                        FILE_WRITE, read_buf, 
                                        (u64_t)tmp_strip_id * seg_config->segment_cap*sizeof(VA),
                                        (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA));
                            }

                            fog_io_queue->add_io_task(one_io_work);
                            if (one_io_work != NULL)
                            {
                                fog_io_queue->wait_for_io_task(one_io_work);
                                fog_io_queue->del_io_task(one_io_work);
                                one_io_work = NULL;
                            }
                        }

                        if (one_io_work != NULL)
                        {
                            fog_io_queue->wait_for_io_task(one_io_work);
                            fog_io_queue->del_io_task(one_io_work);
                            one_io_work = NULL;
                        }
                        if (seg_config->buf0_holder != -1 && seg_config->buf1_holder == -1)
                            buf_index = 1;
                        else
                            buf_index = 0;

                        //u32_t next_buffer_signal = 0;
                        int mmap_ret = -1;
                        //int next_mmap_ret = -1;
                        
                        for (u32_t i = 0; i < seg_config->num_segments; i++)
                        {
                            mmap_ret = cal_strip_size(i,1,1);
                            if (mmap_ret == 0)
                                break;
                        }
                        if (mmap_ret == 0)
                            if (remap_write_attr_file() < 0)
                            {
                                PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                            }

                        io_work_target * write_io_work = NULL;

                        for(u32_t i = 0; i < seg_config->num_segments; i++)
                        {
                            //check if this strip is zero or has been early-gather
                            ret = cal_strip_size(i, 0, 0);
                            if (ret == 0 || (int)i == seg_config->buf0_holder  || (int)i == seg_config->buf1_holder)
                                continue;
                            PRINT_DEBUG( "i=%d, buf_index = %d, one_io_work=%llx\n", i, buf_index, (u64_t)one_io_work );

                            mmap_ret = cal_strip_size(i, 1, 1);

                            //this strip will be gather by mmap
                            if (mmap_ret == 0)
                            {
                                PRINT_DEBUG("for strip %d, mmap_gather starts!\n", i);
                                p_gather_param->attr_array_head = (void *)attr_array_header;
                                p_gather_param->PHASE = PHASE;
                                p_gather_param->threshold = 0;
                                p_gather_param->strip_id = i;
                            
                            }
                            else if (mmap_ret == 1)
                            {
                                if (buf_index%2) read_buf = (char *)seg_config->attr_buf1;
                                else read_buf = (char *)seg_config->attr_buf0;

                                //if (next_buffer_signal == 0)
                                //{
                                    offset = (u64_t)i * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (i == (seg_config->num_segments - 1))
                                    {
                                        read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA);
                                    }
                                    else 
                                        read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                            FILE_READ, read_buf, offset, read_size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    fog_io_queue->wait_for_io_task(one_io_work);
                                    fog_io_queue->del_io_task(one_io_work);
                                    one_io_work = NULL;
                                    PRINT_DEBUG("Finish reading the first segment!\n");
                                //}
                                //else
                                //{
                                //    read_buf = next_buffer;
                                //}
                                p_gather_param->threshold = 1;
                                p_gather_param->strip_id = (int)i;
                                p_gather_param->PHASE = PHASE;
                                p_gather_param->attr_array_head = (void *)read_buf;

                                buf_index++;
                            }
                            else 
                                PRINT_ERROR("return value is false!\n");

                            gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param);
                            pcpu_threads[0]->work_to_do = gather_cpu_work;
                            (*pcpu_threads[0])();

                            delete gather_cpu_work;
                            gather_cpu_work = NULL;
                            //we need to write the attr_buf to attr_file 
                            if (mmap_ret == 1)
                            {
                                if (write_io_work != NULL)
                                {
                                    fog_io_queue->wait_for_io_task(write_io_work);
                                    fog_io_queue->del_io_task(write_io_work);
                                    write_io_work = NULL;
                                }

                                if (i != (seg_config->num_segments-1) )
                                {
                                    write_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                            FILE_WRITE, read_buf, 
                                            (u64_t)i * seg_config->segment_cap*sizeof(VA), (u64_t)seg_config->segment_cap*sizeof(VA));
                                }
                                else
                                {
                                    write_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                            FILE_WRITE, read_buf, 
                                            (u64_t)i * seg_config->segment_cap*sizeof(VA),
                                            (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA));
                                }

                                fog_io_queue->add_io_task(write_io_work);
                            }
                            /*if (write_io_work != NULL)
                            {
                                fog_io_queue->wait_for_io_task(write_io_work);
                                fog_io_queue->del_io_task(write_io_work);
                                write_io_work = NULL;
                            }*/
                            //i = index_j - 1;//Hint:for-loop
                        }
                        if (write_io_work != NULL)
                        {
                            fog_io_queue->wait_for_io_task(write_io_work);
                            fog_io_queue->del_io_task(write_io_work);
                        }
                        seg_config->buf0_holder = seg_config->buf1_holder = -1;

                            
                            /*index_j = i+1;
                            //while ((i+1)< seg_config->num_segments)
                            //if ((i+1)< seg_config->num_segments)
                            while (index_j < seg_config->num_segments)
                            {
                                //if (signal_of_partition_gather == 0)
                                    ret = cal_strip_size(index_j, 0, 0);
                                //if (signal_of_partition_gather == 2)
                                 //   ret = cal_strip_size(index_j, 0, 1);
                                if (ret == 0)//means this strip is ZERO
                                    index_j++;
                                else
                                {
                                    next_mmap_ret = cal_strip_size(i, 1, 1);
                                    if (next_mmap_ret == 1)
                                    buf_index++;
                                    //PRINT_DEBUG("buf_index = %d\n", buf_index);
                                    //next buffer must different from read_buf
                                    //if ((i+1)%2) next_buffer = (char *)seg_config->attr_buf1;
                                    if ((buf_index)%2) next_buffer = (char *)seg_config->attr_buf1;
                                    else  next_buffer = (char *)seg_config->attr_buf0;

                                    offset = (u64_t)(index_j)*(u64_t)seg_config->segment_cap*sizeof(VA);
                                    //offset = (u64_t)(i+1)*(u64_t)seg_config->segment_cap*sizeof(VA);

                                    //if ((i+1) == (seg_config->num_segments - 1))
                                    if ((index_j) == (seg_config->num_segments - 1))
                                    {
                                        read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA);
                                    }
                                    else 
                                        read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                            FILE_READ, next_buffer, offset, read_size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    next_buffer_signal = 1;
                                    //PRINT_DEBUG( "will invoke next read io work, offset %llu, size:%llu\n", offset, read_size );
                                    break;
                                }
                            }*/

                    //}
                }
                else if (signal_of_partition_gather == 1) // means ALL cpus's buffer are FULL
                {
                    PRINT_DEBUG("Super gather starts!\n");
                    u32_t num_hits = 0;
                    int partition_gather_array[(gen_config.num_processors)];
                    u32_t tmp_num = 0;
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                    {
                        context_data * my_context_data = (1-PHASE) > 0 ? 
                            seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1
                            : seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0;
                        int tmp_strip_id = my_context_data->partition_gather_strip_id;
                        PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                        if (tmp_strip_id == -1)
                            continue;
                        u32_t out_signal = 0;
                        int tmp_id = -1;
                        int ret = -1;

                        if (i > 0)
                        {
                            for (u32_t j = 0; j < i; j++)
                            {
                                if (partition_gather_array[j] == tmp_strip_id)
                                {
                                    out_signal = 1;
                                    break;
                                }
                            }
                            if (out_signal == 0)
                            {
                                ret = lru_hit_target(tmp_strip_id);
                                if (ret == 1)
                                {
                                    if (num_hits == 0)
                                    {
                                        tmp_id = partition_gather_array[0];
                                        partition_gather_array[0] = tmp_strip_id;
                                        partition_gather_array[tmp_num] = tmp_id;
                                        tmp_num++;
                                        num_hits++;
                                    }
                                    else if (num_hits == 1)
                                    {
                                        if (tmp_num == 1)
                                        {
                                            partition_gather_array[1] = tmp_strip_id;
                                            tmp_num++;
                                            num_hits++;
                                        }
                                        else//i > 1
                                        {
                                            tmp_id = partition_gather_array[num_hits];
                                            partition_gather_array[num_hits] = tmp_strip_id;
                                            partition_gather_array[tmp_num] = tmp_id;
                                            tmp_num++;
                                            num_hits++;
                                        }
                                    }
                                    else
                                        PRINT_ERROR("Impossible~\n");
                                }
                                else if (ret == 0)
                                {
                                    partition_gather_array[tmp_num] = tmp_strip_id;
                                    tmp_num++;
                                }
                                else
                                    PRINT_ERROR("error happens here!\n");
                            }
                        }
                        if (i == 0)
                        {
                            assert(out_signal == 0);
                            partition_gather_array[0] = tmp_strip_id;
                            tmp_num++;
                            ret = lru_hit_target(tmp_strip_id);
                            if (ret == 1)
                                num_hits++;
                        }
                    }
                    for (u32_t i = 0; i < tmp_num; i++)
                        PRINT_DEBUG("tmp_strip_id[%d] = %d\n",i ,partition_gather_array[i]);
                    PRINT_DEBUG("In super_phase:%d, tmp_num = %d, num_hits = %d\n", super_phase, tmp_num, num_hits);

                    for (u32_t i = 0; i < tmp_num; i++)
                    {
                        int tmp_strip_id = partition_gather_array[i];
                        PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                        PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                        PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                        int ret = -1;
                        ret = lru_hit_target(tmp_strip_id);
                        if (i > 0)
                        {
                            assert(ret == 1);
                        }
                        if (ret == 1)
                        {
                            //this strip_id hits target
                            //PRINT_DEBUG("strip:%d hits target, no need to do read_io_work~!\n", tmp_strip_id);
                            read_buf = get_target_buf_addr(tmp_strip_id);
                            if (read_buf == NULL)
                                PRINT_ERROR("strip:%d hits target, but something error happen!\n", tmp_strip_id);
                        }
                        else if(ret == 0)
                        {
                            if (i > 0)
                                PRINT_ERROR("It is impossible~!\n");
                            if (super_phase == (int)0 && i == 0)
                            {
                                assert(seg_config->buf0_holder == -1);
                                assert(seg_config->buf1_holder == -1);
                                seg_config->buf0_holder = tmp_strip_id;//
                                assert(one_io_work == NULL); 
                                read_buf = (char *)seg_config->attr_buf0;
                                if (one_io_work != NULL)
                                {
                                    fog_io_queue->wait_for_io_task(one_io_work);
                                    fog_io_queue->del_io_task(one_io_work);
                                    one_io_work = NULL;
                                }
                                u64_t offset, size;
                                offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                {
                                    size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                }
                                else
                                    size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                        FILE_READ, read_buf, offset, size);
                                fog_io_queue->add_io_task(one_io_work);
                                if (one_io_work != NULL)
                                {
                                    fog_io_queue->wait_for_io_task(one_io_work);
                                    fog_io_queue->del_io_task(one_io_work);
                                    one_io_work = NULL;
                                }
                            }
                            else //if (super_phase > 0 && i == 0)
                            {
                                assert(i == 0);
                                assert(super_phase > 0);
                                u32_t num_free_bufs = get_free_buf_num();
                                //PRINT_DEBUG("num_free_bufs = %d\n", num_free_bufs);
                                if (num_free_bufs == 0)
                                {
                                    PRINT_DEBUG("this is the strip:%d, need to change 1 segment out~!\n", tmp_strip_id);
                                    int changed_strip_id = -1;
                                    //seg_config->buf1_holder will be changed out
                                    PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                                    PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                                    if (p_strip_count[seg_config->buf0_holder] >= p_strip_count[seg_config->buf1_holder])
                                    {
                                        changed_strip_id = seg_config->buf1_holder;
                                        seg_config->buf1_holder = tmp_strip_id;
                                    }
                                    else// seg_config->buf0_holder will be changed out
                                    {
                                        changed_strip_id = seg_config->buf0_holder;
                                        seg_config->buf0_holder = tmp_strip_id;
                                    }
                                    //when changing the strip happens
                                    //first, we shold store the changed_strip
                                    write_buf = get_target_buf_addr(tmp_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)changed_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (changed_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_WRITE, write_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    PRINT_DEBUG("Strip:%d has been changed out!\n", changed_strip_id);

                                    //must wait before next read
                                    read_buf = get_target_buf_addr(tmp_strip_id);
                                    if (read_buf == NULL)
                                        PRINT_ERROR("caOCACOAOCAO\n");
                                    PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                                    PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                                    PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, read_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (num_free_bufs == 1)
                                {
                                    if (get_free_buf_id() == 1)
                                    {
                                        seg_config->buf0_holder = tmp_strip_id;
                                    }
                                    else if (get_free_buf_id() == 2)
                                    {
                                        seg_config->buf1_holder = tmp_strip_id;
                                    }
                                    read_buf = get_target_buf_addr(tmp_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, read_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (num_free_bufs == 2)
                                    PRINT_ERROR("it is impossible!\n");
                                else
                                    PRINT_ERROR("it is also impossible!\n");
                            }
                        }
                        p_gather_param->attr_array_head = (void *)read_buf;
                        p_strip_count[tmp_strip_id]++;
                        //read next buffer if necessary
                        if ((i+1) < tmp_num )
                        {
                            int next_strip_id = partition_gather_array[i+1];
                            PRINT_DEBUG("next_strip_id = %d\n", next_strip_id);
                            ret = lru_hit_target(next_strip_id);
                            if (ret == 0)
                            {
                                //PRINT_DEBUG("next buffer need to be loaded~!\n");
                                u32_t tmp_free_buf_nums = get_free_buf_num();
                                if (tmp_free_buf_nums == 2)
                                    PRINT_ERROR("tmp_free_buf_nums = 2, nonono\n");
                                else if (tmp_free_buf_nums == 1)
                                {
                                    if (get_free_buf_id() == 1)
                                    {
                                        seg_config->buf0_holder = next_strip_id;
                                    }
                                    else if (get_free_buf_id() == 2)
                                    {
                                        seg_config->buf1_holder = next_strip_id;
                                    }
                                    else if (get_free_buf_id() == 0)
                                        PRINT_ERROR("Impossible~\n");
                                    
                                    next_buffer = get_target_buf_addr(next_strip_id);
                                    //p_strip_count[next_strip_id]++;
                                    //do_io_work(next_strip_id, FILE_READ, next_buffer, one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)next_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (next_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, next_buffer, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (tmp_free_buf_nums == 0)
                                {
                                    //need to changed out some strips
                                    int changed_strip_id = -1;
                                    if (seg_config->buf0_holder == (int)tmp_strip_id)
                                    {
                                        changed_strip_id = seg_config->buf1_holder;
                                        seg_config->buf1_holder = next_strip_id;
                                    }
                                    else if (seg_config->buf1_holder == (int)tmp_strip_id )
                                    {
                                        changed_strip_id = seg_config->buf0_holder;
                                        seg_config->buf0_holder = next_strip_id;
                                    }
                                    else 
                                        PRINT_ERROR("no strip to change out~!\n");
                                    PRINT_DEBUG("next_strip_id = %d, need to changed strip:%d out\n", next_strip_id, changed_strip_id);
                                    
                                    //first, we shold store the changed_strip
                                    write_buf = get_target_buf_addr(next_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)changed_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (changed_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_WRITE, write_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);

                                    next_buffer = get_target_buf_addr(next_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    offset = (u64_t)next_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (next_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, next_buffer, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }

                                }
                            }
                        }
                        p_gather_param->threshold = 1;
                        p_gather_param->strip_id = tmp_strip_id;
                        p_gather_param->PHASE = PHASE;

                        gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                    }
                } 
            }
            PRINT_DEBUG("After Gather!\n");
        }

        int lru_hit_target(int strip_id)
        {
            if (seg_config->buf0_holder == strip_id || seg_config->buf1_holder == strip_id)
                return 1;
            else
                return 0;
        }

        u32_t get_free_buf_num()
        {
            if (seg_config->buf0_holder == -1 && seg_config->buf1_holder == -1)
                return 2;
            else if (seg_config->buf0_holder != -1 && seg_config->buf1_holder != -1)
                return 0;
            else
                return 1;
        }

        //ret 0:none or two 
        //ret 1:seg_config->buf0_holder
        //ret 2:seg_config->buf1_holder
        u32_t get_free_buf_id()
        {
            if (seg_config->buf0_holder == -1 && seg_config->buf1_holder != -1)
                return 1;
            else if (seg_config->buf0_holder != -1 && seg_config->buf1_holder == -1)
                return 2;
            else 
                return 0;
        }

        char * get_target_buf_addr(int strip_id)
        {
            if (strip_id == -1)
                PRINT_ERROR("Onononono\n");
            if (strip_id == seg_config->buf0_holder)
                return (char *)seg_config->attr_buf0;
            else if (strip_id == seg_config->buf1_holder)
                return (char *)seg_config->attr_buf1;
            else
                return NULL;
        }

        void do_io_work(int strip_id, u32_t operation, char * io_buf, io_work_target * one_io_work)
        {
            if (one_io_work != NULL)
            {
                fog_io_queue->wait_for_io_task(one_io_work);
                fog_io_queue->del_io_task(one_io_work);
                one_io_work = NULL;
            }
            assert(io_buf != NULL);
            u64_t offset, size;
            offset = (u64_t)strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
            if (strip_id == (int)(seg_config->num_segments - 1))
            {
                size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
            }
            else
                size = (u64_t)(seg_config->segment_cap*sizeof(VA));
            one_io_work = new io_work_target(gen_config.attr_file_name.c_str(), 
                    operation, io_buf, offset, size);
            fog_io_queue->add_io_task(one_io_work);
        }

        //return:
        //0:The strip_id-buffer of all cpus are ZERO
        //1:some buffer is not ZERO
        u32_t cal_strip_size(int strip_id, u32_t util_rate_signal, u32_t signal_threshold)
        {
            update_map_manager * map_manager;
            u32_t * map_head;
            u32_t strip_cap;
            u32_t total_updates;
            u32_t util_ret = 0; 
            u32_t strip_ret = 0;
            u32_t threshold_ret = 0;
            double util_rate;

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                if (strip_id >= 0 && signal_threshold == 0)
                {
                    for (u32_t j = 0; j < gen_config.num_processors; j++)
                    {
                        if ((*(map_head + strip_id * gen_config.num_processors + j)) != 0)
                        {
                            //PRINT_DEBUG("CPU = %d, strip_id = %d, map_value = %d\n", j, strip_id, 
                                //(*(map_head + strip_id * gen_config.num_processors + j))); 
                            strip_ret = 1;
                            return strip_ret;
                        }
                    }
                }

                if (util_rate_signal == 1 && signal_threshold == 0)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t j = 0; j < (seg_config->num_segments*gen_config.num_processors); j++)
                        total_updates += *(map_head+j);
                    util_rate = (double)total_updates/((double)strip_cap*seg_config->num_segments);
                    PRINT_DEBUG("THere are %u update in processor %d, utilization rate is %f\n", total_updates, i, util_rate);
                    if (util_rate > THRESHOLD)
                    {
                        util_ret = 1;
                        return util_ret;
                    }
                    
                }

                if (strip_id >= 0  && signal_threshold == 1)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                    {
                            total_updates += (*(map_head + strip_id * gen_config.num_processors + i));
                    }
                    util_rate = (double)total_updates/((double)strip_cap);
                    if (util_rate >= THRESHOLD)
                    {
                        threshold_ret = 1;
                        return threshold_ret;
                    }
                }

                if (strip_id >= 0 && util_rate_signal == 1 && signal_threshold == 1)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                        total_updates += (*(map_head + strip_id * gen_config.num_processors + i));
                    util_rate = (double)total_updates/((double)strip_cap);
                    if (util_rate >= MMAP_THRESHOLD)
                        return 1;
                }
            }
            return 0;
        }
        u32_t cal_threshold()
        {
            update_map_manager * map_manager;
            u32_t * map_head;
            u32_t strip_cap;
            u32_t total_updates;
            u32_t ret = 0; 
            double util_rate;

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                show_update_map(i, map_head);
            }

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                total_updates = 0;
                for (u32_t j = 0; j < (seg_config->num_segments*gen_config.num_processors); j++)
                    total_updates += *(map_head+j);

                util_rate = (double)total_updates/((double)strip_cap*seg_config->num_segments);
                //PRINT_DEBUG("THere are %u update in processor %d, utilization rate is %f\n", total_updates, i, (double)total_updates/((double)strip_cap*seg_config->num_segments));
                PRINT_DEBUG("THere are %u update in processor %d, utilization rate is %f\n", total_updates, i, util_rate);
                //PRINT_DEBUG("THRESHOLD = %f\n", THRESHOLD );

                if (util_rate > THRESHOLD)
                {
                    ret = 1;
                }
            }
            return ret=0?1:0;
        }

        void show_update_map(int processor_id, u32_t * map_head)
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

        //map the attribute file
        //return value:
        //0 means success
        //-1 means failure
        int map_attr_file()
        {
            struct stat st;
            char *memblock;

            attr_fd = open(gen_config.attr_file_name.c_str(),O_RDONLY );
            if (attr_fd < 0)
            {
                PRINT_ERROR("fog_engine_target::map_attr_file cannot open attribute file!\n");
                return -1;
            }
            fstat(attr_fd, &st);
            attr_file_length = (u64_t)st.st_size;

            //memblock = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE|MAP_NORESERVE, attr_fd, 0);
            memblock = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, attr_fd, 0);
            if (MAP_FAILED == memblock)
            {
                close(attr_fd);
                PRINT_ERROR("index file mapping failed!\n");
                exit(-1);
            }
            attr_array_header = (VA*)memblock;
            return 0;
        }

        //munmap the attribute file
        //return value:
        //0 means success;
        //-1 means failure
        int unmap_attr_file()
        {
            if (munmap((void*)attr_array_header, attr_file_length) == -1)
                PRINT_ERROR("unmap_filer error!\n");
            close(attr_fd);
            return 0;
        }

        int map_write_attr_file()
        {
            struct stat st;
            char *memblock;

            attr_fd = open(gen_config.attr_file_name.c_str(),O_RDWR, S_IRUSR|S_IWUSR);
            if (attr_fd < 0)
            {
                PRINT_ERROR("fog_engine_target::map_attr_file cannot open attribute file!\n");
                return -1;
            }
            fstat(attr_fd, &st);
            attr_file_length = (u64_t)st.st_size;

            memblock = (char *)mmap(NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, attr_fd, 0);
            if (MAP_FAILED == memblock)
            {
                close(attr_fd);
                PRINT_ERROR("index file mapping failed!\n");
                exit(-1);
            }
            attr_array_header = (VA*)memblock;
            return 0;
        }




        int remap_write_attr_file()
        {
            int ret;
            if (attr_fd)
                if ((ret = unmap_attr_file()) < 0) return ret;
            if ((ret = map_write_attr_file()) < 0) return ret;

            return 0;
        }
        //remap
        int remap_attr_file()
        {
            int ret;
            //PRINT_DEBUG("attr_fd = %d\n", attr_fd);

            if (attr_fd)
                if ( (ret = unmap_attr_file()) < 0 ) return ret;
            if ((ret = map_attr_file()) < 0 ) return ret;

            return 0;
        }

        static void add_schedule(u32_t task_vid, u32_t PHASE)
        {
            u32_t  partition_id;
            bitmap * current_bitmap = NULL ;
            u32_t max_vert = 0;
            u32_t min_vert = 0;
            int schedule_signal = 0;

            partition_id = VID_TO_PARTITION(task_vid);
            assert(task_vid <= gen_config.max_vert_id);
            assert(partition_id < gen_config.num_processors);
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            struct context_data * old_context_data;//on scatter!
            bitmap * old_bitmap = NULL;

            my_sched_bitmap_manager = seg_config->per_cpu_info_list[partition_id]->sched_manager;
            my_context_data = PHASE > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;
            u32_t old_phase = 1 - PHASE;
            old_context_data = old_phase > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;

            if (seg_config->num_attr_buf == 1 && old_context_data->per_bits_true_size > 0)
            {
                assert(old_context_data->per_bits_true_size > 0);
                old_bitmap = old_context_data->p_bitmap;
                if (old_bitmap->get_value(task_vid) == 1 && task_vid != (old_context_data->per_min_vert_id))
                    schedule_signal = 1;
            }

            max_vert = my_context_data->per_max_vert_id;
            min_vert = my_context_data->per_min_vert_id;
            current_bitmap = my_context_data->p_bitmap;

            if (current_bitmap->get_value(task_vid) == 0 && schedule_signal == 0)
            {
                my_context_data->per_bits_true_size++;
                current_bitmap->set_value(task_vid);
                if (task_vid <= min_vert)
                {
                    min_vert = task_vid;
                    my_context_data->per_min_vert_id = min_vert;
                }
                if (task_vid >= max_vert)
                {
                    max_vert = task_vid;
                    my_context_data->per_max_vert_id = max_vert;
                }
            }
        }
		void reclaim_everything()
		{
			PRINT_DEBUG( "begin to reclaim everything\n" );
			//reclaim pre-allocated space

			for(u32_t i=0; i<gen_config.num_processors; i++)
            {
                delete seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->p_bitmap;
                delete seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->p_bitmap;
                PRINT_DEBUG("Delete bitmap!\n");
            }

			munlock( buf_for_write, gen_config.memory_size );
			munmap( buf_for_write, gen_config.memory_size );

			//terminate the cpu threads
			pcpu_threads[0]->terminate = true;
			pcpu_threads[0]->work_to_do = NULL;
			(*pcpu_threads[0])();

			for(u32_t i=1; i<gen_config.num_processors; i++) {
		        boost_pcpu_threads[i]->join();
			}
			for(u32_t i=0; i<gen_config.num_processors; i++)
				delete pcpu_threads[i];


			//terminate the disk thread
			delete fog_io_queue;

			//destroy the vertices mapping
			delete vert_index;

			delete seg_config;

			PRINT_DEBUG( "everything reclaimed!\n" );
		}


		void show_sched_bitmap_tasks( sched_bitmap_manager *sched_manager )
		{
		}



		//will browse all sched_lists and list the tasks.
		/*void show_all_sched_tasks()
		{
			sched_bitmap_manager* sched_manager;

			PRINT_DEBUG( "==========================	Browse all scheduled tasks	==========================\n" );
			//browse all cpus
			for(u32_t i=0; i<gen_config.num_processors; i++){
				sched_manager = seg_config->per_cpu_info_list[i]->sched_manager;
				PRINT_DEBUG( "Processor %d: Number of scheduled tasks: %d, Details:\n", i,
					sched_manager->sched_task_counter );
				show_sched_list_tasks( sched_manager );
			}
			PRINT_DEBUG( "==========================	That's All	==========================\n" );
		}*/
	


		//Note:
		// Should show this configuration information at each run, especially when
		//	it is not debugging.
		// TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.
		void show_sched_update_buf()
		{
			PRINT_DEBUG( "===============\tsched_update buffer for each CPU begin\t=============\n" );
			PRINT_DEBUG( "CPU\tSched_bitmap_manger\tUpdate_map_mangert\n");
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG("%d\t0x%llx\t0x%llx\n",
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager
                    );

			PRINT_DEBUG( "------------------\tschedule bitmap manager\t---------------\n" );
			PRINT_DEBUG( "------------------\tcontext data0\t---------------\n" );
			PRINT_DEBUG( "CPU\tbitmap_buf_head\tper_bitmap_buf_size per_bits_true_size edges max_id min_id\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t\t%d\t%d\t%d\t%d\t%d\n", i,
                        (u64_t)(seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bitmap_buf_head),
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bitmap_buf_size, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bits_true_size, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_num_edges, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_max_vert_id, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_min_vert_id 
                        );
			PRINT_DEBUG( "------------------\tcontext data1\t---------------\n" );
			PRINT_DEBUG( "CPU\tbitmap_buf_head\tper_bitmap_buf_size per_bits_true_size edges max_id min_id\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t\t%d\t%d\t%d\t%d\t%d\n", i,
                        (u64_t)(seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bitmap_buf_head),
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bitmap_buf_size, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bits_true_size, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_num_edges, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_max_vert_id, 
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_min_vert_id 
                        );

			PRINT_DEBUG( "------------------\tupdate manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tUpdate_map_address\tUpdate_map_size\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_head,
					(u32_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_size );

			/*PRINT_DEBUG( "------------------\tauxiliary update buffer manager\t------------\n" );
			PRINT_DEBUG( "CPU\tBuffer_begin\tBuffer_size\tUpdate_head\tBuf_cap\tNum_updates\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_size,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->update_head,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_cap,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->num_updates );*/

			PRINT_DEBUG( "------------------\tstrip buffer\t------------\n" );
			PRINT_DEBUG( "CPU\tStrip_buffer\tBuffer_size\tStrip_cap\tPer_cpu_strip_cap\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_buf_len,
					seg_config->per_cpu_info_list[i]->strip_cap,
					seg_config->per_cpu_info_list[i]->strip_cap/gen_config.num_processors );

			PRINT_DEBUG( "===============\tsched_update buffer for each CPU end\t=============\n" );
		}


        //LATOUT of the management data structure of fog_engine_target at the beginning of sched_update buffer;
        //designed by hejian
		//	(ordered by logical address)
		//	----------------------
		//	| sched_bitmap_manager          |
        //	---------------------------------
		//	| update_map_manager	        |
		//	----------------------------
		//	| update_map(update_map_size)   |
		//	------------------------------
        //	| bitmap0                       |
        //	-------------------------------
        //	| bitmap1                       |
        //	-------------------------------
		//	| update_buffer(strips)		    |
		//	-------------------------------
		

		void init_sched_update_buf()
		{
			u32_t update_map_size, total_header_len;
			u64_t strip_buf_size, strip_size/*, aux_update_buf_len*/;
			u32_t strip_cap;
            u32_t bitmap_buf_size; //bitmap_max_size;
            //io_work_target* init_bitmap_io_work = NULL;
            u32_t total_num_vertices;
            u32_t per_bitmap_buf_size;


			update_map_size = seg_config->num_segments 
					* gen_config.num_processors 
					* sizeof(u32_t);
            total_num_vertices = gen_config.max_vert_id + 1;

            bitmap_buf_size = (u32_t)((ROUND_UP(total_num_vertices, 8))/8);
            per_bitmap_buf_size = (u32_t)(ROUND_UP(((ROUND_UP(total_num_vertices, 8))/8), gen_config.num_processors)/gen_config.num_processors);
            bitmap_buf_size = (u32_t)(per_bitmap_buf_size * gen_config.num_processors);
            PRINT_DEBUG("num_vertices is %d, the origin bitmap_buf_size is %lf\n", total_num_vertices, (double)(total_num_vertices)/8);
            PRINT_DEBUG("After round up, the ROUND_UP(total_num_verttices,8) = %d\n", ROUND_UP(total_num_vertices, 8));
            PRINT_DEBUG("the bitmap_buf_size is %d, per_bitmap_buf_size is %d\n", bitmap_buf_size, per_bitmap_buf_size);


            //PRINT_DEBUG("the bitmap_file_size is %d\n", bitmap_file_size);
            //PRINT_DEBUG("the sched_bitmap_size is %d\n", sched_bitmap_size);

			total_header_len = sizeof(sched_bitmap_manager) 
                    + sizeof(context_data)*2
					+ sizeof(update_map_manager)
					/*+ sizeof(aux_update_buf_manager<VA>)*/
					+ update_map_size
					;

			//PRINT_DEBUG( "init_sched_update_buffer of fog_engine_target--size of sched_bitmap_manager:%lu,  size of update map manager:%lu, size of aux_update buffer manager:%lu\n", 
			PRINT_DEBUG( "init_sched_update_buffer of fog_engine_target--size of sched_bitmap_manager:%lu,  size of update map manager:%lu\n", 
				sizeof(sched_bitmap_manager), sizeof(update_map_manager));
			PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u, bitmap_buf_size:%u, total_head_len:%u\n", 
				update_map_size, bitmap_buf_size, total_header_len );
            PRINT_DEBUG("sizeof(context_data) = %ld\n", sizeof(context_data));

			//total_header_length should be round up according to the size of updates.
			total_header_len = ROUND_UP( total_header_len, sizeof(update<VA>) );

			//	CPU0 should always exist!
			//strip_buf_size = seg_config->per_cpu_info_list[0]->buf_size - total_header_len;
            //

			//divide the update buffer to "strip"s
			//strip_size = strip_buf_size / seg_config->num_segments;
			//round down strip size
			//strip_size = ROUND_DOWN( strip_size, (sizeof(update<VA>)*gen_config.num_processors) );
			//strip_cap = (u32_t)(strip_size / sizeof(update<VA>));


			//aux_update_buf_len = seg_config->aux_update_buf_len / gen_config.num_processors;
			//populate the buffer managers
			for(u32_t i=0; i<gen_config.num_processors; i++){
				//headers
				seg_config->per_cpu_info_list[i]->sched_manager = 
					(sched_bitmap_manager*)seg_config->per_cpu_info_list[i]->buf_head;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0 = 
                    (context_data *)((u64_t)seg_config->per_cpu_info_list[i]->buf_head
                            +sizeof(sched_bitmap_manager));
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1 = 
                    (context_data *)((u64_t)seg_config->per_cpu_info_list[i]->buf_head
                            +sizeof(sched_bitmap_manager) + sizeof(context_data));

				seg_config->per_cpu_info_list[i]->update_manager = 
					(update_map_manager*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_bitmap_manager) + 2*sizeof(context_data) );
                //populate the update map manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->update_manager->update_map_head = 
								(u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_bitmap_manager) + 2*sizeof(context_data)
								+ sizeof(update_map_manager) );

				seg_config->per_cpu_info_list[i]->update_manager->update_map_size =
					update_map_size;
                //PRINT_DEBUG("Here update_map_size:%u\n", seg_config->per_cpu_info_list[i]->update_manager->update_map_size);
	
				//populate sched_manager, refer to types.hpp, new struct for fog_engine_target-by hejian
                //also initilization the bitmap buffer
                //bitmap address
                     
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bitmap_buf_head = 
					(char*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len);
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bitmap_buf_head = 
					(char*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len + per_bitmap_buf_size);

			/*	seg_config->per_cpu_info_list[i]->aux_manager = 
					(aux_update_buf_manager<VA>*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_bitmap_manager)
								+ sizeof(update_map_manager) );*/

				//build the strips
				seg_config->per_cpu_info_list[i]->strip_buf_head = //the first strip
					(char*)(ROUND_UP( 
						(u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len + per_bitmap_buf_size * 2,
						sizeof(update<VA>) ));

				strip_buf_size = seg_config->per_cpu_info_list[i]->buf_size - total_header_len - per_bitmap_buf_size * 2;

				//divide the update buffer to "strip"s
				//round down strip size
				strip_size = ROUND_DOWN( strip_buf_size / seg_config->num_segments, 
					(sizeof(update<VA>)*gen_config.num_processors) );
				strip_cap = (u32_t)(strip_size / sizeof(update<VA>));

				seg_config->per_cpu_info_list[i]->strip_buf_len = strip_size;
				seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;

				
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bitmap_buf_size = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bitmap_buf_size = per_bitmap_buf_size;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bits_true_size = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bits_true_size = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_min_vert_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_min_vert_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_max_vert_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_max_vert_id = 0;

                //u32_t tmp_min_offset = gen_config.min_vert_id%gen_config.num_processors;
                //PRINT_DEBUG("tmp_min_offset = %d\n", tmp_min_offset);
                u32_t tmp_max_offset = gen_config.max_vert_id%gen_config.num_processors;
                PRINT_DEBUG("tmp_max_offset = %d\n", tmp_max_offset);
                //u32_t tmp_min_value;
                u32_t tmp_max_value;

                //if (i == tmp_min_offset)
                //    tmp_min_value = gen_config.min_vert_id;
                //else if (i < tmp_min_offset)
                //    tmp_min_value = gen_config.min_vert_id + gen_config.num_processors - tmp_min_offset + i;
                //else //(i > tmp_min_offset)
                //    tmp_min_value = gen_config.min_vert_id - tmp_min_offset + i;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_max_vert_id =
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_max_vert_id = i;

                if (i == tmp_max_offset)
                    tmp_max_value = gen_config.max_vert_id;
                else if(i < tmp_max_offset)
                    tmp_max_value = gen_config.max_vert_id - tmp_max_offset + i;
                else //(i > tmp_max_offset)
                    tmp_max_value = gen_config.max_vert_id - gen_config.num_processors - tmp_max_offset + i;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_min_vert_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_min_vert_id = tmp_max_value;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_num_edges = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_num_edges = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->signal_to_scatter = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->signal_to_scatter = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->p_bitmap_steal = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->p_bitmap_steal = NULL;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_virt_cpu_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_virt_cpu_id = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_num_virt_cpus = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_num_virt_cpus = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_bits_true_size = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_bits_true_size = 0;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->steal_context_edge_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->steal_context_edge_id = 0;

                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->partition_gather_signal = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->partition_gather_signal = 0;
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->partition_gather_strip_id = 
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->partition_gather_strip_id = -1;


                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->p_bitmap = new bitmap(
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data0->per_bitmap_buf_head, 
                        per_bitmap_buf_size,
                        per_bitmap_buf_size*8,
                        i,
                        tmp_max_value,
                        i,
                        gen_config.num_processors);
                seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->p_bitmap = new bitmap(
                        seg_config->per_cpu_info_list[i]->sched_manager->p_context_data1->per_bitmap_buf_head, 
                        per_bitmap_buf_size,
                        per_bitmap_buf_size*8,
                        i,
                        tmp_max_value,
                        i,
                        gen_config.num_processors);


				//populate the auxiliary update buffer manager
				/*seg_config->per_cpu_info_list[i]->aux_manager->buf_head =
					seg_config->aux_update_buf + i* aux_update_buf_len;
						
				seg_config->per_cpu_info_list[i]->aux_manager->buf_size =
					aux_update_buf_len;
					//should round down&up to make the buffer fitable for updates
				seg_config->per_cpu_info_list[i]->aux_manager->update_head = 
					(update<VA>*)ROUND_UP( 
								(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
								sizeof( update<VA> ) );
				seg_config->per_cpu_info_list[i]->aux_manager->buf_cap =
					(u32_t)(aux_update_buf_len/sizeof( update<VA> ));
				seg_config->per_cpu_info_list[i]->aux_manager->num_updates = 0;*/
                //wait the write process
			}
			show_sched_update_buf();
		}


		static void *map_anon_memory( u64_t size,
                             bool mlocked,
                             bool zero = false)
		{
			void *space = mmap(NULL, size > 0 ? size:4096,
                     PROT_READ|PROT_WRITE,
                     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
			PRINT_DEBUG( "Engine::map_anon_memory had allocated 0x%llx bytes at %llx\n", size, (u64_t)space);

			if(space == MAP_FAILED) {
			    std::cerr << "mmap_anon_mem -- allocation " << "Error!\n";
			    exit(-1);
			}
		  	if(mlocked) {
				if(mlock(space, size) < 0) {
			        std::cerr << "mmap_anon_mem -- mlock " << "Error!\n";
			    }
		  	}
			if(zero) {
		    	memset(space, 0, size);
		  	}
			return space;
        }
	
};
template <typename A, typename VA>
index_vert_array * fog_engine_target<A, VA>::vert_index;

template <typename A, typename VA>
u32_t fog_engine_target<A, VA>::fog_engine_target_state;

template <typename A, typename VA>
u32_t fog_engine_target<A, VA>::current_attr_segment;

template <typename A, typename VA>
segment_config<VA, sched_bitmap_manager> * fog_engine_target<A, VA>::seg_config;

template <typename A, typename VA>
io_queue_target* fog_engine_target<A, VA>::fog_io_queue;

#endif
