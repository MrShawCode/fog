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

#define THRESHOLD 0.3

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

        int attr_fd;
        u64_t attr_file_length;
        VA *attr_array_header;

        static u32_t num_vert_of_next_phase;

	public:

		fog_engine_target()
		{
            //create the index array for indexing the out-going edges
            vert_index = new index_vert_array;

            //
            num_vert_of_next_phase = 0;

            //allocate buffer for writting
            buf_for_write = (char *)map_anon_memory(gen_config.memory_size, true, true );

            //config the buffer for writting
            seg_config = new segment_config<VA, sched_bitmap_manager>((const char *)buf_for_write);

            //create io queue
            fog_io_queue = new io_queue_target;

            //test bitmap
            /*u32_t a[] ={2, 3, 45, 64, 23, 53, 46, 7, 8, 11, 99, 0};
            p_bitmap = new bitmap(64); 
            PRINT_DEBUG("Create the bitmap class\n");
            for (u32_t z = 0; z < sizeof(a)/sizeof(u32_t); z++)
            {
              p_bitmap->set_value(z);
            }
            PRINT_DEBUG("Test for bitmap:");
            p_bitmap->print_binary();*/           


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
            int  loop_counter = 0;
            u32_t PHASE = 0;
            init_phase(PHASE);
            while(1)
            {
                loop_counter++;
                PRINT_WARNING("SCATTER and UPDATE loop %d\n", loop_counter);
                PHASE = (loop_counter%2);
                //PRINT_DEBUG("PHASE = %d\n", PHASE);

                PRINT_DEBUG("Before scatter_updates , there are %d vert to scatter!\n", num_vert_of_next_phase);
                scatter_updates(1-PHASE);
                PRINT_DEBUG("After scatter_updates, there are now %d vert to scatter!\n", num_vert_of_next_phase);

                //PRINT_DEBUG("PHASE = %d\n", PHASE);
                gather_updates(PHASE);
                PRINT_DEBUG("After gather_updates, there are now %d vert to scatter!\n", num_vert_of_next_phase);

                if (num_vert_of_next_phase == 0)
                    break;
            }
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
						(void*)p_init_param, fog_io_queue  );
				}else{	//the last segment, should be smaller than a full segment
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap;
                    p_init_param->PHASE = PHASE;
					new_cpu_work = new cpu_work_target<A,VA>( INIT, 
						(void*)p_init_param, fog_io_queue );
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
						(u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA) );
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
        //-1:failure
        int scatter_updates(u32_t PHASE)
        {
            u32_t ret, unemployed ; 
            cpu_work_target<A,VA>* scatter_cpu_work = NULL;
            scatter_param_target * p_scatter_param = new scatter_param_target;
            callback_scatter_param_target * callback_scatter_param;
            u32_t * p_num_vert_of_next_phase;

			fog_engine_target_state = SCATTER;
            callback_scatter_param = (callback_scatter_param_target *)malloc(sizeof(struct callback_scatter_param_target) 
                        * gen_config.num_processors);
            p_num_vert_of_next_phase = (u32_t *)malloc(sizeof(u32_t) * gen_config.num_processors);
            memset(p_num_vert_of_next_phase, 0, (sizeof(u32_t) * gen_config.num_processors) );
            //initizition the callback param
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                callback_scatter_param_target * tmp_callback_scatter_param = (callback_scatter_param + i) ;
                tmp_callback_scatter_param->vertex_id = 0;
                tmp_callback_scatter_param->edge_id = 0;
                tmp_callback_scatter_param->old_read_buf = NULL;
            }
            /*for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                callback_scatter_param_target * tmp_callback_scatter_param = (callback_scatter_param + i) ;
                PRINT_DEBUG("vertex_id = %d, edge_id = %d\n", tmp_callback_scatter_param->vertex_id, tmp_callback_scatter_param->edge_id);
            }*/
            if (seg_config->num_attr_buf == 1)
            {
                p_scatter_param->attr_array_head = (void*)seg_config->attr_buf0;
                p_scatter_param->PHASE = PHASE;
                p_scatter_param->old_signal = 0;
                p_scatter_param->callback_scatter_param = callback_scatter_param;
                p_scatter_param->p_num_vert_of_next_phase = p_num_vert_of_next_phase;
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
                p_scatter_param->old_signal = 0;
                p_scatter_param->callback_scatter_param = callback_scatter_param;
                p_scatter_param->p_num_vert_of_next_phase = p_num_vert_of_next_phase;
            }

            do{
                scatter_cpu_work = new cpu_work_target<A, VA>(SCATTER, (void *)p_scatter_param, fog_io_queue);
                PRINT_DEBUG("here in scatter_updates, the p_scatter_param->old_signal = %d\n", p_scatter_param->old_signal);
                pcpu_threads[0]->work_to_do = scatter_cpu_work;
                (*pcpu_threads[0])();

                delete scatter_cpu_work;
                scatter_cpu_work = NULL;

                PRINT_DEBUG("After scatter computation!\n");
                ret = 0;
                unemployed = 0;
                for (u32_t i = 0; i < gen_config.num_processors; i++)
                {
                    PRINT_DEBUG("Processor %d status %d\n", i, pcpu_threads[i]->status);
                    if(pcpu_threads[i]->status == FINISHED_SCATTER)
                        ret = 1;
                    if (pcpu_threads[i]->status == UPDATE_BUF_FULL)
                        unemployed = 1;

                    PRINT_DEBUG("Processor %d has scatter %d vert!\n", i, (u32_t)*(p_num_vert_of_next_phase+i));
                    num_vert_of_next_phase -= (u32_t)*(p_num_vert_of_next_phase + i); 
                    if (num_vert_of_next_phase < 0)
                    {
                        PRINT_ERROR("There is something error here!\n");
                    }
                }

                if (unemployed)
                {
                    PRINT_DEBUG("need to gather!\n");
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                    {
                        callback_scatter_param_target * tmp_callback_scatter_param = (callback_scatter_param + i) ;
                        PRINT_DEBUG("vertex_id = %d, edge_id = %d\n", tmp_callback_scatter_param->vertex_id, tmp_callback_scatter_param->edge_id);
                    }
                    gather_updates(1-PHASE);
                    p_scatter_param->old_signal = 1;
                }
            }while(unemployed == 1);
            //return ret;
            free(callback_scatter_param);
            return ret;
        }

        //gather all updates in the update buffer.
        void gather_updates(u32_t PHASE)
        {
            cpu_work_target<A,VA>* gather_cpu_work = NULL;
            io_work_target * read_io_work = NULL;
            io_work_target * write_io_work = NULL;
            char * next_buffer = NULL, *read_buf = NULL;
            //u32_t begin_with;
            u64_t offset, read_size;
            gather_param_target * p_gather_param = new gather_param_target;
            u32_t ret;

			fog_engine_target_state = GATHER;
            //for (u32_t strip_id = 0; strip_id < seg_config->num_segments; strip_id++)
            //{
                if ((ret = cal_threshold()) == 1)
                {
                    for(u32_t i = 0; i < seg_config->num_segments; i++)
                    {
                        p_gather_param->threshold = 1;
                        p_gather_param->strip_id = i;
                        p_gather_param->PHASE = PHASE;
                        PRINT_DEBUG( "i=%d, read_io_work=%llx\n", i, (u64_t)read_io_work );

                        if (i%2) read_buf = (char *)seg_config->attr_buf1;
                        else read_buf = (char *)seg_config->attr_buf0;

                        if (read_io_work == NULL)
                        {
                            offset = (u64_t)i * (u64_t)seg_config->segment_cap * sizeof(VA);
                            if (i == (seg_config->num_segments - 1))
                            {
                                read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA);
                            }
                            else 
                                read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                            read_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                    FILE_READ, read_buf, offset, read_size);
                            fog_io_queue->add_io_task(read_io_work);
                            fog_io_queue->wait_for_io_task(read_io_work);
                            fog_io_queue->del_io_task(read_io_work);
                            PRINT_DEBUG("Finish reading the first segment!\n");
                        }
                        else
                        {
                            fog_io_queue->wait_for_io_task(read_io_work);
                            fog_io_queue->del_io_task(read_io_work);

                            read_buf = next_buffer;
                        }

                        if ((i + 1) < seg_config->num_segments)
                        {
                            //next buffer must different from read_buf
                            if ((i + 1)%2) next_buffer = (char *)seg_config->attr_buf1;
                            else  next_buffer = (char *)seg_config->attr_buf0;

                            offset = (u64_t)(i + 1)*(u64_t)seg_config->segment_cap*sizeof(VA);

                            if ((i + 1) == (seg_config->num_segments - 1))
                            {
                                read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA);
                            }
                            else 
                                read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                            read_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                    FILE_READ, next_buffer, offset, read_size);
                            fog_io_queue->add_io_task(read_io_work);
                            PRINT_DEBUG( "will invoke next read io work, offset %llu, size:%llu\n", offset, read_size );
                        }
                        p_gather_param->attr_array_head = (void *)read_buf;

                        gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param, fog_io_queue);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                        //we need to write the attr_buf to attr_file 
                        if (write_io_work != NULL)
                        {
                            fog_io_queue->wait_for_io_task(write_io_work);
                            fog_io_queue->del_io_task(write_io_work);
                            write_io_work = NULL;
                        }
                        write_io_work = new io_work_target(gen_config.attr_file_name.c_str(),
                                FILE_WRITE, next_buffer, offset, read_size);
                        fog_io_queue->add_io_task(write_io_work);
                    }
                }
                else
                {
                    for(u32_t i = 0; i < seg_config->num_segments; i++)
                    {
                        p_gather_param->threshold = 0;
                        p_gather_param->strip_id = i;
                        p_gather_param->PHASE = PHASE;
                        if (remap_attr_file() < 0)
                        {
                            PRINT_ERROR("FOG_ENGINE::gather_updates failed!\n");
                        }
                        p_gather_param->attr_array_head = (void *)attr_array_header;

                        gather_cpu_work = new cpu_work_target<A, VA>(GATHER, (void *)p_gather_param, fog_io_queue);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                    }

                }

            //}

            PRINT_DEBUG("After gather!!\n");
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
                PRINT_DEBUG("THRESHOLD = %f\n", THRESHOLD );

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

            memblock = (char *)mmap(NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_NORESERVE, attr_fd, 0);
            if (MAP_FAILED == memblock)
            {
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
            munmap((void*)attr_array_header, attr_file_length);
            close(attr_fd);
            return 0;
        }

        //remap
        int remap_attr_file()
        {
            int ret;

            if (attr_fd)
                if ( (ret = unmap_attr_file()) < 0 ) return ret;
            if ((ret = map_attr_file()) < 0 ) return ret;

            return 0;
        }

        void rebalance_sched_tasks()
        {
            PRINT_DEBUG("I am here!\n");
        }

        static void write_one_buf_to_bitmap_file(char * current_write_buf, std::string current_file_name , u32_t write_size)
        {
            io_work_target * write_bitmap_io_work = NULL;
            write_bitmap_io_work = new io_work_target( current_file_name.c_str(),
                FILE_WRITE, 
                current_write_buf,
                0,
                write_size);
            //PRINT_DEBUG("true_size of current_write_buf is %d\n", (u32_t)*current_write_buf);
            
            fog_io_queue->add_io_task( write_bitmap_io_work );
            //PRINT_DEBUG("after add io \n");
            if (write_bitmap_io_work != NULL)
            {
                fog_io_queue->wait_for_io_task( write_bitmap_io_work);
                fog_io_queue->del_io_task( write_bitmap_io_work );
                write_bitmap_io_work = NULL;
            }
        }

        //for exp
        static void read_bitmap_file_to_buf(char * current_read_buf, std::string current_file_name, u32_t read_size )
        {
            io_work_target * read_bitmap_io_work = NULL;
            read_bitmap_io_work = new io_work_target( current_file_name.c_str(),
                FILE_READ, 
                current_read_buf,
                0,
                read_size);
            
            fog_io_queue->add_io_task( read_bitmap_io_work );
            if (read_bitmap_io_work != NULL)
            {
                fog_io_queue->wait_for_io_task( read_bitmap_io_work);
                fog_io_queue->del_io_task( read_bitmap_io_work );
                read_bitmap_io_work = NULL;
            }
        }

        static void add_sched_task_to_bitmap_buffer(u32_t task_vid, u32_t partition_id, 
                u32_t segment_id, u32_t set_id, bitmap * current_write_buf)
        {
            assert(partition_id < gen_config.num_processors);
            PRINT_DEBUG( "VID = %u, the SEGMENT ID=%d, CPU = %d\n", partition_id, segment_id, task_vid );
            //std::string current_write_file_name = get_current_file_name(partition_id, , segment_id); 
        }
        static void add_schedule(u32_t task_vid, u32_t PHASE, u32_t new_signal)
        {
            u32_t segment_id, partition_id;
            segment_id = VID_TO_SEGMENT(task_vid);
            char /** current_read_buf,*/ * current_write_buf;
            bitmap * new_write_bitmap = NULL ;
            partition_id = VID_TO_PARTITION(task_vid);
            //assert(gen_config.min_vert_id <= task_vid);
            assert(task_vid <= gen_config.max_vert_id);
            assert(partition_id < gen_config.num_processors);
            //now just test for the first vertex
            //PRINT_DEBUG( "VID = %u, the SEGMENT ID=%d, CPU = %d\n", task_vid, VID_TO_SEGMENT(task_vid), VID_TO_PARTITION(task_vid) );
            //if (seg_config->per_cpu_info_list[partition_id]->sched_manager->num_of_bufs == 2)
            //{
            //}
            //else
            //{

            //add++
            num_vert_of_next_phase++;
            //PRINT_DEBUG("In add_schedule, the num_vert_of_next_phase = %d\n", num_vert_of_next_phase);

                current_write_buf = seg_config->per_cpu_info_list[partition_id]->sched_manager->sched_bitmap_head
                    + seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size 
                    * (seg_config->per_cpu_info_list[partition_id]->sched_manager->num_of_bufs/2); 
                    //+ seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size * 2;
                std::string current_write_file_name = get_current_file_name(partition_id, PHASE, segment_id);

                if (fog_engine_target_state == INIT)
                {
                    memset(current_write_buf, 0, 
                            seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size);
                    bitmap * new_write_bitmap = new bitmap(
                            seg_config->partition_cap,
                            seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size,
                            START_VID(VID_TO_SEGMENT(task_vid), partition_id), TERM_VID(VID_TO_SEGMENT(task_vid), partition_id),
                            0,
                            (bitmap_t)current_write_buf);
                    new_write_bitmap->set_value(VID_TO_BITMAP_INDEX(task_vid, seg_config->partition_cap));
                    //new_write_bitmap->print_binary(0,100);
                    //PRINT_DEBUG("Will dump the buf to file\n");
                    write_one_buf_to_bitmap_file(current_write_buf, current_write_file_name,
                           seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size);
                }

                else if (fog_engine_target_state == GATHER)
                {
                    //PRINT_DEBUG("new_signal = %d\n", new_signal);
                    if (new_signal == 0)
                    {
                        memset(current_write_buf, 0, 
                            seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size);
                        new_write_bitmap = new bitmap(
                                seg_config->partition_cap,
                                seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size,
                                START_VID(VID_TO_SEGMENT(task_vid), partition_id), TERM_VID(VID_TO_SEGMENT(task_vid), partition_id),
                                0,
                                (bitmap_t)current_write_buf);
                        new_write_bitmap->set_value(VID_TO_BITMAP_INDEX(task_vid, seg_config->partition_cap));
                        //PRINT_DEBUG("task_vid = %d\n", task_vid);
                    }
                    else if(new_signal == 1)
                    {
                        if (new_write_bitmap != NULL)
                            new_write_bitmap->set_value(VID_TO_BITMAP_INDEX(task_vid, seg_config->partition_cap));
                        //PRINT_DEBUG("task_vid = %d\n", task_vid);
                    }
                    else if (new_signal == 2)
                    {
                        if (new_write_bitmap != NULL)
                            new_write_bitmap->set_value(VID_TO_BITMAP_INDEX(task_vid, seg_config->partition_cap));
                        //PRINT_DEBUG("task_vid = %d\n", task_vid);
                        write_one_buf_to_bitmap_file(current_write_buf, current_write_file_name,
                               seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size);
                    }
                }
                //for exp
                /*std::string current_read_file_name = get_current_file_name(partition_id, PHASE, segment_id);
                PRINT_DEBUG("current_file_name = %s\n", current_read_file_name.c_str());
                current_read_buf = seg_config->per_cpu_info_list[partition_id]->sched_manager->sched_bitmap_head;
                bitmap * new_read_bitmap = new bitmap(seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size,
                        seg_config->partition_cap,
                        (bitmap_t)current_read_buf);
                if (fog_engine_target_state == INIT)
                {
                    read_bitmap_file_to_buf(current_read_buf, current_read_file_name, 
                            seg_config->per_cpu_info_list[partition_id]->sched_manager->bitmap_file_size);

                    new_read_bitmap->print_binary(0,100);
                }*/

            //}


        }
		void reclaim_everything()
		{
			PRINT_DEBUG( "begin to reclaim everything\n" );
			//reclaim pre-allocated space
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
			PRINT_DEBUG( "CPU\tSched_bitmap_man\tUpdate_map_man\tAux_update_man\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager );

			PRINT_DEBUG( "------------------\tschedule manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tSched_bitmap_read0\tSched_bitmap_write0\tSched_bitmap_read1\tSched_bitmap_write1\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t0x%llx\n", i,
                        (u64_t)seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_head,
                        (u64_t)(seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_head 
                        + seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_size/2),
                        (u64_t)(seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_head  
                        + seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_size/4),
                        (u64_t)(seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_head 
                        + seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_size*3/4)
                        );

			PRINT_DEBUG( "------------------\tupdate manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tUpdate_map_address\tUpdate_map_size\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_head,
					(u32_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_size );

			PRINT_DEBUG( "------------------\tauxiliary update buffer manager\t------------\n" );
			PRINT_DEBUG( "CPU\tBuffer_begin\tBuffer_size\tUpdate_head\tBuf_cap\tNum_updates\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_size,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->update_head,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_cap,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->num_updates );

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

        //get file name for current segment
        static std::string get_current_file_name(u32_t processor_id, u32_t set_id, u32_t strip_id)
        {
            std::stringstream str_strip, str_procs, str_set;
            str_strip << strip_id;
            str_set << set_id;
            str_procs << processor_id;
            std::string current_file_name = "CPU"+str_procs.str()+"_"+str_set.str()+"_sched_strip"+str_strip.str();
            return current_file_name;
        }


        //LATOUT of the management data structure of fog_engine_target at the beginning of sched_update buffer;
        //designed by hejian
		//	(ordered by logical address)
		//	----------------------
		//	| sched_bitmap_manager          |
        //	---------------------------------
		//	| update_map_manager	        |
		//	----------------------------
		//	| aux_update_buf_manager	    |
		//	--------------------------------
		//	| update_map(update_map_size)   |
		//	------------------------------
		//	| sched_bitmap_buffer(sched_bitmap_size)	|
		//	------------------------------
		//	| update_buffer(strips)		    |
		//	-------------------------------
		

		void init_sched_update_buf()
		{
			u32_t update_map_size, sched_bitmap_size, total_header_len;
			u64_t strip_buf_size, strip_size, aux_update_buf_len;
			u32_t strip_cap;
			u32_t num_bufs;
            u32_t bitmap_file_size; //bitmap_max_size;
            //io_work_target* init_bitmap_io_work = NULL;
            int init_fd;


            if (seg_config->num_segments == 1) // small graph, two buffers are enough. One for READ and the other for WRITE
                num_bufs = 2; 
            else // big graph, four buffers. Just like the dual buffer of the attribute file 
                num_bufs = 4;
			update_map_size = seg_config->num_segments 
					* gen_config.num_processors 
					* sizeof(u32_t);

            bitmap_file_size = (u32_t)((ROUND_UP(seg_config->partition_cap, 8))/8 + 3*sizeof(u32_t));
            //bitmap_max_size = seg_config->partition_cap;
            PRINT_DEBUG("the seg_config->partition_cap is %d, the origin bitmap_file_size is %lf\n", seg_config->partition_cap, (double)(seg_config->partition_cap)/8);
            PRINT_DEBUG("the bitmap_file_size is %d\n", bitmap_file_size);
			sched_bitmap_size = num_bufs * (u32_t)((ROUND_UP(seg_config->partition_cap, 8))/8 + 3*sizeof(u32_t));//bytes
            PRINT_DEBUG("the sched_bitmap_size is %d\n", sched_bitmap_size);

			total_header_len = sizeof(sched_bitmap_manager) 
					+ sizeof(update_map_manager)
					+ sizeof(aux_update_buf_manager<VA>)
					+ update_map_size
					+ sched_bitmap_size;

			PRINT_DEBUG( "init_sched_update_buffer of fog_engine_target--size of sched_bitmap_manager:%lu,  size of update map manager:%lu, size of aux_update buffer manager:%lu\n", 
				sizeof(sched_bitmap_manager), sizeof(update_map_manager), sizeof(aux_update_buf_manager<VA>) );
			PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u, sched_bitmap_size:%u, total_head_len:%u\n", 
				update_map_size, sched_bitmap_size, total_header_len );

			//total_header_length should be round up according to the size of updates.
			total_header_len = ROUND_UP( total_header_len, sizeof(update<VA>) );

			//	CPU0 should always exist!
			strip_buf_size = seg_config->per_cpu_info_list[0]->buf_size - total_header_len;

			//divide the update buffer to "strip"s
			strip_size = strip_buf_size / seg_config->num_segments;
			//round down strip size
			strip_size = ROUND_DOWN( strip_size, (sizeof(update<VA>)*gen_config.num_processors) );
			strip_cap = (u32_t)(strip_size / sizeof(update<VA>));


			aux_update_buf_len = seg_config->aux_update_buf_len / gen_config.num_processors;
			//populate the buffer managers
			for(u32_t i=0; i<gen_config.num_processors; i++){
				//headers
				seg_config->per_cpu_info_list[i]->sched_manager = 
					(sched_bitmap_manager*)seg_config->per_cpu_info_list[i]->buf_head;

				seg_config->per_cpu_info_list[i]->update_manager = 
					(update_map_manager*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_bitmap_manager) );

				seg_config->per_cpu_info_list[i]->aux_manager = 
					(aux_update_buf_manager<VA>*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_bitmap_manager)
								+ sizeof(update_map_manager) );

				//build the strips
				seg_config->per_cpu_info_list[i]->strip_buf_head = //the first strip
					(char*)(ROUND_UP( 
						(u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len,
						sizeof(update<VA>) ));

				strip_buf_size = seg_config->per_cpu_info_list[i]->buf_size - total_header_len;

				//divide the update buffer to "strip"s
				//round down strip size
				strip_size = ROUND_DOWN( strip_buf_size / seg_config->num_segments, 
					(sizeof(update<VA>)*gen_config.num_processors) );
				strip_cap = (u32_t)(strip_size / sizeof(update<VA>));

				seg_config->per_cpu_info_list[i]->strip_buf_len = strip_size;
				seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;

				//populate the update map manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->update_manager->update_map_head = 
					(u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->aux_manager 
						+ sizeof(aux_update_buf_manager<VA>));

				seg_config->per_cpu_info_list[i]->update_manager->update_map_size =
					update_map_size;
                //PRINT_DEBUG("Here update_map_size:%u\n", seg_config->per_cpu_info_list[i]->update_manager->update_map_size);
	
				//populate sched_manager, refer to types.hpp, new struct for fog_engine_target-by hejian
                //also initilization the bitmap buffer
                char * bitmap_head_addr =(char *)( (u64_t)seg_config->per_cpu_info_list[i]->aux_manager
                    +sizeof(aux_update_buf_manager<VA>)+ update_map_size);
                     
                seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_head = bitmap_head_addr;
                            

                seg_config->per_cpu_info_list[i]->sched_manager->num_of_bufs = 2; 
                seg_config->per_cpu_info_list[i]->sched_manager->bitmap_file_size = bitmap_file_size; 
                seg_config->per_cpu_info_list[i]->sched_manager->sched_bitmap_size = sched_bitmap_size; 

                if (num_bufs == 4)
                {
                    seg_config->per_cpu_info_list[i]->sched_manager->num_of_bufs = 4; 
                }

				seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_size = sched_bitmap_size/num_bufs;

                //create new file for bitmap
                for (u32_t set_num = 0; set_num < 2; set_num++ )
                    for(u32_t strip_num = 0; strip_num < seg_config->num_segments; strip_num++) 
                    {
                        std::string current_file_name = get_current_file_name(i, set_num, strip_num); 
                        //PRINT_DEBUG("current_file_name = %s\n", current_file_name.c_str());

                        init_fd = open( current_file_name.c_str(), O_CREAT  , S_IRUSR | S_IRGRP | S_IROTH );
                        if (init_fd < 0)
                            PRINT_ERROR("Generate file %s failed\n", current_file_name.c_str());
                        close(init_fd);
                        //WRITE SOMETHING to the file!--for initilization  
                    }


                //There is a BIG bug for me
				//zero out the update buffer and sched list buffer
				//memset( seg_config->per_cpu_info_list[i]->update_manager->update_map_head, 
				//	0,
				//	update_map_size + sched_bitmap_size + 1 );
	
				//populate the auxiliary update buffer manager
				seg_config->per_cpu_info_list[i]->aux_manager->buf_head =
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
				seg_config->per_cpu_info_list[i]->aux_manager->num_updates = 0;
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

template <typename A, typename VA>
u32_t fog_engine_target<A, VA>::num_vert_of_next_phase;

#endif
