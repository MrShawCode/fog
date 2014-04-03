//fog_engine_target is defined for targeted queries, such as SSSP.
// characters of fog_engine_target:
// 1) Schedule list with dynamic size
// 2) need to consider merging and (possibly) the scheduled tasks
// 3) As the schedule list may grow dramatically, the system may need to consider dump 
//	(partial) of the list to disk (to alieviate pressure on buffer).

#ifndef __FOG_ENGINE_TARGET_H__
#define __FOG_ENGINE_TARGET_H__

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

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fog_engine_target{
		static index_vert_array* vert_index;

		static segment_config<VA> *seg_config;
        char * buf_for_write;

        static u32_t fog_engine_target_state;
        static u32_t current_attr_segment;

        //io work queue
        io_queue_target* fog_io_queue;

        cpu_thread_target<A,VA> ** pcpu_threads;
        boost::thread ** boost_pcpu_threads;

        int attr_fd;
        u64_t attr_file_length;
        VA *attr_array_header;

        //test bitmap
        static bitmap* p_bitmap;

	public:

		fog_engine_target()
		{
            //create the index array for indexing the out-going edges
            vert_index = new index_vert_array;

            //allocate buffer for writting
            buf_for_write = (char *)map_anon_memory(gen_config.memory_size, true, true );

            //config the buffer for writting
            seg_config = new segment_config<VA>((const char *)buf_for_write);

            //create io queue
            fog_io_queue = new io_queue_target;

            //test bitmap
            u32_t a[] ={2, 3, 45, 64, 23, 53, 46, 7, 8, 11, 99, 0};
            p_bitmap = new bitmap(64); 
            PRINT_DEBUG("Create the bitmap class\n");
            for (int z = 0; z < sizeof(a)/sizeof(u32_t); z++)
            {
                p_bitmap->set_value(z);
            }
            PRINT_DEBUG("Test for bitmap:");
            p_bitmap->print_binary();           


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
            int ret, loop_counter = 0;
            init_phase();
            while(1)
            {
                PRINT_WARNING("SCATTER and UPDATE loop %d\n", loop_counter++);
                ret = scatter_updates();

                if (0 == ret) break;
            }
        }

		void init_phase()
		{
			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work_target<A,VA>* new_cpu_work = NULL;
			io_work_target* init_io_work = NULL;
			char * buf_to_dump = NULL;
			init_param* p_init_param=new init_param;

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
					new_cpu_work = new cpu_work_target<A,VA>( INIT, 
						(void*)p_init_param );
				}else{	//the last segment, should be smaller than a full segment
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap;
					new_cpu_work = new cpu_work_target<A,VA>( INIT, 
						(void*)p_init_param );
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
					init_io_work = new io_work_target( FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)seg_config->segment_cap*sizeof(VA) );
				}else{
					init_io_work = new io_work_target( FILE_WRITE, 
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
        int scatter_updates()
        {
            int ret, unemployed; 
            cpu_work_target<A,VA>* scatter_cpu_work = NULL;
            scatter_param * p_scatter_param = new scatter_param;

            if (remap_attr_file() < 0)
            {
                PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                return -1;
            }
            p_scatter_param->attr_array_head = (void *)attr_array_header;
            pcpu_threads[0]->work_to_do = scatter_cpu_work;
            (*pcpu_threads[0])();
            delete scatter_cpu_work;
            scatter_cpu_work = NULL;

            PRINT_DEBUG("After scatter computation!\n");
            /*ret = 0;
            unemployed = 0;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                PRINT_DEBUG("Processor %d status %d\n", i, pcpu_threads[i]->status);
                if(pcpu_threads[i]->status != FINISHED_SCATTER)
                    ret = 1;
                if (pcpu_threads[i]->status != UPDATE_BUF_FULL)
                    unemployed = 1;
            }

            if (0 == ret )
                return ret;

                */

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


        static bool add_sched_task_to_ring_buf(sched_list_manager * sched_manager, sched_task * task)
        {
            if ((sched_manager->sched_task_counter + 1) > sched_manager->sched_buf_size)
                return false;
            *(sched_manager->tail) = *task;
            //moving rounds
            //remember:sched_mamager->pointers(sched_buf_head, tail, etc)are sched_tasks
            if (sched_manager->tail >= (sched_manager->sched_buf_head + sched_manager->sched_buf_size)) 
                sched_manager->tail = sched_manager->sched_buf_head;
            else
                sched_manager->tail++;
            sched_manager->sched_task_counter++;
            return true;
        }

        static void add_sched_task_to_processor(u32_t processor_id, sched_task *task)
        {
            assert(processor_id < gen_config.num_processors);
            if (!add_sched_task_to_ring_buf(seg_config->per_cpu_info_list[processor_id]->sched_manager, task))
                PRINT_ERROR("add_sched_task_to_processor failed on adding the task to ring buffer!\n");
            delete task;
        }
        static void add_schedule(sched_task * task)
        {
            PRINT_DEBUG("I am here in add_schedule in the first time!\n");
            assert(task->start !=  -1);
            assert(task->term == -1);
            //now just test for the first vertex
            int sched_processor_id = task->start%4;
            PRINT_DEBUG("PROCESSOR_ID = %d\n", sched_processor_id);

            add_sched_task_to_processor(sched_processor_id, task);

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


		void show_sched_list_tasks( sched_list_manager *sched_manager )
		{
			sched_task* head = sched_manager->head;
			sched_task* p_task = head;

			for( u32_t i=0; i<sched_manager->sched_task_counter; i++ ){
				PRINT_DEBUG( "Task %d: Start from :%d to:%d\n", i, p_task->start, p_task->term );
				if( (p_task - sched_manager->sched_buf_head) > sched_manager->sched_buf_size )
					p_task = sched_manager->sched_buf_head;
				else
					p_task++;
			}
		}



		//will browse all sched_lists and list the tasks.
		void show_all_sched_tasks()
		{
			sched_list_manager* sched_manager;

			PRINT_DEBUG( "==========================	Browse all scheduled tasks	==========================\n" );
			//browse all cpus
			for(u32_t i=0; i<gen_config.num_processors; i++){
				sched_manager = seg_config->per_cpu_info_list[i]->sched_manager;
				PRINT_DEBUG( "Processor %d: Number of scheduled tasks: %d, Details:\n", i,
					sched_manager->sched_task_counter );
				show_sched_list_tasks( sched_manager );
			}
			PRINT_DEBUG( "==========================	That's All	==========================\n" );
		}
	


		//Note:
		// Should show this configuration information at each run, especially when
		//	it is not debugging.
		// TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.
		void show_sched_update_buf()
		{
			PRINT_DEBUG( "===============\tsched_update buffer for each CPU begin\t=============\n" );
			PRINT_DEBUG( "CPU\tSched_list_man\tUpdate_map_man\tAux_update_man\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager );

			PRINT_DEBUG( "------------------\tschedule manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tSched_list_head\tSched_list_tail\tSched_list_current\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\n", i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->head,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->tail,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->current);

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


		//Initialize the sched_update buffer for all processors.
		//Embed the data structure in the buffer, 
		//	seg_config->per_cpu_info_list[i] only stores the pointers of the 
		//	managment data structure, which is stored at the beginning of
		//	the sched_update buffer.
		//Layout of the management data structure at the beginning of sched_update buffer:
		//	(ordered by logical address)
		//	---------------------------------
		//	| sched_list_manager			|
		//	---------------------------------
		//	| update_map_manager			|
		//	---------------------------------
		//	| aux_update_buf_manager		|
		//	---------------------------------
		//	| update_map(update_map_size)	|
		//	---------------------------------
		//	| sched_list(sched_list_size)	|
		//	---------------------------------
		//	| update_buffer(strips)			|
		//	---------------------------------
		
		void init_sched_update_buf()
		{
			u32_t update_map_size, sched_list_size, total_header_len;
			u64_t strip_buf_size, strip_size, aux_update_buf_len;
			u32_t strip_cap;

			update_map_size = seg_config->num_segments 
					* gen_config.num_processors 
					* sizeof(u32_t);

			sched_list_size = SCHED_BUFFER_LEN * sizeof(sched_task);
			
			total_header_len = sizeof(sched_list_manager) 
					+ sizeof(update_map_manager)
					+ sizeof(aux_update_buf_manager<VA>)
					+ update_map_size
					+ sched_list_size;

			PRINT_DEBUG( "init_sched_update_buffer--size of sched_list_manager:%lu, size of update map manager:%lu, size of aux_update buffer manager:%lu\n", 
				sizeof(sched_list_manager), sizeof(update_map_manager), sizeof(aux_update_buf_manager<VA>) );
			PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u, sched_list_size:%u, total_head_len:%u\n", 
				update_map_size, sched_list_size, total_header_len );
/*
			//total_header_length should be round up according to the size of updates.
			total_header_len = ROUND_UP( total_header_len, sizeof(update<VA>) );

			//	CPU0 should always exist!
			strip_buf_size = seg_config->per_cpu_info_list[0]->buf_size - total_header_len;

			//divide the update buffer to "strip"s
			strip_size = strip_buf_size / seg_config->num_segments;
			//round down strip size
			strip_size = ROUND_DOWN( strip_size, (sizeof(update<VA>)*gen_config.num_processors) );
			strip_cap = (u32_t)(strip_size / sizeof(update<VA>));

*/
			aux_update_buf_len = seg_config->aux_update_buf_len / gen_config.num_processors;
			//populate the buffer managers
			for(u32_t i=0; i<gen_config.num_processors; i++){
				//headers
				seg_config->per_cpu_info_list[i]->sched_manager = 
					(sched_list_manager*)seg_config->per_cpu_info_list[i]->buf_head;

				seg_config->per_cpu_info_list[i]->update_manager = 
					(update_map_manager*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_list_manager) );

				seg_config->per_cpu_info_list[i]->aux_manager = 
					(aux_update_buf_manager<VA>*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_list_manager)
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
	
				//populate sched_manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_head =
					(sched_task*)(
								(u64_t)seg_config->per_cpu_info_list[i]->aux_manager
								+ sizeof(aux_update_buf_manager<VA>) 
								+ update_map_size );

				seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_size = 
								SCHED_BUFFER_LEN;

				seg_config->per_cpu_info_list[i]->sched_manager->sched_task_counter = 0;
				seg_config->per_cpu_info_list[i]->sched_manager->head = 
				seg_config->per_cpu_info_list[i]->sched_manager->tail = 
				seg_config->per_cpu_info_list[i]->sched_manager->current = 
					seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_head;

				//zero out the update buffer and sched list buffer
				memset( seg_config->per_cpu_info_list[i]->update_manager->update_map_head, 
					0, 
					update_map_size + sched_list_size );
	
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
			}
			//show_sched_update_buf();
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
segment_config<VA> * fog_engine_target<A, VA>::seg_config;

template <typename A, typename VA>
bitmap * fog_engine_target<A,VA>::p_bitmap;

#endif
