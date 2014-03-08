//declaration of fog_engine class
// The features of this class:
// 1) Schedule list with fixed size (defined by a MACRO)
// 2) donot need to sort and merge the scheduled tasks, just FIFO.

#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <cassert>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"

#define SCHED_BUFFER_LEN	1024

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute structure
template <typename A, typename VA>
class fog_engine{
		//global variables
		static segment_config<VA> *seg_config;
		static index_vert_array* vert_index;
		char* buffer_for_write;

		static u32_t fog_engine_state;
		static u32_t current_attr_segment;

		cpu_thread<A,VA> ** pcpu_threads;
		boost::thread ** boost_pcpu_threads;
		
		VA *vert_attr_head0, *vert_attr_head1;
		disk_thread * attr_disk_thread;
		boost::thread* boost_attr_disk_thread;

	public:

		fog_engine()
		{
			//create the index array for indexing the out-going edges
			vert_index = new index_vert_array;
			verify_vertex_indexing();

			//allocate buffer for writting
			buffer_for_write = (char*)map_anon_memory( gen_config.memory_size, true, true );

			//config the buffer for writting
			seg_config = new segment_config<VA>( (const char*)buffer_for_write );

			//create disk thread
			attr_disk_thread = new disk_thread( DISK_THREAD_ID_BEGIN_WITH );
			boost_attr_disk_thread = new boost::thread( boost::ref(*attr_disk_thread) );

			//assume buffer_for_write is aligned, although it is possible that it is NOT! TODO

			//create cpu threads
			pcpu_threads = new cpu_thread<A,VA> *[gen_config.num_processors];
			boost_pcpu_threads = new boost::thread *[gen_config.num_processors];
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				pcpu_threads[i] = new cpu_thread<A,VA>(i, vert_index, buffer_for_write);
				if( i>0 )	//Do not forget, "this->" is thread 0
					boost_pcpu_threads[i] = new boost::thread( boost::ref(*pcpu_threads[i]) );
			}

			//setup the dual vertex attribute buffer:
			vert_attr_head0 = (VA*)((u64_t)buffer_for_write + gen_config.memory_size/2);
			vert_attr_head1 = (VA*)((u64_t)vert_attr_head0 + gen_config.memory_size/4);

			init_sched_update_buffer();
		}
			
		~fog_engine(){
			reclaim_everything();
		}

		void operator() ()
		{
			test_add_sched_tasks();

			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work<A,VA>* new_cpu_work = NULL;
			io_work* new_io_work = NULL;
			char * buffer_to_dump = NULL;

			printf( "fog engine operator is called, conduct init phase for %d times.\n", seg_config->num_segments );
			current_attr_segment = 0;
			fog_engine_state = INIT;
			for( u32_t i=0; i < seg_config->num_segments; i++ ){
				//which attribute buffer I will write to?
				if ( current_attr_segment%2== 0 ) buffer_to_dump = (char*)vert_attr_head0;
				else buffer_to_dump = (char*)vert_attr_head1;
		
				//create cpu threads
				if( i != (seg_config->num_segments-1) ){
					new_cpu_work = new cpu_work<A,VA>( INIT, 
						buffer_to_dump, 
						seg_config->segment_cap*i,
						seg_config->segment_cap, 
						seg_config );
				}else{	//the last segment, should be smaller than a full segment
					new_cpu_work = new cpu_work<A,VA>( INIT, 
						buffer_to_dump, 
						seg_config->segment_cap*i,
						gen_config.max_vert_id%seg_config->segment_cap,
						seg_config );
				}
				pcpu_threads[0]->work_to_do = new_cpu_work;
				(*pcpu_threads[0])();
				//cpu threads finished init current attr buffer
				delete new_cpu_work;
				new_cpu_work = NULL;
				
				//dump current attr buffer to disk
				if ( new_io_work != NULL ){
					//keep waiting till previous disk work is finished
					while( 1 ){
						if( new_io_work->finished ) break;
						//should measure the time spend on waiting! TODO
					};
					delete new_io_work;
					new_io_work = NULL;
				}

				if( i != (seg_config->num_segments-1) ){
					new_io_work = new io_work( FILE_WRITE, 
						buffer_to_dump, 
						seg_config->segment_cap*sizeof(VA) );
				}else{
					new_io_work = new io_work( FILE_WRITE, 
						buffer_to_dump, 
						(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA) );
				}

				//activate the disk thread
				attr_disk_thread->io_work_to_do = new_io_work;
				attr_disk_thread->disk_task_sem.post();

				current_attr_segment++;
			}
	
			//wait till the last write work is finished.
			while( 1 ){
				if( new_io_work->finished ) break;
				//should measure the time spend on waiting! TODO
			};
			delete new_io_work;

			printf( "fog engine finished initializing attribute files\n" );
		}

		void reclaim_everything()
		{
			printf( "begin to reclaim everything\n" );
			//reclaim pre-allocated space
			munlock( buffer_for_write, gen_config.memory_size );
			munmap( buffer_for_write, gen_config.memory_size );
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
			attr_disk_thread->terminate = true;
			attr_disk_thread->disk_task_sem.post();
			boost_attr_disk_thread->join();
			delete attr_disk_thread;

			//destroy the vertices mapping
			delete vert_index;

			delete seg_config;
			printf( "everything reclaimed!\n" );
		}

		//==================================	follow functions handle scheduling buffer	===========================
		void show_sched_update_buffer()
		{
			printf( "===============\tShow sched and update buffer infor for each CPU\t=============\n" );
			printf( "CPU\tSched_list_man\tUpdate_map_man\tUpdate_map\tSched_list\tUpdate_buf\tStripLen\tStripCap\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				printf( "%d\t0x%llx\t0x%llx\t0x%llx\t0x%llx\t0x%llx\t0x%llx\t0x%llx\n", i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_map,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_list_head,
					(u64_t)seg_config->per_cpu_info_list[i]->update_buffer_head,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_len,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_cap
				);
			}
			printf( "CPU\tSched_list_head\tSched_list_tail\tSched_list_current\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				printf( "%d\t0x%llx\t0x%llx\t0x%llx\n", i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->head,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->tail,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->current);
			}
			printf( "===============\tBuffer show finished!\t=============\n" );
		}

		//Initialize the schedule and update buffer for all processors.
		//Embed the data structure in buffer_to_write, 
		//	seg_config->per_cpu_info_list[i] only stores the pointers
		//	save the computed results in seg_config->per_cpu_info_list[i]
		//Layout of the per CPU buffer:
		//| sched_list_manager | update_map_manager | update_map(update_map_size) | -->
		//		--> sched_list(sched_list_size) | update_buffer(strips) |
		void init_sched_update_buffer()
		{
			u32_t update_map_size, sched_list_size, total_header_len;
			u64_t update_buffer_size, strip_size, strip_cap;

			update_map_size = seg_config->num_segments * gen_config.num_processors * sizeof(u32_t);
			sched_list_size = SCHED_BUFFER_LEN * sizeof(sched_task);
			
			total_header_len = sizeof(sched_list_manager) + sizeof(update_map_manager) + update_map_size + sched_list_size;
			printf( "init_sched_update_buffer--size of sched_list_manager:%lu, size of update map manager:%lu\n", 
				sizeof(sched_list_manager), sizeof(update_map_manager) );
			printf( "init_sched_update_buffer--update_map_size:%u, sched_list_size:%u, total_head_len:%u\n", 
				update_map_size, sched_list_size, total_header_len );

			//total_header_length should be round up according to the size of updates.
			//	CPU0 should always exist!
			total_header_len = (total_header_len+sizeof(update<VA>)) & ~(sizeof(update<VA>)-1);
			update_buffer_size = seg_config->per_cpu_info_list[0]->buffer_size - total_header_len;

			printf( "after round up, total_header_len = %d, update_buffer_size:0x%llx\n", total_header_len, update_buffer_size );

			//divide the update buffer to "strip"s
			strip_size = update_buffer_size / seg_config->num_segments;
			//round down strip size
			if( strip_size % sizeof(update<VA>) )
				strip_size -= strip_size%sizeof(update<VA>);
			strip_cap = strip_size / sizeof(update<VA>);

			total_header_len = seg_config->per_cpu_info_list[0]->buffer_size - seg_config->num_segments*strip_size;
			printf( "Strip size:0x%llx, Strip cap:%llu, total_header_len:%u\n", strip_size, strip_cap, total_header_len );
			
			for(u32_t i=0; i<gen_config.num_processors; i++){
				seg_config->per_cpu_info_list[i]->sched_manager = 
					(sched_list_manager*)seg_config->per_cpu_info_list[i]->buffer_head;
				seg_config->per_cpu_info_list[i]->update_manager = 
					(update_map_manager*)((u64_t)seg_config->per_cpu_info_list[i]->buffer_head + sizeof(sched_list_manager));
				seg_config->per_cpu_info_list[i]->update_map = 
					(u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->update_manager + sizeof(update_map_manager));
				seg_config->per_cpu_info_list[i]->sched_list_head = 
					(sched_task*)((u64_t)seg_config->per_cpu_info_list[i]->update_map + update_map_size);

				//regarding the strips
				seg_config->per_cpu_info_list[i]->update_buffer_head = //the first strip
					(char*)((u64_t)seg_config->per_cpu_info_list[i]->buffer_head + total_header_len);
				seg_config->per_cpu_info_list[i]->strip_len = strip_size;
				seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;

				//populate sched_manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->sched_manager->sched_buffer_size = SCHED_BUFFER_LEN;
				seg_config->per_cpu_info_list[i]->sched_manager->sched_task_counter = 0;
				seg_config->per_cpu_info_list[i]->sched_manager->head = 
				seg_config->per_cpu_info_list[i]->sched_manager->tail = 
				seg_config->per_cpu_info_list[i]->sched_manager->current = 
					(sched_task*)seg_config->per_cpu_info_list[i]->sched_list_head;

				seg_config->per_cpu_info_list[i]->update_manager->max_margin_value = 0;
			}
//			show_sched_update_buffer();
		}

		//will browse all sched_lists and list the tasks.
		void show_all_sched_tasks()
		{

		}
	
		void test_add_sched_tasks( )
		{
			sched_task t_task;
			u32_t temp_vid;
			t_task.start = 0;
			t_task.term = gen_config.max_vert_id;
			printf( "test_add_schedule: will add a new task, starts %u term %u. cpu number:%u\n", 
				t_task.start, t_task.term, gen_config.num_processors );

			temp_vid = 1073741824;
			printf( "VID = %u, the SEGMENT ID=%d, CPU = %d\n", temp_vid, VID_TO_SEGMENT(temp_vid), VID_TO_PARTITION(temp_vid) );
					
			//KNOWN PROBLEM: assert does NOT work!
			//test if "assert" works in add_sched_task_to_processor
			//add_sched_task_to_processor( 6, &t_task );

			add_schedule( &t_task );

			//will show all scheduled tasks of all processors
			//show_all_sched_tasks();
		}

		//KNOWN PROBLEM: assert does NOT work!
		void add_sched_task_to_processor( u32_t processor_id, sched_task *task )
		{
			//assert *task point to a valid task
			assert(processor_id < gen_config.num_processors );

			if ( task->term == 0 )
				assert( VID_TO_PARTITION( task->start ) == processor_id );
			else{
				assert( task->start < task->term );
				assert( VID_TO_SEGMENT( task->start ) == VID_TO_SEGMENT( task->term ));
				assert( VID_TO_PARTITION( task->start ) == VID_TO_PARTITION( task->term ));
			}

			//now add the task
			printf( "add_sched_task_to_processor: will add sched task from:%u to:%u at CPU:%u\n", task->start, task->term, processor_id );

			sched_list_manager* this_manager;
			this_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;

			printf( "this sched_list manager:head:0x%llx,tail:0x%llx,current:0x%llx\n",
				(u64_t)this_manager->head, (u64_t)this_manager->tail, (u64_t)this_manager->current );
			printf( "number of schedule list counter:%u, sched_buffer_size:%u\n",
				this_manager->sched_task_counter, this_manager->sched_buffer_size );

			delete task;
		}

		//The "task" may be very huge, e.g., [0, max_vert_id],
		// should fragment it before adding it to the sched_list of processors
		void add_schedule( sched_task * task )
		{
			printf( "add_schedule:Will add schedule from %d to %d.\n", task->start, task->term );
			
			if( task->term == 0 ){
				assert( task->start <= seg_config.max_vert_id );
				add_sched_task_to_processor( VID_TO_PARTITION(task->start), task );
				return;
			}

			assert( task->start <= task->term );
			assert( task->term <= seg_config.max_vert_id );
			u32_t i, j;
			sched_task* p_task;

//			printf( "segment start:%u, term:%u, cpu start:%u, term:%u\n", VID_TO_SEGMENT(task->start), VID_TO_SEGMENT(task->term), VID_TO_PARTITION(task->start), VID_TO_PARTITION(task->term) );
			for( i=VID_TO_SEGMENT(task->start); i<=VID_TO_SEGMENT(task->term); i++ ){	
				//loop for segment times
				//handle the task from task->start to the end of segment i (i.e., seg_config->segment_cap*i).
				for( j=0; j<gen_config.num_processors; j++ ){ //loop for number of processors
					//just find common parts between [task->start, task->term] and
					//[START_VID(i,j), TERM_VID(i,j)]
					if( TERM_VID(i,j) < task->start ) continue;
					if( START_VID(i,j) > task->term ) continue;

					//there will be common part(s)
					p_task = new sched_task;
					p_task->start = (task->start>START_VID(i,j))?task->start:START_VID(i,j);
					p_task->term = (task->term>TERM_VID(i,j))?TERM_VID(i,j):task->term;
					add_sched_task_to_processor( j, p_task );
				}
			}
//			delete task;
			return;
		}

		//==================================	follow functions handle update buffer	===========================

		static void add_update( update<VA> * u )
		{
			printf( "will add update to vertex:%d\n", u->dest_vert );
			delete u;
		}

		//==================================	auxiliary functions below	===========================
		//this is a self-proving member function, just make sure vert_index is correctly set.
		void verify_vertex_indexing()
		{
			//TODO:
		}

		static void *map_anon_memory( u64_t size,
                             bool mlocked,
                             bool zero = false)
		{
			void *space = mmap(NULL, size > 0 ? size:4096,
                     PROT_READ|PROT_WRITE,
                     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
			printf( "Engine::map_anon_memory had allocated 0x%llx bytes at %llx\n", size, (u64_t)space);

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
index_vert_array* fog_engine<A,VA>::vert_index;

template <typename A, typename VA>
u32_t fog_engine<A,VA>::fog_engine_state;

template <typename A, typename VA>
u32_t fog_engine<A,VA>::current_attr_segment;

template <typename A, typename VA>
segment_config<VA> * fog_engine<A,VA>::seg_config;

#endif
