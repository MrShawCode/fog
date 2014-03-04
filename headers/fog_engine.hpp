//declaration of fog_engine class

#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fog_engine{
		index_vert_array* vert_index;

		segment_config<VA> *seg_config;

		//will shared among cpu_threads
		u32_t fog_engine_state;
		u32_t current_segment;
		char* buffer_for_write;

		cpu_thread<A,VA> ** pcpu_threads;
		boost::thread ** boost_pcpu_threads;
		
		VA *vert_attr_head0, *vert_attr_head1;
		disk_thread * attr_disk_thread;
		boost::thread* boost_attr_disk_thread;

	public:

		fog_engine(segment_config<VA> *seg_config_in)
			:seg_config( seg_config_in)
			{
			//fog_engine is created with following segment configurations:
			printf( "Fog_engine is created with following segment configurations:\n" );
            printf( "Sizeof vertex attribute:%lu\n", sizeof(VA) );
			printf( "Number of segments=%u\nSegment capacity:%u(vertices)\n", 
				seg_config->num_segments, seg_config->segment_cap );
			printf( "Partition capacity:%u\n", seg_config->partition_cap );

			//create the index array for indexing the out-going edges
			vert_index = new index_vert_array;
			verify_vertex_indexing();

			//allocate buffer for writing
			buffer_for_write = (char*)map_anon_memory( gen_config.memory_size, true, true );

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

			printf( "fog_engine: setup vertex attribute buffer at 0x%llx and 0x%llx.\n", 
				(u64_t)vert_attr_head0, (u64_t)vert_attr_head1 );
		}
			
		~fog_engine(){
		}

		void operator() ()
		{
			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work<A,VA>* new_cpu_work = NULL;
			io_work* new_io_work = NULL;
			char * buffer_to_dump = NULL;

			printf( "fog engine operator is called, conduct init phase for %d times.\n", seg_config->num_segments );
			current_segment = 0;
			fog_engine_state = INIT;
			for( u32_t i=0; i < seg_config->num_segments; i++ ){
				//which attribute buffer I will write to?
				if ( current_segment%2== 0 ) buffer_to_dump = (char*)vert_attr_head0;
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
						gen_config.max_vertex_id%seg_config->segment_cap,
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
						(gen_config.max_vertex_id%seg_config->segment_cap)*sizeof(VA) );
				}

				//activate the disk thread
				attr_disk_thread->io_work_to_do = new_io_work;
				attr_disk_thread->disk_task_sem.post();

				current_segment++;
			}
	
			//wait till the last write work is finished.
			while( 1 ){
				if( new_io_work->finished ) break;
				//should measure the time spend on waiting! TODO
			};
			delete new_io_work;

			printf( "fog engine finished initializing attribute files\n" );

			//reclaim everything
			reclaim_everything();
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
			printf( "everything reclaimed!\n" );
		}

		//not finished yet.
		void add_sched_task_to_processor( u32_t processor_id, sched_task *task )
		{
			u32_t position;

//			position = pcpu_threads[partition_no]->sched_list_counter;
//			*((sched_task*)(pcpu_threads[partition_no]->sched_list_head) + poistion) = *task;
//			pcpu_threads[partition_no]->sched_list_counter ++;
//			pcpu_threads[partition_no]->sched_list_updated = true;
		}

		//add vertex id (not finished yet)
		static void add_schedule( sched_task * task )
		{
//			u32_t segment_offset, partition_no, position;

			printf( "Will add schedule from %d to %d.\n", task->start, task->term );
			return;
/*			//sanity check first
			if( task->term != 0 )
				if( (task->term <= task->start) || (task->term > gen_config.max_vertex_id) )
					goto WRONG_TASK;
				
			//find out which processor will have it.
			if( task->term == 0 ){ //isolated task
				if(task->start == 0 ) goto WRONG_TASK;
				segment_offset = task->start % seg_config->segment_cap;
				partition_no = segment_offset / seg_config->partition_cap;
				//add task to cpu_thread[partition_no]
				add_sched_task_to_processor( partition_no, task );
			}

			return;
WRONG_TASK:
			printf( "Engine: add_schedule wrong with task: from %d to %d.\n", task->start, task->term );
*/
		}

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
			printf( "Engine::map_anon_memory had allocated %llu bytes at %llx\n", size, (u64_t)space);

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

#endif
