//declaration of fog_engine class

#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"

enum fog_engine_state_enum{
	INIT = 0,
	RUN,
	TERM
};

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fog_engine{
		index_vert_array* vert_index;
		VA* vert_attr_header;
		char* buffer_for_write;
		segment_config<VA> *seg_config;

		static u32_t fog_engine_state;

		disk_thread * attr_disk_thread;
		boost::thread* boost_attr_disk_thread;

		cpu_thread<A,VA> * pcpu_threads[NUM_PROCESSORS];
		boost::thread * boost_pcpu_threads[NUM_PROCESSORS];
		
	public:
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
			for( int i=0; i< NUM_PROCESSORS; i++ ){
				pcpu_threads[i] = new cpu_thread<A,VA>(i, vert_index, buffer_for_write);
				if( i>0 )	//Do not forget, "this->" is thread 0
					boost_pcpu_threads[i] = new boost::thread( boost::ref(*pcpu_threads[i]) );
			}

			//set fog engine state
			fog_engine_state = INIT;
		}
			
		~fog_engine(){
/*			//terminate the cpu threads
			cpu_thread::terminate = true;
			for(int i=1; i<NUM_PROCESSORS; i++) {
		        boost_pcpu_threads[i]->join();
			}
			for(i=0; i< NUM_PROCESSORS; i++)
				delete pcpu_threads[i];
*/
			//terminate the disk thread
			attr_disk_thread->terminate = true;
			boost_attr_disk_thread->join();
			delete attr_disk_thread;

			//destroy the vertices mapping
			delete vert_index;
		}

		//add vertex id 
		static void add_schedule( sched_task * task ){
			printf( "Will add schedule from %d to %d.\n", task->start, task->term );
			//sanity check first
			if( task->term != 0 )
				if( (task->term <= task->start) || (task->term > gen_config.max_vertex_id) )
					goto WRONG_TASK;
				
			//find out which processor will have it.
			if( task->term == 0 ){ //isolated task
				if(task->start == 0 ) goto WRONG_TASK;
				u32_t segment_offset = task->start % seg_config->segment_cap;
				u32_t partition_no = segment_offset / seg_config->partition_cap;
			}

			return;
WRONG_TASK:
			printf( "Engine: add_schedule wrong with task: from %d to %d.\n", task->start, task->term );
		}

		void operator() ()
		{
			printf( "fog_engine: operator is called!\n" );
			io_work* new_io_work;
			new_io_work = new io_work( FILE_READ, buffer_for_write, 1024 );

			//activate the disk thread
			attr_disk_thread->io_work_to_do = new_io_work;
			attr_disk_thread->disk_tasks.post();
		}
};

#endif
