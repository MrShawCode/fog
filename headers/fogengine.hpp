//declaration of fogengine class

#ifndef __FOGENGINE_H__
#define __FOGENGINE_H__

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

static void *map_anon_memory(unsigned long size,
                             bool mlocked,
                             bool zero = false)
{
  void *space = mmap(NULL, size > 0 ? size:4096,
                     PROT_READ|PROT_WRITE,
                     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
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

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA>
class fogengine{
		class index_vert_array* vert_index;
		VA* vert_attr_header;
		char* buffer_for_write;

		static u32_t fog_engine_state;

		disk_thread * attr_disk_thread;
		boost::thread* boost_attr_disk_thread;

		cpu_thread * pcpu_threads[NUM_PROCESSORS];
		boost::thread * boost_pcpu_threads[NUM_PROCESSORS];
		
	public:
		fogengine(){
			//create the index array for indexing the out-going edges
			vert_index = new index_vert_array;

			//allocate buffer for writing
			buffer_for_write = (char*)map_anon_memory( config::memory_size, true, true );

			//create disk thread
			attr_disk_thread = new disk_thread( DISK_THREAD_BEGIN_WITH );
			boost_attr_disk_thread = new boost::thread( boost::ref(*attr_disk_thread) );

			//create cpu threads
			for( int i=0; i< NUM_PROCESSORS; i++ ){
				pcpu_threads[i] = new cpu_thread(i);
				if( i>0 )	//Do not forget, "this->" is thread 0
					boost_pcpu_threads[i] = new boost::thread( boost::ref(*pcpu_threads[i]) );
			}

			//set fog engine state
			fog_engine_state = INIT;
		}
			
		~fogengine(){
			//terminate the cpu threads
			cpu_thread::terminate = true;
			for(int i=1; i<NUM_PROCESSORS; i++) {
		        boost_pcpu_threads[i]->join();
			}
			for(i=0; i< NUM_PROCESSORS; i++)
				delete pcpu_threads[i];

			//terminate the disk thread
			attr_disk_thread->terminate = true;
			boost_attr_disk_thread->join();
			delete attr_disk_thread;

			//destroy the vertices mapping
			delete vert_index;
		}
		static void add_schedule( u32_t vid ){
			printf( "Will add VID %d to schedule\n", vid );
		}
		void operator() ()
		{
			printf( "fogengine: operator is called!\n" );
			io_work* new_io_work;
			new_io_work = new io_work( FILE_READ, buffer_for_write, 1024 );

			//activate the disk thread
			attr_disk_thread->io_work_to_do = new_io_work;
			attr_disk_thread->disk_tasks.post();
		}
};

#endif
