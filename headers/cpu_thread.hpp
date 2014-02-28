#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include <boost/interprocess/sync/interprocess_mutex.hpp>

class cpu_thread;

class barrier {
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
    friend class cpu_thread;
};

struct cpu_work{
	char* attr_buffer;
	u32_t attr_buffer_size;
	u32_t start_vertex_id;

	cpu_work( char* buffer, u32_t size, u32_t start_vert )
		:attr_buffer(buffer), attr_buffer_size(size), start_vertex_id(start_vert)
	{}
	
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index )
	{
        sync->wait();
		//first find the works that "I" should do
		u32_t start, term;
		

		//do works here
		
        sync->wait();
	}
};

template <typename A, typename VA>
class cpu_thread {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	//long vision is to merge these two buffers.
	sched_task *sched_list_head;
	boost::interprocess::interprocess_mutex sched_list_mutex;
	bool sched_list_updated;	//is the list updated (e.g., a new task inserted)
	u32_t sched_list_counter; 	//how many sched tasks are there?

	update<VA> *update_buffer_start;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, char* write_buffer_start )
    :processor_id(processor_id_in), vert_index( vert_index_in )
    {   
        if(sync == NULL) { //as it is shared, be created for one time
	        sync = new barrier(NUM_PROCESSORS);
        }
		//compute my sched and update buffer according to the inputted parameters

    }   

    void operator() ()
    {
        do{
            sync->wait();
            if(terminate) {
	            break;
            }
            else {
            (*work_to_do)(processor_id, sync, vert_index );

            sync->wait(); // Must synchronize before p0 exits (object is on stack)
            }
        }while(processor_id != 0);
    }
};

#endif
