#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include "config.hpp"

enum fog_engine_state_enum{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM
};

template <typename A, typename VA>
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
//    friend class cpu_thread<A,VA>;
};

struct cpu_work{
	u32_t engine_state;
	char* attr_buffer_head;
	char* my_attr_buffer;
	u32_t attr_buffer_size;
	u32_t start_vertex_id;

	cpu_work( u32_t state, char* buffer, u32_t size, u32_t start_vert )
		:engine_state(state), attr_buffer_head(buffer), attr_buffer_size(size), start_vertex_id(start_vert)
	{}
	
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index )
	{
        sync->wait();
		
		switch( engine_state ){
			case INIT:
				printf( "processor:%d, will initialized 0x%llx for %u (size)\n", 
					processor_id, (u64_t)attr_buffer_head, attr_buffer_size );
				break;
			case SCATTER:
				break;
			default:
				printf( "Unknow fog engine state is encountered\n" );
		}

        sync->wait();
	}
};

template <typename A, typename VA>
class cpu_thread {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	//long vision is to merge these two buffers. TODO
	sched_task *sched_list_head;
	bool sched_list_updated;	//is the list updated (e.g., a new task inserted)
	u32_t sched_list_counter; 	//how many sched tasks are there?

	update<VA> *update_list_head;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, char* write_buffer_start )
    :processor_id(processor_id_in), vert_index( vert_index_in )
    {   
        if(sync == NULL) { //as it is shared, be created for one time
	        sync = new barrier(gen_config.num_processors);
        }
		//compute my sched and update buffer according to the inputted parameters
		//NOTE: sched task buffer and update buffer for each processor should be contiguous!
		sched_list_head = (sched_task*)((u64_t)write_buffer_start + processor_id*((gen_config.memory_size/2)/gen_config.num_processors));
		sched_list_counter = 0;
		sched_list_updated = false;

		update_list_head = (update<VA>*)((u64_t)sched_list_head + (gen_config.memory_size/4)/gen_config.num_processors);

		printf( "for processor %lu, sched_list_head:0x%llx, update_list_head:0x%llx\n", 
			processor_id, (u64_t)sched_list_head, (u64_t)update_list_head );
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

	sched_task* get_sched_task()
	{return NULL;}

	void sort_sched_list()
	{}

	void merge_sched_list()
	{}

	void browse_sched_list()
	{}

};

template <typename A, typename VA>
barrier * cpu_thread<A, VA>::sync;

template <typename A, typename VA>
volatile bool cpu_thread<A, VA>::terminate;

template <typename A, typename VA>
cpu_work * volatile cpu_thread<A, VA>::work_to_do;

#endif
