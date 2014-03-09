#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

#include "config.hpp"

enum fog_engine_state_enum{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM
};

#define SCHED_BUFFER_LEN    1024

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

struct init_param{
	char* attr_buffer_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
};

struct scatter_param{

};

struct gather_param{

};

template <typename A, typename VA>
struct cpu_work{
	u32_t engine_state;
	void* state_param;

	cpu_work( u32_t state, void* state_param_in )
		:engine_state(state), state_param(state_param_in)
	{}
	
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index, segment_config<VA>* seg_config )
	{
		u32_t local_start_vert_off, local_term_vert_off;
        sync->wait();
		
		switch( engine_state ){
			case INIT:
			{	//add {} to prevent "error: jump to case label" error. Cann't believe that!
				init_param* p_init_param = (init_param*) state_param;

				if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

				//compute loca_start_vert_id and local_term_vert_id
				local_start_vert_off = processor_id*(seg_config->partition_cap);

				if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
					local_term_vert_off = p_init_param->num_of_vertices - 1;
				else
					local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;
			
				printf( "processor:%d, vert start from %u, number:%u local start from vertex %u to %u\n", 
					processor_id, 
					p_init_param->start_vert_id, 
					p_init_param->num_of_vertices, 
					local_start_vert_off, 
					local_term_vert_off );

				//Note: for A::init, the vertex id and VA* address does not mean the same offset!
				for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
					A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buffer_head) + i );

				break;
			}
			case SCATTER:
			{

			}
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
	segment_config<VA>* seg_config;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work<A,VA> * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA>* seg_config_in )
    :processor_id(processor_id_in), vert_index(vert_index_in), seg_config(seg_config_in)
    {   
        if(sync == NULL) { //as it is shared, be created for one time
	        sync = new barrier(gen_config.num_processors);
        }
    }   

    void operator() ()
    {
        do{
            sync->wait();
            if(terminate) {
	            break;
            }
            else {
	            (*work_to_do)(processor_id, sync, vert_index, seg_config );

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
cpu_work<A,VA> * volatile cpu_thread<A, VA>::work_to_do;

#endif
