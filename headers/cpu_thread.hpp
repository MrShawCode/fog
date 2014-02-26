#ifndef __CPU_THREAD_HPP__
#define __CPU_THREAD_HPP__

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

class cpu_thread {
public:
    static barrier *sync;
    const unsigned long processor_id; 
	index_vert_array* vert_index;
    static volatile bool terminate;
    static struct cpu_work * volatile work_to_do;

    cpu_thread(u32_t processor_id_in, class index_vert_array * vert_index_in)
    :processor_id(processor_id_in), vert_index( vert_index_in )
    {   
        if(sync == NULL) { // First object
	        sync = new barrier(NUM_PROCESSORS);
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
            (*work_to_do)(processor_id, sync, vert_index );

            sync->wait(); // Must synchronize before p0 exits (object is on stack)
            }
        }while(processor_id != 0);
    }
};

#endif
