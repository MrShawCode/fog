#ifndef __DISK_THREAD_HPP__
#define __DISK_THREAD_HPP__

#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

enum{
	FILE_READ = 0,
	FILE_WRITE
};

struct io_work{
	io_work( u32_t oper, char* buf, u32_t size_in )
		:operation(oper), finished(false), buffer(buf), size(size_in)
	{}

    void operator() (unsigned int processor_id)
    {   
		printf( "disk tasks is received by disk thread %d\n", processor_id );
		printf( "the io work to do: operation:%d, size:%d, buffer:%llx\n", 
			operation, size, (u64_t)buffer );
		finished = true;
	}

	u32_t operation; //choose from enum
	bool finished;	//is the work finished?
	char* buffer;
	u32_t size;
};

class disk_thread {
public:
    const unsigned long processor_id; 
    struct io_work * volatile io_work_to_do;
	boost::interprocess::interprocess_semaphore disk_tasks;
	int attr_fd;
    bool terminate;

    disk_thread(unsigned long processor_id_in)
    :processor_id(processor_id_in), disk_tasks(0), terminate(false)
    {
		attr_fd = open( gen_config.attr_file_name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IRGRP | S_IROTH );
		if( attr_fd < 0 ){
			printf( "Cannot create attribute file for writing!\n");
			exit( -1 );
		}
	}

	~disk_thread(){
		close( attr_fd );
	}

    void operator() ()
    {
        do{
            disk_tasks.wait();
            (*io_work_to_do)(processor_id);

			//TODO: problem on logic!
            if(terminate) {
				printf( "disk thread terminating\n" );
 	        	break;
            }
        }while(1);
    }
};

#endif
