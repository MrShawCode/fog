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
	u32_t operation; //choose from enum
	volatile int finished;	//is the work finished? 0 means not finished, 1 means finished
	char* buffer;
	u32_t size;

	io_work( u32_t oper, char* buf, u32_t size_in )
		:operation(oper), finished(0), buffer(buf), size(size_in)
	{}

    void operator() (u32_t processor_id, int fd)
    {   
		//dump buffer to filea
		switch( operation ){
			case FILE_READ:
				break;
			case FILE_WRITE:
				printf( "dump to disk tasks is received by disk thread, buffer:0x%llx, size:%u\n", 
					(u64_t)buffer, size );
				int written=0, remain=size, res;

				while( written < (int)size ){
					if( (res = write(fd, buffer, remain)) < 0 )
						printf( "failure on disk writing!\n" );
					written += res;
					remain -= res;
				}
				break;
		}
		//atomically increment finished 
		__sync_fetch_and_add(&finished, 1);
		finished = true;
		__sync_synchronize();
	}
};

class disk_thread {
public:
    const unsigned long processor_id; 
    struct io_work * volatile io_work_to_do;
	boost::interprocess::interprocess_semaphore disk_task_sem;
	int attr_fd;
    bool terminate;

    disk_thread(unsigned long processor_id_in)
    :processor_id(processor_id_in), disk_task_sem(0), terminate(false)
    {
		attr_fd = open( gen_config.attr_file_name.c_str(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR | S_IRGRP | S_IROTH );
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
            disk_task_sem.wait();

			//TODO: problem on logic!
            if(terminate) {
				printf( "disk thread terminating\n" );
 	        	break;
            }
			if( io_work_to_do )
	            (*io_work_to_do)(processor_id, attr_fd);

        }while(1);
    }
};

#endif
