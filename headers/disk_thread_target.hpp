#ifndef __DISK_THREAD_TARGET_HPP__
#define __DISK_THREAD_TARGET_HPP__

#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/thread.hpp>
//#include <boost/thread/mutex.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include "print_debug.hpp"
#include "disk_thread.hpp"

/*enum{
	FILE_READ = 0,
	FILE_WRITE
};*/

struct io_work_target{
	u32_t operation; //choose from enum
	volatile int finished;	//is the work finished? 0 means not finished, 1 means finished
	char* buffer;
	u64_t offset,size;
	bool someone_work_on_it;
	int fd;
    const char * io_file_name;
	//mutex that control the accesses to the disk task queue
	boost::interprocess::interprocess_mutex work_mutex;

	io_work_target( const char *file_name_in, u32_t oper, char* buf, u64_t offset_in, u64_t size_in )
		:operation(oper), finished(0), buffer(buf), offset(offset_in), 
		 size(size_in), someone_work_on_it( false ), io_file_name(file_name_in)
	{}

    void operator() (u32_t disk_thread_id)
    {   
		//dump buffer to file
		switch( operation ){
			case FILE_READ:
			{
                PRINT_DEBUG( "read to disk content to buffer :0x%llx, offset:%llu, size:%llu\n", 
                    (u64_t)buffer, offset, size );
                        
                int finished=0, remain=size, res;

                //the file should exist now
                //fd = open( gen_config.attr_file_name.c_str(), O_RDWR, S_IRUSR | S_IRGRP | S_IROTH );
                fd = open( io_file_name, O_RDWR, S_IRUSR | S_IRGRP | S_IROTH );
                if( fd < 0 ){
                    PRINT_ERROR( "Cannot open attribute file for writing!\n");
                    exit( -1 );
                }
                if( lseek( fd, offset, SEEK_SET ) < 0 ){
                    PRINT_ERROR( "Cannot seek the attribute file!\n");
                    exit( -1 );
                }
                while( finished < (int)size ){
                    if( (res = read(fd, buffer, remain)) < 0 ) 
                        PRINT_ERROR( "failure on disk reading!\n" );
                    finished += res;
                    remain -= res;
                }

                close(fd);

				break;
			}
			case FILE_WRITE:
			{
				//PRINT_DEBUG( "dump to disk tasks is received by disk thread, buffer:0x%llx, offset:%llu, size:%llu\n", 
				//	(u64_t)buffer, offset, size );
					
				int written=0, remain=size, res;

				//the file should exist now
				//fd = open( gen_config.attr_file_name.c_str(), O_RDWR, S_IRUSR | S_IRGRP | S_IROTH );
				//fd = open( io_file_name, O_RDWR | O_CREAT, S_IRUSR | S_IRGRP | S_IROTH );
				fd = open( io_file_name, O_RDWR , S_IRUSR | S_IRGRP | S_IROTH );
                PRINT_DEBUG("io_file_name:%s\n", io_file_name);
				if( fd < 0 ){
					PRINT_ERROR( "Cannot open attribute file for writing!\n");
					exit( -1 );
				}
				if( lseek( fd, offset, SEEK_SET ) < 0 ){
					PRINT_ERROR( "Cannot seek the attribute file!\n");
					exit( -1 );
				}
				while( written < (int)size ){
					if( (res = write(fd, buffer, remain)) < 0 )
						PRINT_ERROR( "failure on disk writing!\n" );
					written += res;
					remain -= res;
				}

				close(fd);
				break;
			}
		}
		//atomically increment finished 
		__sync_fetch_and_add(&finished, 1);
		__sync_synchronize();
	}
};

class io_queue_target;

class disk_thread_target {
public:
    const unsigned long disk_thread_id;
    class io_queue_target* work_queue;

    disk_thread_target(unsigned long disk_thread_id_in, class io_queue_target* work_queue_in)
    :disk_thread_id(disk_thread_id_in), work_queue(work_queue_in)
    {
    }

    ~disk_thread_target(){
    }

    void operator() ();
};

class io_queue_target{
	public:
	//the disk task queue
	std::vector<struct io_work_target*> io_work_queue;

	//mutex that control the accesses to the disk task queue
	//although in fog_engine, it is unlikely that the io_work_queue will be accessed
	// by multiple objects at the same time, the queue is likely to be accessed simultaneously
	// by multiple objects in fog_engine_target!
	boost::interprocess::interprocess_mutex io_queue_mutex;

	//semaphore to control the wakeup/block of disk threads
	boost::interprocess::interprocess_semaphore io_queue_sem;

	int attr_fd;
	bool terminate_all;

	disk_thread_target ** disk_threads;
	boost::thread ** boost_disk_threads;

	io_queue_target():io_queue_sem(0), terminate_all(false)
	{
        //clear io work array
        io_work_queue.clear();
        std::vector<struct io_work_target*>().swap(io_work_queue);

		//the attribute file may not exist!
		attr_fd = open( gen_config.attr_file_name.c_str(), O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR | S_IRGRP | S_IROTH );
		if( attr_fd < 0 ){
			PRINT_ERROR( "Cannot create attribute file for writing!\n");
			exit( -1 );
		}
		if( ftruncate( attr_fd, 0 ) < 0 ){
			PRINT_ERROR( "Cannot create attribute file for writing!\n");
			exit( -1 );
		}
			
		close( attr_fd );

		//invoke the disk threads
		disk_threads = new disk_thread_target * [gen_config.num_io_threads];
		boost_disk_threads = new boost::thread *[gen_config.num_io_threads];
		for( u32_t i=0; i<gen_config.num_io_threads; i++ ){
			disk_threads[i] = new disk_thread_target( DISK_THREAD_ID_BEGIN_WITH + i, this );
			boost_disk_threads[i] = new boost::thread( boost::ref(*disk_threads[i]) );
		}
	}

	~io_queue_target()
	{
		//should wait the termination of all disk threads
		terminate_all = true;
		for( u32_t i=0; i<gen_config.num_io_threads; i++ )
			io_queue_sem.post();
		
		for( u32_t i=0; i<gen_config.num_io_threads; i++ )
			boost_disk_threads[i]->join();

		PRINT_DEBUG( "IO_QUEUE, terminated all disk threads\n" );
		//reclaim the resources occupied by the disk threads
	}

	void add_io_task( io_work_target* new_task )
	{
		assert( new_task->finished==0 );
		boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(io_queue_mutex);
		io_work_queue.push_back( new_task );

		//activate one disk thread to handle this task
		io_queue_sem.post();
	}

	void del_io_task( io_work_target* task_to_del )
	{
		assert( task_to_del->finished==1 );
		boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(io_queue_mutex);
		for( unsigned i=0; i<io_work_queue.size(); i++){
			if( io_work_queue.at(i) == task_to_del ){
//				PRINT_DEBUG( "in del_io_task, will delete the element at position %u.\n", i );
				io_work_queue.erase(io_work_queue.begin()+i);
			}
		}
		delete task_to_del;
	}

	//Note:
	// After calling this member function, the thread will spin on waiting for the completion
	// of specific io task!
	// i.e., this is a BLOCKING operation!
	void wait_for_io_task( io_work_target* task_to_wait )
	{
        while( 1 ){
			//should measure the time spent on waiting
	        if( task_to_wait->finished ) break;
        };
	}
};

void disk_thread_target::operator() ()
{

       do{
            work_queue->io_queue_sem.wait();

            if(work_queue->terminate_all) {
                PRINT_DEBUG( "disk thread terminating\n" );
                break;
            }

            //find work to do now.
			//Concurrency HAZARD: the io_work may be deleted during the searching!
			// e.g., the main thread delete some io work by invoking io_queue->del_io_task,
			// at the time of searching!
			//THEREFOR, do NOT allow any changes to the io task queue during searching.
			//HOWEVER, keep this critical section as small as possible!
			io_work_target* found=NULL;
			unsigned i;
			for( i=0; i<work_queue->io_work_queue.size(); i++ )
			{
				boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex> lock(work_queue->io_queue_mutex);
				found = work_queue->io_work_queue.at(i);
				if( found->finished ) continue;
				if( found->someone_work_on_it ) continue;
				found->someone_work_on_it = true;
				break;
			}

			if( (found==NULL) || (i == work_queue->io_work_queue.size()) ) //nothing found! unlikely to happen
				PRINT_DEBUG( "IO Thread %lu found no work to do!\n", disk_thread_id );

			(*found)(disk_thread_id);

        }while(1);
};

#endif
