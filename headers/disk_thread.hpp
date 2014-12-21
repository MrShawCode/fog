/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao
 *
 * Declaration:
 *   Prototype ot the disk thread objects.
 *************************************************************************************************/

#ifndef __DISK_THREAD_HPP__
#define __DISK_THREAD_HPP__

#include <unistd.h>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/thread.hpp>
//#include <boost/thread/mutex.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include "print_debug.hpp"

#include <sys/stat.h>
enum{
	FILE_READ = 0,
	FILE_WRITE
};

typedef unsigned int u32_t;
typedef unsigned long long u64_t;

//declaration of io_work
struct io_work{
	u32_t operation; //choose from enum
	volatile int finished;	//is the work finished? 0 means not finished, 1 means finished
	char* buffer;
	u64_t offset,size;
	bool someone_work_on_it;
	int fd;
    const char * io_file_name;
	//mutex that control the accesses to the disk task queue
	boost::interprocess::interprocess_mutex work_mutex;

	io_work( const char *file_name_in, u32_t oper, char* buf, u64_t offset_in, u64_t size_in );
    void operator()(u32_t disk_thread_id);
};

class io_queue;

//declaration of disk_thread
class disk_thread{
public:
    const unsigned long disk_thread_id;
    class io_queue* work_queue;

    disk_thread(unsigned long disk_thread_id_in, class io_queue* work_queue_in);
    ~disk_thread();
    void operator() ();
};

//declaration of io_queue 
class io_queue{
	public:
	//the disk task queue
	std::vector<struct io_work*> io_work_queue;

	//mutex that control the accesses to the disk task queue
	//although in fog_engine, it is unlikely that the io_work_queue will be accessed
	// by multiple objects at the same time, the queue is likely to be accessed simultaneously
	// by multiple objects in fog_engine_target!
	boost::interprocess::interprocess_mutex io_queue_mutex;

	//semaphore to control the wakeup/block of disk threads
	boost::interprocess::interprocess_semaphore io_queue_sem;

	int attr_fd;
	bool terminate_all;

	disk_thread ** disk_threads;
	boost::thread ** boost_disk_threads;

	io_queue();
	~io_queue();
	void add_io_task( io_work* new_task );
	void del_io_task( io_work * task_to_del );
	void wait_for_io_task( io_work * task_to_wait );
};
#endif
