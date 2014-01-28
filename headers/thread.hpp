#ifndef __THREAD_HPP__
#define __THREAD_HPP__

#define NUM_OF_THREADS	4

class fog_thread{
	private:
	unsigned int thread_id;

	public:
	fog_thread( unsigned int thread_id_in )
		:thread_id(thread_id_in){};

	void operator()();
};

struct task{
	struct vertex *start_vertex;
	int number_of_vertices;
};

class task_queue()
{

	public:
	void add_task();
	void remove_task();
}

#endif

