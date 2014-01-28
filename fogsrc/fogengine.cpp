#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <iostream>

#include "engine.hpp"

boost::thread ** thread_array;
fog_thread ** run_threads;

fog_engine::fog_engine()
{
	std::cout << "fog_engine() constructor\n";
//	return 0;
}

fog_engine::~fog_engine()
{
	std::cout << "fog_engine() destructor\n";
//	return 0;
}

void fog_engine::init()
{
	std::cout << "fog_engine, start to initialize\n";

	//spawn the thread array now.
	run_threads = new fog_thread *[NUM_OF_THREADS];
	thread_array = new boost::thread *[NUM_OF_THREADS];

    for (unsigned int j = 0; j < NUM_OF_THREADS; j++)
    {   
        run_threads[j] = new fog_thread(j);
        thread_array[j] = new boost::thread(boost::ref(*run_threads[j]));
    }

	std::cout << "fog_engine, initialized\n";
}

void fog_engine::run( class program *po )
{
	std::cout << "fog_engine, start to run\n";
	po->init();
}

void fog_engine::reclaim_res()
{
	for( int i=0; i<NUM_OF_THREADS; i++ )
		thread_array[i]->join();
}

