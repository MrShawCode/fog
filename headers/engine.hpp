#ifndef __ENGINE_HPP__
#define __ENGINE_HPP__

#include "program.hpp"
#include "thread.hpp"

class fog_engine{
	public:
	fog_engine();
	~fog_engine();

	void init();
	void run( class program *po );
	void reclaim_res();

};

#endif

