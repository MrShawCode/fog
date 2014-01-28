#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <iostream>

#include "program.hpp"
#include "engine.hpp"

class sssp_prog: public program{

	public:
	void init(){
		std::cout << "sssp program init()\n";
	}

	void update(){
		std::cout << "sssp program update()\n";
	}
};

main()
{
	class sssp_prog sssp_prog;
	class fog_engine fogeng;

	fogeng.init( );
//	prog.init();
	fogeng.run( &sssp_prog );
	fogeng.reclaim_res();
}

