#include "program.hpp"
#include "engine.hpp"

class pagerank_prog: public program{

	public:
	void init(){
		std::cout << "pagerank program init()\n";
	}

	void update(){
		std::cout << "pagerank program update()\n";
	}
};

main()
{
	class pagerank_prog prog;
	class fog_engine fogeng;

	fogeng.init( );
//	prog.init();
	fogeng.run( &prog );

}

