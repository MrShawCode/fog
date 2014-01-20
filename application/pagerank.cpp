#include "program.hpp"

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

	prog.init();

}

