#include <iostream>

class program{
	public:
		program();
		~program();

		virtual void init(){ std::cout << "in virtual init()\n"; };
		virtual void update(){ std::cout << "in virtual update()\n"; };
};

