#ifndef __SSSP_H__
#define __SSSP_H__

#include "type.hpp"
#include "fogengine.hpp"

struct sssp_vert_attr{
	unsigned int predecessor;
	float	value;
};

class sssp_program{
	public:
		static unsigned int start_vid;
		//init the vid-th vertex
		static void init(unsigned int vid, sssp_vert_attr* va){
			if ( vid == start_vid ){
				va->value = 0;
				fogengine<sssp_program, sssp_vert_attr>::add_schedule( vid );
				//should add schedule of vid, need api from engine
			}else
				va->value = INFINITY;
		}

		//scatter updates at vid-th vertex 
		static void scatter(unsigned int vid ){
	
		}

		//gather one update "u" from outside
		static void gather_one_update( unsigned int vid, sssp_vert_attr* va, struct update<sssp_vert_attr>* u ){
			//compare the value of u, if it is smaller, absorb the update
			if( u->vert_attribute.value < va->value ){
				*va = u->vert_attribute;
				//should add schedule of vid, need api from engine
			}
		}
};

#endif
