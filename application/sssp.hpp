#ifndef __SSSP_H__
#define __SSSP_H__

#include "type.hpp"

struct vert_att{
	unsigned int predecessor;
	float	value;
};

class sssp_program{
	private:
		unsigned int start_vid;
		struct vert_att* vert_att_header;
	pulbic:
		sssp_program( unsigned int input_vid ):
			start_vid(input_vid){
			//create attribute file, need api from engine
		};
		//init the vid-th vertex
		void init(unsigned int vid){
			if ( vid == start_vid ){
				vert_att_header[vid].value = 0;
				//should add schedule of vid, need api from engine
			}else
				vert_att_header[vid].value = INFINITY;
		}
		//scatter updates at vid-th vertex 
		void scatter(unsigned int vid ){
	
		}

		//gather one update "u" from outside
		void gather_one_update( unsigned int vid, struct update<vert_att>* u ){
			//compare the value of u, if it is smaller, absorb the update
			if( u.vert_attribute.value < vert_att_header[vid].value ){
				vert_att_header[vid] = u.vert_attribute;
				//should add schedule of vid, need api from engine
			}
		}
}

#endif

