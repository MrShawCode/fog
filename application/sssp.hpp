#ifndef __SSSP_H__
#define __SSSP_H__

#include "types.hpp"
#include "fog_engine_target.hpp"

struct sssp_vert_attr{
	u32_t predecessor;
	float	value;
};

class sssp_program{
	public:
		static unsigned int start_vid;
		//init the vid-th vertex
		static void init(unsigned int vid, sssp_vert_attr* va){
			if ( vid == start_vid ){
				va->value = 0;
                //PRINT_DEBUG("VID = %d\n", vid);
				fog_engine_target<sssp_program, sssp_vert_attr>::add_schedule( vid, 0 /*phase:decide which file to read and write */);
				//should add schedule of vid, need api from engine
			}else
				va->value = INFINITY;
		}

		//scatter updates at vid-th vertex 
		static update<sssp_vert_attr> *scatter_one_edge(u32_t vid,
                sssp_vert_attr * this_vert,
                u32_t num_outedge,
                edge * this_edge)
        {
            update<sssp_vert_attr> *ret;
            float scatter_weight = this_vert->value + this_edge->edge_weight;
            u32_t scatter_predecessor = vid;
            ret = new update<sssp_vert_attr>; 
            ret->dest_vert = this_edge->dest_vert;
            ret->vert_attr.value = scatter_weight;
            ret->vert_attr.predecessor = scatter_predecessor;
            return ret;
		}

		//gather one update "u" from outside
		static void gather_one_update( unsigned int vid, sssp_vert_attr* va, struct update<sssp_vert_attr>* u ){
			//compare the value of u, if it is smaller, absorb the update
			if( u->vert_attr.value < va->value ){
				*va = u->vert_attr;
				//should add schedule of {vid,0}, need api from engine
			}
		}
};

unsigned int sssp_program::start_vid = 0;

#endif
