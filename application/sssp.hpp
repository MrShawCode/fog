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
		static void init(u32_t vid, sssp_vert_attr* va, u32_t PHASE){
			if ( vid == start_vid ){
				va->value = 0;
                PRINT_DEBUG("VID = %d\n", vid);
				fog_engine_target<sssp_program, sssp_vert_attr>::add_schedule( vid, 
                        PHASE /*phase:decide which buf to read and write */
                        );
			}
            else
            { 
				va->value = INFINITY;
            }
                va->predecessor = (u32_t)-1;
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
		static void gather_one_update( u32_t vid, sssp_vert_attr* this_vert, 
                struct update<sssp_vert_attr>* this_update, 
                u32_t PHASE)
        {
			//compare the value of u, if it is smaller, absorb the update
            if (PHASE == 0)
            {
           //     PRINT_DEBUG("this_update.value = %f, this_vert->value = %f\n", this_update->vert_attr.value, this_vert->value);
            }
                //PRINT_DEBUG("check in gather_one_update!\n");
			if( this_update->vert_attr.value < this_vert->value ){
				*this_vert = this_update->vert_attr;
				//should add schedule of {vid,0}, need api from engine
				fog_engine_target<sssp_program, sssp_vert_attr>::add_schedule( vid, PHASE);
			}
		}
};

unsigned int sssp_program::start_vid = 0;

#endif
