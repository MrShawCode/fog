#ifndef __CC_H__
#define __CC_H__

#include "types.hpp"
#include "fog_engine_scc.hpp"

struct cc_vert_attr{
	u32_t component_root;
};

class cc_program{
	public:
        
		static void init(u32_t vid, cc_vert_attr* va, u32_t PHASE)
        {
            va->component_root = vid;
            //add schedule for cc work
            fog_engine_scc<cc_program, cc_vert_attr, cc_vert_attr>::add_schedule( vid, 
                    PHASE /*phase:decide which buf to read and write */
                    );
		}
        static void init(u32_t vid, cc_vert_attr* va, u32_t PHASE, u32_t init_forward_backward_phase, u32_t loop_counter,
            index_vert_array * vert_index){}
        static void init( u32_t vid, cc_vert_attr* this_vert ){}

		//scatter updates at vid-th vertex 
		static update<cc_vert_attr> *scatter_one_edge(u32_t vid,
                cc_vert_attr * this_vert,
                edge * this_edge)//, bool forward_in_backward)
        {
            /*
             * Conditions to enter this function:
             * 1.this_vert->found_component = false
             * 2.if scc_phase is forward traversal, this_edge->dest_vert = vid, and this_vert is vid's attr;
             * 3.if scc_phase is backward traversal, this_edge->src = vid, and this vert is vid's attr;
             *
             * backword traversal: 
             * if this is an edge:A->B, and A's component is little than B, 
             * then after forward traversal, A has pass the component-root to B,
             * now, if this is an edge B->A, we can see A<->B, so A,B share the 
             * same component, so, after init to respring B's component,
             * when backward traversal, we transfer A's component to B
             */
            update<cc_vert_attr> *ret;
            ret = new update<cc_vert_attr>;
            ret->dest_vert = this_edge->dest_vert;
            ret->vert_attr.component_root = this_vert->component_root;
            return ret;
		}

        static update<cc_vert_attr>* scatter_one_edge( u32_t vid, 
            cc_vert_attr* this_vert, 
            u32_t num_outedge, 
            edge* this_edge ){return NULL;}

		//gather one update "u" from outside
		static void gather_one_update( u32_t vid, cc_vert_attr* this_vert, 
                struct update<cc_vert_attr>* this_update, 
                u32_t PHASE)
        {
            /*
             * just gather everything
             */
            if (this_update->vert_attr.component_root < this_vert->component_root)
            {
                this_vert->component_root = this_update->vert_attr.component_root;
                fog_engine_scc<cc_program, cc_vert_attr, cc_vert_attr>::add_schedule(vid, PHASE);
            }
		}

        static void gather_one_update( u32_t vid, cc_vert_attr * dest_vert_attr, update<cc_vert_attr> * u )
        {}
        static void set_finish_to_vert(u32_t vid, cc_vert_attr * this_vert){}
        static bool judge_true_false(cc_vert_attr* va){return false;}
        static bool judge_src_dest(cc_vert_attr *va_src, cc_vert_attr *va_dst){return false;}
};

#endif
