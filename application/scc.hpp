#ifndef __SCC_H__
#define __SCC_H__

#include "types.hpp"
#include "fog_engine.hpp"
#include "print_debug.hpp"

struct scc_vert_attr{
	u32_t prev_root;
	u32_t component_root;
    bool found_component;
};

struct scc_update
{
    u32_t component_root;
};

class scc_program{
	public:
        static int do_scc_phase;
        static int forward_backward_phase;
        
        static void init( u32_t vid, scc_vert_attr* this_vert ){}
        static void init(u32_t vid, scc_vert_attr* va, u32_t PHASE){}
		static void init(u32_t vid, scc_vert_attr* va, u32_t PHASE, u32_t init_forward_backward_phase, u32_t loop_counter,
                index_vert_array * vert_index){
            if (loop_counter == 1 && forward_backward_phase == FORWARD_TRAVERSAL && vert_index->num_out_edges(vid) == 0)
            {
                va->found_component = true;
                va->prev_root = va->component_root = vid;
            }
            else
            {
                forward_backward_phase = init_forward_backward_phase;
                if (loop_counter == 1 && init_forward_backward_phase == FORWARD_TRAVERSAL)
                {
                    //first init
                    va->component_root = vid;
                    va->prev_root = (u32_t)-1;
                    va->found_component = false;
                    //add schedule for cc work
                    fog_engine<scc_program, scc_vert_attr, scc_update>::add_schedule( vid, 
                            PHASE /*phase:decide which buf to read and write */
                            );
                }
                else 
                {
                    if (loop_counter == 1)
                        assert(init_forward_backward_phase == BACKWARD_TRAVERSAL);
                    //if (loop_counter == 1 && (vid == 9 || vid == 11 || vid == 13 || vid == 768))
                    //    PRINT_DEBUG("Before:vid = %d, va->prev_root = %d, va->component_root = %d\n",
                    //            vid, va->prev_root, va->component_root);
                    //do_scc_phase > 0
                    if (va->component_root != va->prev_root)
                    {
                        va->prev_root = va->component_root;
                        va->component_root = vid;
                        //if (loop_counter == 1 && (vid == 9 || vid == 11 || vid == 13 || vid == 768))
                        //    PRINT_DEBUG("After:vid = %d, va->prev_root = %d, va->component_root = %d\n",
                        //            vid, va->prev_root, va->component_root);
                        if (forward_backward_phase == FORWARD_TRAVERSAL )
                        {
                            fog_engine<scc_program, scc_vert_attr, scc_update>::add_schedule(vid, PHASE);
                            //if (loop_counter == 2)
                            //    PRINT_DEBUG("va->prev_root = %d, va->component_root = %d\n",
                            //            va->perv_root, va->component_root);
                        }
                        if (forward_backward_phase == BACKWARD_TRAVERSAL)
                            fog_engine<scc_program, scc_vert_attr, scc_update>::add_schedule(vid, PHASE);
                    }
                    else //(va->component_root == va->prev_root)
                    {
                        assert(va->component_root == va->prev_root);
                        if (forward_backward_phase == FORWARD_TRAVERSAL && va->found_component == false)
                            va->found_component = true;
                        //if (forward_backward_phase == BACKWARD_TRAVERSAL)
                        //{
                        //    fog_engine<scc_program, scc_vert_attr>::add_schedule(vid, PHASE);
                        //    PRINT_DEBUG("after schedule!\n");
                        //}
                    }
                    
                }
            }
		}

		//scatter updates at vid-th vertex 
		static update<scc_update> *scatter_one_edge(u32_t vid,
                scc_vert_attr * this_vert,
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
            update<scc_update> *ret;
            ret = new update<scc_update>;
            if (forward_backward_phase == FORWARD_TRAVERSAL) 
                ret->dest_vert = this_edge->dest_vert;
            else 
            {
                assert (forward_backward_phase == BACKWARD_TRAVERSAL);
                ret->dest_vert = vid;
                //ret->vert_attr.prev_root = this_vert->prev_root;
                //ret->vert_attr.found_component = true;
            }
            ret->vert_attr.component_root = this_vert->component_root;
            return ret;
		}

        static update<scc_update>* scatter_one_edge( u32_t vid, 
            scc_vert_attr* this_vert, 
            u32_t num_outedge, 
            edge* this_edge ){return NULL;}

		//gather one update "u" from outside
		static void gather_one_update( u32_t vid, scc_vert_attr* this_vert, 
                struct update<scc_update>* this_update, 
                u32_t PHASE)
        {
            /*
             * just gather everything
             */
            if (forward_backward_phase == FORWARD_TRAVERSAL)
            {
                if (this_update->vert_attr.component_root < this_vert->component_root && this_vert->found_component == false)
                {
                    this_vert->component_root = this_update->vert_attr.component_root;
                    fog_engine<scc_program, scc_vert_attr, scc_update>::add_schedule(vid, PHASE);
                }
            }
            else
            {
                assert (forward_backward_phase == BACKWARD_TRAVERSAL);
                //assert(this_update->vert_attr.prev_root == this_vert->prev_root);
                this_vert->component_root = this_update->vert_attr.component_root;
                //assert(this_update->vert_attr.found_component == true);
                //this_vert->found_component = this_update->vert_attr.found_component;
                this_vert->found_component = true;
            }
		}
        static void gather_one_update( u32_t vid, scc_vert_attr * dest_vert_attr, update<scc_update> * u )
        {}

        static void set_finish_to_vert(u32_t vid, scc_vert_attr * this_vert)
        {
            PRINT_DEBUG("seg>1, here!\n");
            this_vert->found_component = true;
            this_vert->prev_root = this_vert->component_root = vid;
        }

        static bool judge_true_false(scc_vert_attr* va)
        {
            if (va->found_component == true || 
                va->prev_root == va->component_root)
                return true;
            return false;
        }
        static bool judge_src_dest(scc_vert_attr *va_src, scc_vert_attr *va_dst, float edge_weight)
        {
            if (va_src == NULL || va_dst == NULL)
                return false;
            assert(va_src != NULL);
            assert(va_dst != NULL);
            if (forward_backward_phase == FORWARD_TRAVERSAL )
            {
                if (va_src->component_root < va_dst->component_root)
                    return true;
                return false;
            }
            else
            {
                assert(forward_backward_phase == BACKWARD_TRAVERSAL);
                if (va_src->prev_root == va_dst->prev_root && 
                    va_dst->prev_root == va_dst->component_root && 
                    va_src->prev_root != va_src->component_root)
                {
                //PRINT_DEBUG("src:prev = %d, component_root = %d, dst:prev_root = %d, component_root = %d\n", 
                //        va_src->prev_root, va_src->component_root, 
                //        va_dst->prev_root, va_dst->component_root);
                    return true;
                }
                return false;
            }
        }
        static void print_result(u32_t vid, scc_vert_attr * va)
        {
            PRINT_DEBUG("SCC:result[%d], prev_root = %d, component_root = %d\n", vid, va->prev_root, va->component_root);
        }
};

int scc_program::do_scc_phase = 0;
int scc_program::forward_backward_phase = -1;
#endif
