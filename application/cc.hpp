#ifndef __CC_H__
#define __CC_H__

#include "types.hpp"
#include "fog_engine.hpp"

struct cc_vert_attr{
	u32_t component_root;
};

template <typename T>
class cc_program{
	public:
        static u32_t num_tasks_to_sched ;
        static int forward_backward_phase;
        static int CONTEXT_PHASE;
        static int loop_counter;
        static bool init_sched;
        
		static void init(u32_t vid, cc_vert_attr* va, index_vert_array<T> * vert_index)
        {
            va->component_root = vid;
            //add schedule for cc work
            fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T>::add_schedule( vid, 
                    CONTEXT_PHASE /*phase:decide which buf to read and write */
                    );
		}
		//scatter updates at vid-th vertex 
		static update<cc_vert_attr> *scatter_one_edge(
                cc_vert_attr * this_vert,
                T * this_edge,
                u32_t backward_update_dest)//, bool forward_in_backward)
        {
            update<cc_vert_attr> *ret;
            ret = new update<cc_vert_attr>;
            if (forward_backward_phase == FORWARD_TRAVERSAL)
                ret->dest_vert = this_edge->get_dest_value();
            else
            {
                assert(forward_backward_phase == BACKWARD_TRAVERSAL);
                ret->dest_vert = backward_update_dest;
            }
            ret->vert_attr.component_root = this_vert->component_root;
            return ret;
		}

		//gather one update "u" from outside
		static void gather_one_update( u32_t vid, cc_vert_attr* this_vert, 
                struct update<cc_vert_attr>* this_update) 
        {
            /*
             * just gather everything
             */
            if (this_update->vert_attr.component_root < this_vert->component_root)
            {
                this_vert->component_root = this_update->vert_attr.component_root;
                fog_engine<cc_program<T>, cc_vert_attr, cc_vert_attr, T>::add_schedule(vid, CONTEXT_PHASE);
            }
		}

        static void before_iteration()
        {
            if (forward_backward_phase == FORWARD_TRAVERSAL)
                PRINT_DEBUG("CC engine is running FORWARD_TRAVERSAL for the %d iteration, there are %d tasks to schedule!\n",
                        loop_counter, num_tasks_to_sched);
            else
            {
                assert(forward_backward_phase == BACKWARD_TRAVERSAL);
                PRINT_DEBUG("CC engine is running BACKWARD_TRAVERSAL for the %d iteration, there are %d tasks to schedule!\n",
                        loop_counter, num_tasks_to_sched);
            }
        }
        static int after_iteration()
        {
            PRINT_DEBUG("CC engine has finished the %d iteration, there are %d tasks to schedule at next iteration!\n",
                    loop_counter, num_tasks_to_sched);
            if (num_tasks_to_sched == 0)
                return ITERATION_STOP;
            else
                return ITERATION_CONTINUE;
        }
        static int finalize()
        {
            if (forward_backward_phase == FORWARD_TRAVERSAL)
            {
                forward_backward_phase = BACKWARD_TRAVERSAL;
                loop_counter = 0;
                CONTEXT_PHASE = 0;
                return ENGINE_CONTINUE;
            }
            else
            {
                assert(forward_backward_phase == BACKWARD_TRAVERSAL);
                return ENGINE_STOP;
            }
        }

        static void print_result(u32_t vid, cc_vert_attr * va)
        {
            PRINT_DEBUG("CC:result[%d], component_root = %d\n",vid, va->component_root);
        }
};
/*
 * forward_backward_phase is setup for backward algorithms,
 * we will let the fog-engine read in-edge for backward algorithms.
 * -1 for initilization, you can set in main() and finalize(). 
 * FORWARD_TRAVERSAL = 0
 * BACKWARD_TRAVERSAL = 1
 */
template <typename T>
unsigned int cc_program<T>::num_tasks_to_sched = 0;

template <typename T>
int cc_program<T>::forward_backward_phase = FORWARD_BACKWARD_TRAVERSAL;

template <typename T>
int cc_program<T>::CONTEXT_PHASE = 0;

template <typename T>
int cc_program<T>::loop_counter = 0;


template <typename T>
bool cc_program<T>::init_sched = true;
//if you want add_schedule when init(), please set this to be true~!

#endif
