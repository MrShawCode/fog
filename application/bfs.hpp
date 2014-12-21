/**************************************************************************************************
 * Authors: 
 *   Jian He,
 *
 * Routines:
 *   
 *************************************************************************************************/

#ifndef __BFS_H__
#define __BFS_H__

#include "types.hpp"
#include "fog_engine.hpp"
#include "limits.h"

struct bfs_vert_attr{
	u32_t bfs_level;
};

template <typename T>
class bfs_program{
	public:
        static u32_t num_tasks_to_sched;
		static u32_t bfs_root;
        static int forward_backward_phase;
        static int CONTEXT_PHASE;
        static int loop_counter;
        static bool init_sched;
        static bool set_forward_backward;
		//init the vid-th vertex
		static void init(u32_t vid, bfs_vert_attr* va, index_vert_array<T> * vert_index){
			if ( vid == bfs_root){
				va->bfs_level = 0;
                //PRINT_DEBUG("VID = %d\n", vid);
				fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T>::add_schedule( vid, 
                        CONTEXT_PHASE /*phase:decide which buf to read and write */
                        );
			}
            else
            { 
				va->bfs_level = UINT_MAX;
            }
		}

		//scatter updates at vid-th vertex 
        /*
         * params:
         * 1.attribute
         * 2.edge(type1, type2, in_edge)
         * 3.update_src
         */
		static update<bfs_vert_attr> *scatter_one_edge(
                bfs_vert_attr * this_vert,
                T * this_edge,
                u32_t update_src)
        {
            assert(forward_backward_phase == FORWARD_TRAVERSAL);
            update<bfs_vert_attr> *ret;
            u32_t scatter_value = this_vert->bfs_level + 1;
            ret = new update<bfs_vert_attr>; 
            ret->dest_vert = this_edge->get_dest_value();
            ret->vert_attr.bfs_level = scatter_value;
            return ret;
		}

		//gather one update "u" from outside
		static void gather_one_update( u32_t vid, bfs_vert_attr* this_vert, 
                struct update<bfs_vert_attr>* this_update)
        {
			//compare the value of u, if it is smaller, absorb the update
			if( this_update->vert_attr.bfs_level < this_vert->bfs_level){
				*this_vert = this_update->vert_attr;
				//should add schedule of {vid,0}, need api from engine
				fog_engine<bfs_program<T>, bfs_vert_attr, bfs_vert_attr, T>::add_schedule( vid, CONTEXT_PHASE);
			}
		}

        static void before_iteration()
        {
            PRINT_DEBUG("BFS engine is running for the %d iteration, there are %d tasks to schedule!\n",
                    loop_counter, num_tasks_to_sched);
        }
        static int after_iteration()
        {
            PRINT_DEBUG("BFS engine has finished the %d iteration, there are %d tasks to schedule at next iteration!\n",
                    loop_counter, num_tasks_to_sched);
            if (num_tasks_to_sched == 0)
                return ITERATION_STOP;
            else
                return ITERATION_CONTINUE;
        }
        static int finalize()
        {
            return ENGINE_STOP;
        }

        static void print_result(u32_t vid, bfs_vert_attr * va)
        {
            PRINT_DEBUG("BFS:result[%d], bfs_level = %d\n", vid, va->bfs_level);
        }
};

template <typename T>
unsigned int bfs_program<T>::num_tasks_to_sched = 0;

template <typename T>
unsigned int bfs_program<T>::bfs_root = 0;

template <typename T>
int bfs_program<T>::forward_backward_phase = FORWARD_TRAVERSAL;

template <typename T>
int bfs_program<T>::CONTEXT_PHASE = 0;

template <typename T>
int bfs_program<T>::loop_counter = 0;

template <typename T>
bool bfs_program<T>::set_forward_backward = false;

template <typename T>
bool bfs_program<T>::init_sched = false;
//if you want add_schedule when init(), please set this to be true~!

#endif
