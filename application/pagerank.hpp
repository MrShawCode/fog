/**************************************************************************************************
 * Authors: 
 *   Zhiyuan Shao, Jian He
 *
 * Routines:
 *   Implements PageRank algorithm
 *
 * IMPORTANT: The executions of the core functions (init, scatter_one_edge, 
 *   gather_on_update) are in PARALLEL during execution. Updates made by these functions
 *   to global variables (i.e., static variables, member variables of your algorithm 
 *   class) will result in RACE CONDITION, and may produce unexpected results. 
 *   Therefore, program with CARE!
 *************************************************************************************************/

#ifndef __PAGERANK_HPP__
#define __PAGERANK_HPP__

#include "types.hpp"
#include "fog_engine.hpp"
#include "print_debug.hpp"

#define DAMPING_FACTOR	0.85

//this structure will define the "attribute" of one vertex, the only member will be the rank 
// value of the vertex
struct pagerank_vert_attr{
	float rank;
};

template <typename T>
class pagerank_program{
	public:
        static u32_t num_tasks_to_sched;
		static u32_t iteration_times;	//how many iterations will there be?
        static u32_t reduce_iters;
        static int forward_backward_phase;
        static int CONTEXT_PHASE;
        static int loop_counter;
        static bool init_sched;
        static bool set_forward_backward;

		//initialize each vertex of the graph
		static void init( u32_t vid, pagerank_vert_attr* this_vert, index_vert_array<T> * vert_index )
		{
			this_vert->rank = 1.0;
		}

		//This member function is defined to process one of the out edges of vertex "vid".
		//Explain the parameters:
		// vid: the id of the vertex to be scattered.
		// this_vert: point to the attribute of vertex to be scattered.
		// num_edge: the number of out edges of the vertex to be scattered.
		// this_edge: the edge to be scattered this time. 
		// result_update: the result update
		//Notes: 
		// 1) this member fuction will be used to scatter ONE edge of a vertex.
		// 2) the return value will be a pointer to the generated update.
		//	However, it is possible that no update will be generated at all! 
		//	In that case, this member function should return NULL.
		// 3) This function should be "re-enterable", therefore, no global variables
		//	should be visited, or visited very carefully.
		static void scatter_one_edge(
                    pagerank_vert_attr* this_vert, 
					T &this_edge, // type1 or type2 , only available for FORWARD_TRAVERSAL
					u32_t num_edges,
                    update<pagerank_vert_attr> &result_update) 
		{
            if (forward_backward_phase == BACKWARD_TRAVERSAL)
                PRINT_ERROR("forward_backward_phase must set to FORWARD_TRAVERSAL\n");
			//update<pagerank_vert_attr> * ret;
			//ret = new update<pagerank_vert_attr>;
            result_update.dest_vert = this_edge.get_dest_value();

            assert(forward_backward_phase == FORWARD_TRAVERSAL);
            float scatter_weight = DAMPING_FACTOR *(this_vert->rank/num_edges) + (1- DAMPING_FACTOR);
            result_update.vert_attr.rank = scatter_weight;
		}
        /*
		static update<pagerank_vert_attr>* scatter_one_edge(
                    pagerank_vert_attr* this_vert, 
					T * this_edge, // type1 or type2 , only available for FORWARD_TRAVERSAL
					u32_t num_edges) 
		{
            if (forward_backward_phase == BACKWARD_TRAVERSAL)
                PRINT_ERROR("forward_backward_phase must set to FORWARD_TRAVERSAL\n");
			update<pagerank_vert_attr> * ret;
			ret = new update<pagerank_vert_attr>;
            ret->dest_vert = this_edge->get_dest_value();

            assert(forward_backward_phase == FORWARD_TRAVERSAL);
            float scatter_weight = DAMPING_FACTOR *(this_vert->rank/num_edges) + (1- DAMPING_FACTOR);
            ret->vert_attr.rank = scatter_weight;
			return ret;
		}
        */
       

		// Gather one update. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// va: the attribute of destination vertex;
		// u: the update from the "update" buffer.
		static void gather_one_update( u32_t vid, pagerank_vert_attr * dest_vert_attr, update<pagerank_vert_attr> * u )
		{
			assert( vid == u->dest_vert );
			dest_vert_attr->rank += u->vert_attr.rank;
		}

        static void before_iteration()
        {
            PRINT_DEBUG("PageRank engine is running for the %d-th iteration, there are %d tasks to schedule!\n",
                    loop_counter, num_tasks_to_sched);
        }
        static int after_iteration()
        {
            reduce_iters = iteration_times - loop_counter;
            PRINT_DEBUG("Pagerank engine has finished the %d-th iteration!\n", loop_counter);
            if (reduce_iters == 0)
                return ITERATION_STOP;
            return ITERATION_CONTINUE;
        }
        static int finalize(pagerank_vert_attr * va)
        {
            for (unsigned int id = 0; id < 100; id++)
                PRINT_DEBUG_LOG("Pagerank:result[%d], rank = %f\n", id, (va+id)->rank);

            PRINT_DEBUG("Pagerank engine stops!\n");
            return ENGINE_STOP;
        }
};

template <typename T>
bool pagerank_program<T>::set_forward_backward = false;

template <typename T>
u32_t pagerank_program<T>::num_tasks_to_sched = 0;

template <typename T>
u32_t pagerank_program<T>::iteration_times = 0;

template <typename T>
u32_t pagerank_program<T>::reduce_iters = 0;

template <typename T>
int pagerank_program<T>::forward_backward_phase = FORWARD_TRAVERSAL;

template <typename T>
int pagerank_program<T>::CONTEXT_PHASE = 0;

template <typename T>
int pagerank_program<T>::loop_counter = 0;

template <typename T>
bool pagerank_program<T>::init_sched = false;
//if you want add_schedule when init(), please set this to be true~!
#endif
