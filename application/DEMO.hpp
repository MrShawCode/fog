/**************************************************************************************************
 * Authors: 
 *   Jian He, Zhiyuan Shao
 *
 * Routines:
 *   This is an example graph algorithm
 *
 * Notes:
 *   This file will tell you how to use FOGP to write a demo algorithm, if you want to 
 *   write a new application, please copy this file to your applicaiton's name.
 *
 *   After you finish this header file, you need to implement the algorithm (i.e., place
 *   your code) in cooresponding section of fogsrc/main.cpp.
 *
 *   IMPORTANT: The executions of the core functions (init, scatter_one_edge, 
 *   gather_on_update) are in PARALLEL during execution. Updates made by these functions
 *   to global variables (i.e., static variables, member variables of your algorithm 
 *   class) will result in RACE CONDITION, and may produce unexpected results. 
 *   Therefore, program with CARE!
 *************************************************************************************************/

#ifndef __DEMO_HPP__
#define __DEMO_HPP__

/*
 * These three hpp files are not necessary for your application. The file fog_engine.hpp defines
 * some static function like add_schedule, and your application can use those functions to do 
 * what you want. The file print_debug.hpp defines a function to print the result of the algorithm,
 * you can use it in print_result().
 */
#include "types.hpp"
#include "fog_engine.hpp"
#include "print_debug.hpp"

//this structure will define the "attribute" of one vertex, the only member will be the rank 
// value of the vertex
struct demo_vert_attr{
    float xxx;
    int xxx;
    //Or other things
};

/*
 * The structure of UPDATE is the same as the structure of demo_vert_attr.
 * But if You want to define the UPDATE structure like the algorithm of SCC(see scc.hpp),
 * Please define the same as demo_vert_attr but changing some different name.
 */
 struct demo_update
 {
      xxx;
      xxx;
 };

//the typename T is the edge file-type(type1 or type2)
template <typename T>
class demo_program{
	public:
        /*
         * These variablies below must be set, beacuse the FOGP engine will use them
         * to control the algorithm.
         */
        static u32_t num_tasks_to_sched;
		static u32_t iteration_times;	
        static u32_t reduce_iters;
        static int forward_backward_phase;
        static int CONTEXT_PHASE;
        static int loop_counter;
        static bool init_sched;
        static bool set_forward_backward;

		//initialize each vertex of the graph
		static void init( u32_t vid, demo_vert_attr* this_vert, index_vert_array<T> * vert_index )
		{
            //define your application's init() function.
		}

		//Notes: 
		// 1) this member fuction will be used to scatter ONE edge of a vertex.
		// 2) the return value will be a pointer to the generated update.
		//	However, it is possible that no update will be generated at all! 
		//	In that case, this member function should return NULL.
		// 3) This function should be "re-enterable", therefore, no global variables
		//	should be visited, or visited very carefully.
		static void scatter_one_edge(
                    demo_vert_attr* this_vert, 
					T &this_edge, // type1 or type2 , only available for FORWARD_TRAVERSAL
					u32_t PARAMETER_BY_YOURSELF,
                    update<demo_update> &this_update) 
            //The PARAMETER_BY_YOURSELF can be set to everything all by youself
		{
			//update<demo_update> * ret;
			//ret = new update<demo_update>;

            //some operations to ret

			//return ret;
		}
        /*
		static update<demo_update>* scatter_one_edge(
                    demo_vert_attr* this_vert, 
					T * this_edge, // type1 or type2 , only available for FORWARD_TRAVERSAL
					u32_t PARAMETER_BY_YOURSELF) 
            //The PARAMETER_BY_YOURSELF can be set to everything all by youself
		{
			update<demo_update> * ret;
			ret = new update<demo_update>;

            //some operations to ret

			return ret;
		}
        */

		// Gather one update. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// va: the attribute of destination vertex;
		// u: the update from the "update" buffer.
		static void gather_one_update( u32_t vid, demo_vert_attr * dest_vert_attr, update<demo_update> * u )
		{
            //This assert is set for debug
			//assert( vid == u->dest_vert );

            //some operations to dest_vert_attr
		}

        //A function before every iteration
        static void before_iteration()
        {
            /*
             * This function will be used to print some important information about the algorithm.
             */
            PRINT_DEBUG("DEMO engine is running for the %d iteration, there are %d tasks to schedule!\n",
                    loop_counter, num_tasks_to_sched);
        }
        
        //A function before every iteration
        static int after_iteration()
        {
            /*
             * You can reverse the engine to run read IN-EDGE here like cc.hpp.
             * Also, you can count something like pagerank.hpp.
             * In a word, using this function, you can do something after one iteration.
             */
            //return ITERATION_CONTINUE;
            //return ITERATION_STOP;
        }

        //A function at the end.
        static int finalize(demo_vert_attr * va)
        {
            //A function to print all the result
            for (unsigned int id = 0; id < 100; id++)
                PRINT_DEBUG("demo:result[%d], xxx = %f\n", id, (va+id)->xxx);

            //if you don't do some algorithm like scc.hpp, just write this bellow.
            PRINT_DEBUG("DEMO engine stops!\n");
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
