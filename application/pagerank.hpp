#ifndef __PAGERANK_HPP__
#define __PAGERANK_HPP__

#define DAMPING_FACTOR	0.85

//this structure will define the "attribute" of one vertex, the only member will be the rank 
// value of the vertex
struct pagerank_vert_attr{
	float rank;
};

class pagerank_program{
	public:
		static u32_t iteration_times;	//how many iterations will there be?
        static u32_t reduce_iters;

		//initialize each vertex of the graph
		static void init( u32_t vid, pagerank_vert_attr* this_vert )
		{
			this_vert->rank = 1.0;
		}

		static void init(u32_t vid, pagerank_vert_attr* va, u32_t PHASE, u32_t init_forward_backward_phase, u32_t loop_counter,
            index_vert_array * vert_index){}

		static void init(u32_t vid, pagerank_vert_attr* va, u32_t PHASE){}

		//This member function is defined to process one of the out edges of vertex "vid".
		//Explain the parameters:
		// vid: the id of the vertex to be scattered.
		// this_vert: point to the attribute of vertex to be scattered.
		// num_outedge: the number of out edges of the vertex to be scattered.
		// this_edge: the edge to be scattered this time. 
		//Notes: 
		// 1) this member fuction will be used to scatter ONE edge of a vertex.
		// 2) the return value will be a pointer to the generated update.
		//	However, it is possible that no update will be generated at all! 
		//	In that case, this member function should return NULL.
		// 3) This function should be "re-enterable", therefore, no global variables
		//	should be visited, or visited very carefully.
		static update<pagerank_vert_attr>* scatter_one_edge( u32_t vid, 
					pagerank_vert_attr* this_vert, 
					u32_t num_outedge, 
					edge* this_edge )
		{
			update<pagerank_vert_attr> * ret;
			float scatter_weight = DAMPING_FACTOR *(this_vert->rank/num_outedge) + (1- DAMPING_FACTOR);
			ret = new update<pagerank_vert_attr>;
			ret->dest_vert = this_edge->dest_vert;
			ret->vert_attr.rank = scatter_weight;
			return ret;
		}

		static update<pagerank_vert_attr> *scatter_one_edge(u32_t vid,
        pagerank_vert_attr * this_vert,
        edge * this_edge){return NULL;}

		// Gather one update. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// va: the attribute of destination vertex;
		// u: the update from the "update" buffer.
		static void gather_one_update( u32_t vid, pagerank_vert_attr * dest_vert_attr, update<pagerank_vert_attr> * u )
		{
			assert( vid == u->dest_vert );
			dest_vert_attr->rank += u->vert_attr.rank;
		}

		static void gather_one_update( u32_t vid, pagerank_vert_attr* this_vert, 
                struct update<pagerank_vert_attr>* this_update, 
                u32_t PHASE){}
		static void set_finish_to_vert(u32_t vid, pagerank_vert_attr * this_vert){}
        //special realization for pagerank
		static bool judge_true_false(pagerank_vert_attr* va)
        {
            reduce_iters++;
            if (reduce_iters < iteration_times)
            {
                PRINT_DEBUG("iters = %d\n", reduce_iters);
                return false;
            }
            assert(reduce_iters == iteration_times);
            return true;
        }
		static bool judge_src_dest(pagerank_vert_attr *va_src, pagerank_vert_attr *va_dst, float edge_weight){return false;}
        static void print_result(u32_t vid, pagerank_vert_attr * va)
        {
            PRINT_DEBUG("Pagerank:result[%d], rank = %f\n", vid, va->rank);
        }
};

u32_t pagerank_program::iteration_times = 0;
u32_t pagerank_program::reduce_iters = 0;

#endif
