#ifndef __PAGERANK_HPP__
#define __PAGERANK_HPP__

#define DAMPING_FACTOR	0.85

//this structure will define the "attribute" of one vertex, the only member will be the rank 
// value of the vertex
struct pagerank_vertex_attr{
	float rank;
};

class pagerank_program{
	public:
		static u32_t iteration_times;	//how many iterations will there be?

		//initialize each vertex of the graph
		static void init( u32_t vid, pagerank_vertex_attr* this_vert )
		{
			this_vert->rank = 1.0;
		}

		//scatter the updates according to the out-edges of vertex "vid"
		static void scatter( u32_t vid, pagerank_vertex_attr* this_vert )
		{
			// attr_array[vid]->rank will retrieve the latest rank value of vertex vid
			// vert_index_array->num_out_edges(vid) will return the number of out 
			// edges of vertex vid
			// config.max_vertex_id+1 denotes the total number of vertices of the graph
			int num_of_out_edges = fog_engine<pagerank_program, pagerank_vertex_attr>::num_of_out_edges( vid );

			int scatter_weight = DAMPING_FACTOR*(this_vert->rank / num_of_out_edges) +
				1 - DAMPING_FACTOR;

			struct edge *t_edge;
			struct update<pagerank_vertex_attr> t_update;

			for( int i=0; i<num_of_out_edges; i++){
				t_edge = fog_engine<pagerank_program, pagerank_vertex_attr>::get_ith_out_edge( vid, i );

				t_update.dest_vert = t_edge->dest_vert;
				t_update.vert_attr.rank = scatter_weight;
				// will add the update to buffer
				fog_engine<pagerank_program, pagerank_vertex_attr>::add_update( &t_update );
			}
		}

		// Gather one update. Explain the parameters:
		// vid: the vertex id of destination vertex;
		// va: the attribute of destination vertex;
		// u: the update from the "update" buffer.
		static void gather_one_update( u32_t vid, pagerank_vertex_attr * dest_vert_attr, update<pagerank_vertex_attr> * u )
		{
			assert( vid == u->dest_vertex );
			dest_vert_attr->rank += u->vert_attr.rank;
		}
};

u32_t pagerank_program::iteration_times = 0;

#endif
