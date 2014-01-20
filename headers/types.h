#ifndef __TYPES_H__
#define __TYPES_H__

typedef struct edge_t{
	unsigned int dest_vid;
	float	weight;
} edge;

typedef struct vertex_t{
	unsigned long out_edge_offset;

	union{
	  struct sssp_t{
		unsigned int predecessor;
		unsigned int path_len;
	  }sssp;

	  struct pagerank_t{
		float rank;
	  }pagerank;
	}u;
} vertex;

#endif

