#ifndef __TYPES_H__
#define __TYPES_H__

struct edge  
{
    unsigned int dst_vert;                                         
    float edge_weight;
}__attribute__ ((aligned(8)));                                     

struct vertex_index
{
    unsigned long long  offset;                                    
}__attribute__ ((aligned(8)));   

#endif

