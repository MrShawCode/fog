/**************************************************************************************************
 * Authors: 
 *   lilang
 *
 * Routines:
 *   greedy_coloring:
 *        1. 
 *        2. 
 *        3. 
 *
 * There is some problems for big graph.
 *************************************************************************************************/

#ifndef __GREEDY_COLORING_H__
#define __GREEDY_COLORING_H__

#include "../headers/types.hpp"
#include "../headers/fog_engine.hpp"
#include "../headers/index_vert_array.hpp"
#include "print_debug.hpp"
#include "limits.h"


template <typename T>
class Greedy_coloring{
    public:
        void run()
        {
            index_vert_array<T> * vert_index = new index_vert_array<T>;

            //store all vertices' color
            u32_t *color = new u32_t[gen_config.max_vert_id+1];
            u32_t *color_used = new u32_t[gen_config.max_vert_id+1];//store the label's times of presentation
            u32_t max_color_num = 0;
            
            u32_t tmp_vert_id = gen_config.min_vert_id;
            u32_t max_vert_id = gen_config.max_vert_id;
            u32_t v_num_edges = 0;
            T edge;

            //init the array 
            for(u32_t i=0;i<gen_config.max_vert_id+1;i++)
            {
                color[i]=0;
                color_used[i]=0;
            }

            std::cout << "start handling" << std::endl;
            //init the first vertex
            color[tmp_vert_id] = 1;
            max_color_num = 1;
            tmp_vert_id++;
            while(tmp_vert_id <= max_vert_id){
                v_num_edges = vert_index->num_edges(tmp_vert_id, OUT_EDGE);
                if(v_num_edges == 0)
                    continue;
                for(unsigned int i=0;i<v_num_edges;i++){
                    vert_index->get_out_edge(tmp_vert_id, i, edge);
                    if(color[edge.dest_vert] != 0)
                        color_used[color[edge.dest_vert]]=1;
                }
                
                for(unsigned int i=1;i<max_color_num+1;i++)
                    if(color_used[i]!=1){
                        color[tmp_vert_id]=i;
                        break;
                    }
                //add new color?
                if(color[tmp_vert_id]==0){
                    max_color_num++;
                    color[tmp_vert_id] = max_color_num;
                }
            
                //init
                for(unsigned int i=0;i<v_num_edges;i++){
                    vert_index->get_out_edge(tmp_vert_id, i, edge);
                    color_used[color[edge.dest_vert]] = 0;
                }

                tmp_vert_id++;
            }   


            //statistics
            std::cout << "the total number of colors is: " << max_color_num << std::endl;

            //print the result 
            for(u32_t i=gen_config.min_vert_id;i<gen_config.max_vert_id+1;i++)
                std::cout << i << "\t" << color[i] << std::endl;

            //PRINT_DEBUG("in_deg_sum = %lld\n", in_deg_sum);

        }
};

#endif
