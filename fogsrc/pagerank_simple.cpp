/**************************************************************************************************
 * Authors: 
 *   lilang
 *
 * Routines:
 *   pagerank_simple:
 *        1. 
 *        2. 
 *        3. 
 *
 * this is sync
 *************************************************************************************************/

#ifndef __PAGERANK_SIMPLE_H__
#define __PAGERANK_SIMPLE_H__

#include "../headers/types.hpp"
#include "../headers/fog_engine.hpp"
#include "../headers/index_vert_array.hpp"
#include "print_debug.hpp"
#include "limits.h"


template <typename T>
class Pagerank_simple{
    public:
        static u32_t iteration_times;
    public:
        void run()
        {
            index_vert_array<T> * vert_index = new index_vert_array<T>;

            u32_t min_vert_id = gen_config.min_vert_id;
            u32_t max_vert_id = gen_config.max_vert_id;
            u32_t total = max_vert_id - min_vert_id + 1;

            float p = 0.85;
            float delta = (1-p)/total;

            //every page's pagerank value, old and new
            float **pr = new float*[2];
            pr[0] = new float[total];
            pr[1] = new float[total];
            for(u32_t i=0;i<total;i++){
                pr[0][i] = 1.0/total;
            }
            memset(pr[1], 0, (max_vert_id - min_vert_id + 1) * sizeof(float));
            //every page's in-degree
            //u32_t *in_deg = new u32_t[max_vert_id - min_vert_id + 1];
            //every page's out-degree
            u32_t *out_deg = new u32_t[max_vert_id - min_vert_id + 1];

            u32_t reverse = 1;

            //assume there is no vertex that has zero degree
            //init the link matrix
            u32_t tmp_vert_id = min_vert_id;
            u32_t v_num_edges = 0;
            //T in_edge;
            T out_edge;
            while(tmp_vert_id <= max_vert_id){
                //v_num_edges = vert_index->num_edges(tmp_vert_id, IN_EDGE);
                //in_deg[tmp_vert_id - min_vert_id] = v_num_edges;
                v_num_edges = vert_index->num_edges(tmp_vert_id, OUT_EDGE);
                out_deg[tmp_vert_id - min_vert_id] = v_num_edges;

                tmp_vert_id++;
            }   

            //the core of pagerank algorithm
            //this is a push model
            u32_t iters = 0;
            while(iters < iteration_times){
                tmp_vert_id = min_vert_id;
                while(tmp_vert_id <= max_vert_id){
                    if(out_deg[tmp_vert_id-min_vert_id] == 0)//if the page's out-degree is zero, every page shares it
                        for(u32_t i=0;i<max_vert_id - min_vert_id + 1;i++)
                            pr[reverse][i] += pr[1-reverse][tmp_vert_id-min_vert_id]/(max_vert_id - min_vert_id + 1);
                    else//if not, every linked page shares it
                        for(u32_t i=0;i < out_deg[tmp_vert_id-min_vert_id];i++){
                            vert_index->get_out_edge(tmp_vert_id, i, out_edge);
                            pr[reverse][out_edge.dest_vert - min_vert_id] += pr[1-reverse][tmp_vert_id - min_vert_id] / out_deg[tmp_vert_id-min_vert_id];
                        }

                    tmp_vert_id++;
                }
                //take transition probability into consideration
                for(u32_t i=0;i<max_vert_id - min_vert_id + 1;i++)
                    pr[reverse][i] = p * pr[reverse][i] + delta;

                reverse = 1 - reverse;
                iters++;
                //init to zero
                for(u32_t i=0;i<max_vert_id - min_vert_id+1;i++){
                    pr[reverse][i] = 0;
                }

                //print the temp result
                std::cout << "the " << iters << "th iteration:" << std::endl;
                float sum = 0;
                for(u32_t i=0;i<max_vert_id - min_vert_id+1;i++){
                    std::cout << (i+min_vert_id) << " = " << pr[1-reverse][i] << std::endl;
                    sum += pr[1-reverse][i];
                }
                std::cout << "sum is " << sum << std::endl;
            }

            //print the result 
            std::cout << "the result is:" << std::endl;
            for(u32_t i=0;i<max_vert_id - min_vert_id+1;i++){
                std::cout << (i+min_vert_id) << " = " << pr[1-reverse][i] << std::endl;
            }
        }
};

template <typename T>
u32_t Pagerank_simple<T>::iteration_times = 0;
#endif
