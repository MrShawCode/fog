/**************************************************************************************************
 * Authors: 
 *   lilang
 *
 * Routines:
 *   LPA: 1. give every vertex a unique label
 *        2. update a vertex's label using the label presents most among its neibors
 *           if there are more than one and its label is in the set, don't change, and if not, choose randomly
 *        3. repeat 2, until all vertices' labels don't change.
 *  
 *  this is async.
 *  It is very slow to processing big graph.
 *************************************************************************************************/

#ifndef __LPA_ASYNC_PROGRAM_H__
#define __LPA_ASYNC_PROGRAM_H__

#include "../headers/types.hpp"
#include "../headers/fog_engine.hpp"
#include "../headers/index_vert_array.hpp"
#include "print_debug.hpp"
#include "limits.h"

//#define 

template <typename T>
class LPA_async_program{
    public:
        void run()
        {
            index_vert_array<T> * vert_index = new index_vert_array<T>;
            
            bool convergence = false;
            //store the old label, and the new label
            u32_t *label = new u32_t[gen_config.max_vert_id+1];
            u32_t *label_times = new u32_t[gen_config.max_vert_id+1];//store the label's times of presentation
            u32_t *label_of_same_times = new u32_t[gen_config.max_vert_id+1];//store the labels present most 
            u32_t niters = 0;
             
            //init vertex's label(equal to its id)
            for(u32_t i=0;i<gen_config.max_vert_id+1;i++)
            {
                label[i]=i;
                label_times[i]=0;
                label_of_same_times[i]=0;
            }

            std::cout << "start handling" << std::endl;
            while(convergence == false){
                for(u32_t i=gen_config.min_vert_id;i<gen_config.max_vert_id+1;i++)
                    std::cout << i << "\t" << label[i] << std::endl;

                niters++;
                convergence = true;
                
                u32_t tmp_vert_id = gen_config.min_vert_id;
                u32_t max_vert_id = gen_config.max_vert_id;
                u32_t v_num_edges = 0;
                u32_t label_most = -1;
                T edge;

                while(tmp_vert_id <= max_vert_id){
                    v_num_edges = vert_index->num_edges(tmp_vert_id, OUT_EDGE);
                    if(v_num_edges == 0)
                        continue;
                    u32_t times = 0;
                    u32_t tmp_times;
                    for(unsigned int i=0;i<v_num_edges;i++){
                        vert_index->get_out_edge(tmp_vert_id, i, edge);
                        label_times[label[edge.dest_vert]]++;
                    }

                    //find the label presents most
                    u32_t key = 0;
                    u32_t neibor_label;
                    for(unsigned int i=0;i<v_num_edges;i++){
                        vert_index->get_out_edge(tmp_vert_id, i, edge);
                        neibor_label = label[edge.dest_vert];
                        tmp_times = label_times[neibor_label];
                        if(times < tmp_times){
                            times = tmp_times;
                            key = 0;
                            label_of_same_times[key] = neibor_label;
                        }else if(times == tmp_times){
                            bool go_to_next = false;
                            for(unsigned int j=0;j<=key;j++)
                                if(neibor_label == label_of_same_times[key])
                                    go_to_next = true;
                            if(go_to_next == false){
                                key++;
                                label_of_same_times[key] = neibor_label;
                            }
                        }
                        //if the times is equal, randomly choose
                    }

                    //take its label into consideration
                    bool update = true;
                    for(unsigned int k=0;k<=key;k++)
                        if(label[tmp_vert_id] == label_of_same_times[k]){
                            label_most = label[tmp_vert_id];
                            update = false;
                            break;
                        }
                    if(update == true){
                        srand( (unsigned)time(NULL) - niters);
                        u32_t random = rand()%(key+1);
                        label_most = label_of_same_times[random];
                    }

                    //init
                    for(unsigned int i=0;i<v_num_edges;i++){
                        vert_index->get_out_edge(tmp_vert_id, i, edge);
                        label_times[label[edge.dest_vert]] = 0;
                    }

                    if(label[tmp_vert_id] != label_most){
                        label[tmp_vert_id] = label_most;
                        convergence = false;
                    }else{
                        label[tmp_vert_id] = label[tmp_vert_id];
                    }

                    tmp_vert_id++;
                }   
            }

            //statistics
            u32_t label_s[gen_config.max_vert_id+1];
            int count = 0;
            memset(label_s, -1, (gen_config.max_vert_id+1) * sizeof(int));
            for(u32_t i=gen_config.min_vert_id;i<gen_config.max_vert_id+1;i++){
                bool plus=true;
                for(int k=0;k<=count;k++)
                    if(label[i]==label_s[k])
                        plus = false;
                label_s[count] = label[i];
                if(plus==true)
                    count++;
            }
            std::cout << "the total number of labels is: " << count << std::endl;
            std::cout << "the iterations is: " << niters << std::endl;

            //print the result 
            for(u32_t i=gen_config.min_vert_id;i<gen_config.max_vert_id+1;i++)
                std::cout << i << "\t" << label[i] << std::endl;

            //PRINT_DEBUG("in_deg_sum = %lld\n", in_deg_sum);

        }
};

#endif
