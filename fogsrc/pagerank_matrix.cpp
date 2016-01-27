/**************************************************************************************************
 * Authors: 
 *   lilang
 *
 * Routines:
 *   pagerank_matrix:
 *        1.use matrix to calculate the pagerank, don't need iteration 
 *        2.http://segmentfault.com/a/1190000000711128#articleHeader0 
 *        3.may not be useful for the transition probability that is nearly to 1 or randomly jumping probability that is nearly to 0
 *        4.cannot process big graph like soc-Livejournal now(2015.10.23)( error: bad_alloc )
 *************************************************************************************************/

#ifndef __PAGERANK_MATRIX_H__
#define __PAGERANK_MATRIX_H__

#include "../headers/types.hpp"
#include "../headers/fog_engine.hpp"
#include "../headers/index_vert_array.hpp"
#include "print_debug.hpp"
#include "limits.h"

#include <eigen3/Eigen/Dense>

using namespace Eigen;
using namespace std;

template <typename T>
class Pagerank_matrix{
    public:
        static u32_t iteration_times;
    public:
        void run()
        {
            index_vert_array<T> * vert_index = new index_vert_array<T>;

            u32_t min_vert_id = gen_config.min_vert_id;
            u32_t max_vert_id = gen_config.max_vert_id;
            u32_t total = max_vert_id - min_vert_id + 1;
            u32_t v_num_edges = 0;
            struct in_edge in_edge;

            float p = 0.85;
            float delta = (1-p)/total;
            
            std::cout << "init matrix" << std::endl;
            MatrixXf m_g(total,total);//the link matrix
            MatrixXf m_d(total,total);//the reciprocal of Cj's out-deg, it is a diagonal matrix
            MatrixXf m_i(total,total);//it is a identity matrix
            MatrixXf m_temp(total,total);
            MatrixXf m_temp_inv(total,total);
            VectorXf v(total);
            float v_sum = 0;

            m_g = MatrixXf::Zero(total,total);
            m_d = MatrixXf::Zero(total,total);
            m_i.setIdentity(total,total);
            m_temp = MatrixXf::Zero(total,total);
            //v = VectorXf::Ones();
            for(u32_t i=0;i<total;i++){
                v(i) = 1;
            }
            //std::cout << "v is " << v << std::endl;

            //init m_g and m_d
            for(u32_t i=0;i<total;i++){
                v_num_edges = vert_index->num_edges(i+min_vert_id, IN_EDGE);
                for(u32_t j=0;j<v_num_edges;j++){
                    vert_index->get_in_edge(i+min_vert_id, j, in_edge);
                    m_g(i,in_edge.src_vert - min_vert_id) = 1;
                }

                v_num_edges = vert_index->num_edges(i+min_vert_id, OUT_EDGE);
                if(v_num_edges != 0)
                    m_d(i,i) = 1.0/v_num_edges;
                else 
                    std::cout << "the " << i+min_vert_id << " vertex's out-degree is zero";
            }
            
            std::cout << "m_i\n" << m_i << std::endl;
            std::cout << "m_g\n" << m_g << std::endl;
            std::cout << "m_d\n" << m_d << std::endl;

            m_temp = m_i - p * m_g * m_d;
            m_temp_inv /= delta;
            m_temp_inv = m_temp.inverse();
            v = m_temp_inv * v;
            //normalizing, ensure the E * v = 1
            for(u32_t i=0;i<total;i++)
                v_sum += v(i);
            v /= v_sum;

            //print the result
            std::cout << "the result is:" << std::endl;
            for(u32_t i=0;i<total;i++){
                std::cout << (i+min_vert_id) << " = " << v(i) << std::endl;
            }
        }
};

template <typename T>
u32_t Pagerank_matrix<T>::iteration_times = 0;
#endif
