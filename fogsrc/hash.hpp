/**************************************************************************************************
 * Authors: 
 *   Huiming Lv
 *
 * Routines:
 *   Implements finding 1-step out-neighbors from a given query vertex ID 
 *   
 *************************************************************************************************/

#ifndef __HASH_PROGRAM_H__
#define __HASH_PROGRAM_H__

#include "types.hpp"
#include "fog_engine.hpp"
#include "../headers/index_vert_array.hpp"
#include "print_debug.hpp"
#include "limits.h"

double cal_cv(u64_t * start, u64_t * end);
template <typename T>
class Hash_program{
	public:
        void run()
        {
            index_vert_array<T> * vert_index = new index_vert_array<T>;

            int partition_num = 0; 
            u64_t * in_deg_array = new u64_t[gen_config.num_processors];
            memset(in_deg_array, 0, gen_config.num_processors*sizeof(u64_t));
            u64_t * out_deg_array = new u64_t[gen_config.num_processors];
            memset(out_deg_array, 0, gen_config.num_processors*sizeof(u64_t));
            for (u32_t i = gen_config.min_vert_id; i <= gen_config.max_vert_id; i++)
            {
                partition_num = i % gen_config.num_processors;
                in_deg_array[partition_num] += vert_index->num_edges(i, IN_EDGE); 
                out_deg_array[partition_num] += vert_index->num_edges(i, OUT_EDGE);
            }
            u64_t in_deg_sum = 0;
            u64_t out_deg_sum = 0;
           
            for (u32_t j = 0; j < gen_config.num_processors; j++)
            {
                PRINT_DEBUG("Partition %d, in_deg = %lld, out_deg = %lld\n", j, in_deg_array[j], out_deg_array[j]);
                in_deg_sum += in_deg_array[j];
                out_deg_sum += out_deg_array[j];
            }
            assert(in_deg_sum==gen_config.num_edges);
            assert(out_deg_sum==gen_config.num_edges);
            //PRINT_DEBUG("in_deg_sum = %lld\n", in_deg_sum);
            //PRINT_DEBUG("out_deg_sum = %lld\n", out_deg_sum);

            double in_deg_cv = cal_cv(in_deg_array, in_deg_array+gen_config.num_processors);
            double out_deg_cv = cal_cv(out_deg_array, out_deg_array+gen_config.num_processors);

            //PRINT_DEBUG_LOG("In degree cv is: %f ; Out degree cv is: %f\n", in_deg_cv, out_deg_cv);
            PRINT_DEBUG("In degree cv is: %.3f%% ; Out degree cv is: %.3f%%\n", in_deg_cv*100, out_deg_cv*100);


        }
};
double cal_cv(u64_t * start, u64_t * end)
{
    double average = 0.0;
    double stan_dev = 0.0;
    double cv = 0.0;
    double temp = 0.0;
	for (const u64_t * ptr = start; ptr != end; ptr++)
	{
	    average += static_cast<double>(*ptr);
	}

    average /= gen_config.num_processors;
    for (const u64_t * ptr = start; ptr != end; ptr++)
    {
        temp = average - static_cast<double>(*ptr);
        stan_dev += pow(temp, 2);
    }
    stan_dev /= gen_config.num_processors;
    stan_dev = sqrt(stan_dev);
    cv = stan_dev/average;
    return cv;
}

#endif
