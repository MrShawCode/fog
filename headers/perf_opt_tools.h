/**************************************************************************************************
 * Authors: 
 *   Huiming Lv 
 *
 * Declaration:
 *   Performance analyze tools.
 *
 * Notes:
 *   1.Created by Huiming Lv   2016/11/29 
 *************************************************************************************************/

#ifndef __PERF_OPT_TOOLS_H__
#define __PERF_OPT_TOOLS_H__

#include "print_debug.hpp"

extern FILE **trace_file_array;

extern u64_t out_index_array_head;
extern u64_t out_edge_array_head;
extern u64_t in_index_array_head;
extern u64_t in_edge_array_head;
extern u64_t attr_array_head;
extern u64_t meta_data_head;

#define CACHE_LINE_SIZE 64

static inline void print_trace_log(unsigned cpu_id, unsigned long long tsc, unsigned which_area, char R_or_W, unsigned long long addr){
    u64_t block_num = 0;
    switch(which_area){
        case 0:
            block_num = (addr - attr_array_head) >> 6;
            break;
        case 1:
            block_num = (addr - out_index_array_head) >> 6;
            break;
        case 2:
            block_num = (addr - out_edge_array_head) >> 6;
            break;
        case 3:
            block_num = (addr - in_index_array_head) >> 6;
            break;
        case 4:
            block_num = (addr - in_edge_array_head) >> 6;
            break;
        case 5:
            block_num = (addr - meta_data_head) >> 6;
            break;
        default:
            PRINT_ERROR("print trace log error, unknown area id:,%d\n", which_area);
            exit(-1);
            break;
    }
    //fprintf(trace_file_array[cpu_id], "%lld\t%d\t%c\t0x%llx\n", tsc, which_area, R_or_W, addr);
    fprintf(trace_file_array[cpu_id], "%lld\t%d\t%c\t%lld\n", tsc, which_area, R_or_W, block_num);
}

static inline unsigned long long get_rdtsc(){
    unsigned hi, lo;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long)lo) | (((unsigned long long)hi)<<32);
}


#endif
