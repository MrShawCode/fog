/**************************************************************************************************
 * Authors: 
 *   Jian He,
 *
 * Routines:
 *   Radix sort to help the construction of in-edge files, this method is borrowed from GraphChi.
 *   
 *************************************************************************************************/

#include <iostream>
#include <sstream>
#include <cassert>

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "convert.h"
using namespace convert;

#define MAX_RADIX 8
#define BUCKETS (1 << MAX_RADIX)

typedef unsigned char u8_t;
typedef unsigned long long u64_t;

void radix_step(struct tmp_in_edge * buf_1, struct tmp_in_edge * buf_2,
        u8_t * tmp, u64_t * counts, u64_t num_edges, u64_t rbits, u64_t bits_offset)
{
    u64_t left_move = 1L << rbits;
    u64_t radix_value;
    for (u64_t i = 0; i < left_move; i++)
    {
        counts[i] = 0;
    }
    for (u64_t j = 0; j < num_edges; j++)
    {
        radix_value = ((1 << rbits) - 1) & (((*(buf_1+j)).dest_vert)>>bits_offset);
        tmp[j] = radix_value;
        u64_t k = radix_value;
        counts[k]++;
    }
    u64_t s = 0;
    for (u64_t i = 0; i < left_move; i++)
    {
        s += counts[i];
        counts[i] = s;
    }

    for (u64_t j = num_edges-1; (int)j >= (int)0; j--)
    {
        u64_t x = --counts[tmp[j]];
        (*(buf_2+x)).src_vert = (*(buf_1+j)).src_vert;
        (*(buf_2+x)).dest_vert = (*(buf_1+j)).dest_vert;
    }
    //std::cout << "end radix_step" << std::endl;
}

template <class T>
u64_t log2up(T i)
{
    u64_t a = 0;
    while ((1L << a) <= i) a++;
    return a;
}

void radix_sort(struct tmp_in_edge * buf_1, struct tmp_in_edge * buf_2, 
        u64_t num_edges, unsigned int max_vert_id)
{
    u64_t bits = log2up(max_vert_id);
    u8_t *tmp = (u8_t *)malloc(sizeof(u8_t)*num_edges);
    u64_t *counts = (u64_t *)malloc(sizeof(u64_t)*BUCKETS);

    u64_t rounds = 1 + (bits-1)/MAX_RADIX;
    u64_t rbits = 1 + (bits-1)/rounds;
    u64_t bit_offset = 0;
    bool flipped = 0;


    while (bit_offset < bits)
    {
        if (bit_offset + rbits > bits) rbits = bits - bit_offset;
        //std::cout << "bit_offset = " << bit_offset << std::endl;
        //std::cout << "bits = " << bits << std::endl;
        //std::cout << "rbits = " << rbits << std::endl;
        //std::cout << "rounds = " << rounds << std::endl;

        if (flipped)
            radix_step(buf_2, buf_1, tmp, counts, num_edges, rbits, bit_offset);
        else
            radix_step(buf_1, buf_2, tmp, counts, num_edges, rbits, bit_offset);

        bit_offset += rbits;
        flipped = !flipped;
    }
    //std::cout << "end here!" << std::endl;
    if (flipped)
        for (u64_t i = 0; i < num_edges; i++)
        {
            (*(buf_1+i)).src_vert = (*(buf_2+i)).src_vert;
            (*(buf_1+i)).dest_vert = (*(buf_2+i)).dest_vert;
        }

    free(tmp);
    free(counts);
}
