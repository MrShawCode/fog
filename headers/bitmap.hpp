#ifndef __BITMAP_H__
#define __BITMAP_H__

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <cassert>
#include "config.hpp"
#include "types.hpp"
#include "print_debug.hpp"

#define bit_num_bytes sizeof(unsigned int)
#define BITS_SHIFT 5
#define BITS_MASK 0x1f

typedef unsigned int * bitmap_t;

class bitmap
{
    private:
        u32_t max_size;
        u32_t bits_true_size;
        bitmap_t bits_array;

    public:
        bitmap(u32_t max_size_in);         
        ~bitmap();
        void set_value(u32_t index); 
        void clear_value(u32_t index);
        u32_t get_value(u32_t index);
        u32_t get_bits_true_size();
        u32_t print_binary();

};

bitmap::bitmap(u32_t max_size_in):max_size(max_size_in)
{
    bits_array = (bitmap_t)malloc(max_size/bit_num_bytes + 1);
    if (!bits_array)
    {
        PRINT_ERROR("No enough memory!\n");
        exit(-1);
    }

    memset(bits_array, 0, max_size/bit_num_bytes + 1);
    bits_true_size = 0;
}
bitmap::~bitmap()
{
    free(bits_array);
}
void bitmap::set_value(u32_t index)
{
    assert(index <= max_size);            
    bits_array[index >> BITS_SHIFT] |= 1 << (index & BITS_MASK);
    bits_true_size++;
    assert(bits_true_size <= max_size);
    /*
     * This code may equal the bellow:
     * int byte_index = index/bit_num_bytes // find the target BYTE
     * int value = 1 << (index%bit_num_bytes) // find the INDEX of the BYTE
     * bits_array[byte_index] = bits_array[byte_index]|value; //store the value
     */
}

void bitmap::clear_value(u32_t index)
{
    assert(index <= max_size);
    bits_array[index >> BITS_SHIFT] &= ~(1 << (index & BITS_MASK));
}

u32_t bitmap::get_value(u32_t index)
{
    return bits_array[index >> BITS_SHIFT] & (1 << (index & BITS_MASK));
}

u32_t bitmap::get_bits_true_size()
{
    return bits_true_size;
}

u32_t bitmap::print_binary()
{
    for(u32_t j = 0; j < max_size; j++)
    {
        if (get_value(j))
        {
            PRINT_SHORT("1");
        }
        else
        {
            PRINT_SHORT("0");
        }
    } 
    PRINT_SHORT("\n");
}
#endif
