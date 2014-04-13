#ifndef __BITMAP_H__
#define __BITMAP_H__

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <cassert>
#include "config.hpp"
#include "print_debug.hpp"

typedef unsigned int u32_t;
typedef unsigned long long u64_t;

#define bit_num_bytes sizeof(u32_t)
#define BITS_SHIFT 5
#define BITS_MASK 0x1f

typedef u32_t * bitmap_t;

class bitmap
{
    private:
        u32_t max_size;
        u32_t bits_true_size;
        u32_t buf_size;
        bitmap_t bits_array;

    public:
        bitmap(u32_t buf_size_in, u32_t max_size_in, bitmap_t bitmap_buffer_head, u32_t mode_t);         
        ~bitmap();
        void set_value(u32_t index); 
        void clear_value(u32_t index);
        u32_t get_value(u32_t index);
        u32_t get_bits_true_size();
        void print_binary(u32_t start, u32_t stop);

};

bitmap::bitmap(u32_t buf_size_in, u32_t max_size_in, bitmap_t bitmap_buffer_head, u32_t mode_t)
                :max_size(max_size_in),  buf_size(buf_size_in), bits_array(bitmap_buffer_head)
{
    //bits_array = (bitmap_t)malloc(max_size/bit_num_bytes + 1);
    //if (!bits_array)
    //{
     //   PRINT_ERROR("No enough memory!\n");
      //  exit(-1);
    //}
    //PRINT_DEBUG("the bitmap_buf_size is : %d\n", buf_size);
    //PRINT_DEBUG("the bits size is %d\n", max_size);
    memset(bits_array, 0, max_size/bit_num_bytes + 1);
    if (mode_t == 0)// means write a bitmap to a file
        bits_true_size = 0;
}
bitmap::~bitmap()
{
    //free(bits_array);
}
void bitmap::set_value(u32_t index)
{
    assert(index <= max_size);            
    bits_array[index >> BITS_SHIFT] |= 1 << (index & BITS_MASK);
    bits_true_size++;
    PRINT_DEBUG("size:%d\n", bits_true_size);
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

void bitmap::print_binary(u32_t start, u32_t stop)
{
    for(u32_t j = start; j <= stop; j++)
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
