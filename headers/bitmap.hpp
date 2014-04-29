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

#define VID_TO_BITMAP_INDEX(_VID, _CAP) \
    (_VID%_CAP)


typedef u32_t * bitmap_t;

class bitmap
{
    private:
        u32_t max_size;
        u32_t bits_true_size;
        u32_t buf_size;
        u32_t start_vert, term_vert;
        u32_t max_vert, min_vert;
        u32_t mode_t;
        bitmap_t bits_array;
        bitmap_t buf_head;

    public:
        bitmap(u32_t max_size_in, u32_t buf_size_in, u32_t start_vert_in, u32_t term_vert_in, u32_t mode_t, bitmap_t bitmap_buffer_head);
        ~bitmap();
        void set_value(u32_t index); 
        void clear_value(u32_t index);
        u32_t get_value(u32_t index);
        u32_t get_bits_true_size();
        u32_t get_max_vert();
        u32_t get_min_vert();
        void print_binary(u32_t start, u32_t stop);

};

bitmap::bitmap(u32_t max_size_in, u32_t buf_size_in, u32_t start_vert_in, u32_t term_vert_in, u32_t mode_t, bitmap_t bitmap_buffer_head)
                :max_size(max_size_in), buf_size(buf_size_in), 
                start_vert(start_vert_in),term_vert(term_vert_in),
                mode_t(mode_t),
                bits_array(bitmap_buffer_head + 3*sizeof(u32_t)), 
                buf_head(bitmap_buffer_head)
{
    //PRINT_DEBUG("ADDRESS: BITMAP_BUFFER_HEAD\t TRUE_SIZE\tMIN_VALUE\tMAX_VALUE\tBITS_ARRAY\t\n");
    //PRINT_DEBUG("ADDRESS: 0x%llx\t 0x%llx\t0x%llx\t0x%llx\t0x%llx\n",
      //      (u64_t)(bitmap_buffer_head), (u64_t)(buf_head), (u64_t)(buf_head+1), (u64_t)(buf_head+2), (u64_t)(buf_head+3));
    if (mode_t == 0)// means write a bitmap to a file
    {
        max_vert = start_vert;
        min_vert = term_vert;
    }
        //memset(buf_head, 0, max_size/bit_num_bytes + 1 + sizeof(u32_t));
    else
    {
        bits_true_size = (u32_t )*buf_head;
        min_vert = (u32_t)*(buf_head + 1);
        max_vert = (u32_t)*(buf_head + 2);
        //memset(bits_array, 0, max_size/bit_num_bytes+1);
       // PRINT_DEBUG("bits_true_size = %d\n", (u32_t)*buf_head);
       // PRINT_DEBUG("MAX_READ = %d, MIN_READ = %d\n", max_vert, min_vert);
    }
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
    assert(bits_true_size <= max_size);
    *buf_head = bits_true_size;
    //PRINT_DEBUG("size:%d\n", *buf_head);
    //PRINT_DEBUG("max = %d, min = %d\n", max_vert, min_vert);

    if (index < min_vert)
        min_vert = index;

    if (index > max_vert)
        max_vert = index;

    //PRINT_DEBUG("max = %d, min = %d\n", max_vert, min_vert);
    bitmap_t  max_vert_buf = (buf_head + 2);
    bitmap_t  min_vert_buf = (buf_head + 1);
    if (mode_t == 0)
    { 
        *max_vert_buf = max_vert;
        *min_vert_buf = min_vert;
    }
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

u32_t bitmap::get_max_vert()
{
    return max_vert;
}

u32_t bitmap::get_min_vert()
{
    return min_vert;
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
