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

typedef unsigned char * bitmap_t;

#define bit_num_bytes sizeof(unsigned char)
#define BITS_SHIFT 3
#define BITS_MASK 0x7

#define VID_TO_BITMAP_INDEX(_VID, _PROC_ID, _NUM_PROCS) \
    (_VID - _PROC_ID)/_NUM_PROCS

#define INDEX_TO_VID(_U32_INDEX, _U8_INDEX, _OFFSET, num_proc, proc_id) \
    ((_U32_INDEX * sizeof(u32_t) * 8) + (_U8_INDEX * sizeof(char) * 8) + _OFFSET)*num_proc+ proc_id


class bitmap
{
    private:
        char *bitmap_buf_head;
        char * bits_array;
        u32_t buf_len_bytes;
        u32_t buf_num_bits;
        u32_t bits_true_size;
        u32_t start_vert, term_vert;
        u32_t max_vert, min_vert;
        u32_t start_index, term_index;
        u32_t max_index, min_index; 
        u32_t processor_id, num_processors;
        u32_t edge_num;

    public:
        //new bitmap 
        bitmap(char * bitmap_buf_head_in, u32_t buf_len_bytes_in, u32_t buf_num_bits_in, 
                u32_t start_vert_in, u32_t term_vert_in, u32_t processor_id_in, u32_t num_processors_in);
        ~bitmap();
        void set_value(u32_t index); 
        void clear_value(u32_t index);
        u32_t get_value(u32_t index);
        u32_t get_bits_true_size();
        u32_t ch_vid_to_bitmap_index(u32_t value);
        u32_t get_max_vert();
        u32_t get_min_vert();
        u32_t get_term_vert();
        u32_t get_start_vert();
        u32_t get_index(u32_t index);
        void set_max_value(u32_t index);
        void set_min_value(u32_t index);
        void set_edge_num(u32_t edge_num_in);
        u32_t get_edge_num();
        void print_binary(u32_t start, u32_t stop);
        void reset_value();

};

//bitmap::bitmap(u32_t max_size_in, u32_t buf_size_in, u32_t start_vert_in, u32_t term_vert_in, u32_t mode_t, bitmap_t bitmap_buffer_head)

bitmap::bitmap(char * bitmap_buf_head_in, u32_t buf_len_bytes_in, u32_t buf_num_bits_in, 
        u32_t start_vert_in, u32_t term_vert_in, u32_t processor_id_in, u32_t num_processors_in)
    :bitmap_buf_head(bitmap_buf_head_in),
    bits_array(bitmap_buf_head_in),
    buf_len_bytes(buf_len_bytes_in),
    buf_num_bits(buf_num_bits_in),
    bits_true_size(0), start_vert(start_vert_in), term_vert(term_vert_in), 
    processor_id(processor_id_in), num_processors(num_processors_in)
{
    /*PRINT_DEBUG("Create a new bitmap for buf:0x%llx, the bitmap_buf_head is 0x%llx, the address of bits_array is 0x%llx.\nthe buf_len_bytes is %d,the buf_num_bits is %d, the bits_true_size is %d, the start_Vert is %d, the term_vert is %d\n", 
            (u64_t)bitmap_buf_head_in, (u64_t)bitmap_buf_head, (u64_t)bits_array, buf_len_bytes,
            buf_num_bits, bits_true_size, start_vert, term_vert);*/
    max_vert = start_vert;
    max_index = VID_TO_BITMAP_INDEX(start_vert, processor_id, num_processors);
    min_vert = term_vert;
    min_index = VID_TO_BITMAP_INDEX(term_vert, processor_id, num_processors);
    //PRINT_DEBUG("max_vert = %d, min_vert = %d\n", max_vert, min_vert);
    ///PRINT_DEBUG("max_index= %d, min_index= %d\n", max_index, min_index);
    start_index = 0;
    term_index = buf_num_bits-1;
    //PRINT_DEBUG("start_index = %d, term_index = %d\n", start_index, term_index);
    //PRINT_DEBUG("processor_id = %d\n",processor_id);
}
bitmap::~bitmap()
{
    //free(bits_array);
}
void bitmap::set_value(u32_t value)
{
    u32_t index = ch_vid_to_bitmap_index(value);

    bits_array[index >> BITS_SHIFT] |= 1 << (index & BITS_MASK);
    assert(bits_true_size <= buf_num_bits);
    /*
     * This code may equal the bellow:
     * int byte_index = index/bit_num_bytes // find the target BYTE
     * int value = 1 << (index%bit_num_bytes) // find the INDEX of the BYTE
     * bits_array[byte_index] = bits_array[byte_index]|value; //store the value
     */
}

void bitmap::clear_value(u32_t value)
{
    assert(value <= term_vert);
    assert(value >= start_vert);
    u32_t index = ch_vid_to_bitmap_index(value);
    bits_array[index >> BITS_SHIFT] &= ~(1 << (index & BITS_MASK));
}

u32_t bitmap::get_value(u32_t value)
{
    u32_t index = ch_vid_to_bitmap_index(value);
    return bits_array[index >> BITS_SHIFT] & (1 << (index & BITS_MASK));
}

u32_t bitmap::get_index(u32_t index)
{
    return bits_array[index >> BITS_SHIFT] & (1 << (index & BITS_MASK));
}

u32_t bitmap::ch_vid_to_bitmap_index(u32_t value)
{
    if (value == 0)
    {
        u32_t index = 0;
        return index;
    }
    u32_t index = VID_TO_BITMAP_INDEX(value, processor_id, num_processors);
    if (value < processor_id)
        PRINT_DEBUG("value = %d, processor_id = %d\n", value, processor_id);
    assert(index <= term_index);
    assert(index >= start_index);
    assert(value <= term_vert);
    assert(value >= start_vert);
    return index;
}
void bitmap::set_max_value(u32_t value)
{
    u32_t index = ch_vid_to_bitmap_index(value);
    max_index = index;
    max_vert = value;
}

void bitmap::set_min_value(u32_t value)
{
    u32_t index = ch_vid_to_bitmap_index(value);
    min_index = index;
    min_vert = value;
}

void bitmap::set_edge_num(u32_t edge_num_in)
{
    edge_num = edge_num_in;
}

u32_t bitmap::get_edge_num()
{
    return edge_num;
}
void bitmap::reset_value()
{
    max_vert = start_vert;
    min_vert = term_vert;
    max_index = VID_TO_BITMAP_INDEX(start_vert, processor_id, num_processors);
    min_index = VID_TO_BITMAP_INDEX(term_vert, processor_id, num_processors);
    edge_num = 0;
}

u32_t bitmap::get_bits_true_size()
{
    return bits_true_size;
}

u32_t bitmap::get_max_vert()
{
    return max_vert;
}

u32_t bitmap::get_term_vert()
{
    return term_vert;
}
u32_t bitmap::get_start_vert()
{
    return start_vert;
}
u32_t bitmap::get_min_vert()
{
    return min_vert;
}
void bitmap::print_binary(u32_t start, u32_t stop)
{
    for(u32_t j = start; j <= stop; j++)
    {
        if (get_index(j))
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
