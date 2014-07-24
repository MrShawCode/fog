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
        u32_t start_vert, term_vert;
        u32_t processor_id, num_processors;

    public:
        //new bitmap 
        bitmap(char * bitmap_buf_head_in, u32_t buf_len_bytes_in, u32_t buf_num_bits_in, 
                u32_t start_vert_in, u32_t term_vert_in, u32_t processor_id_in, u32_t num_processors_in);
        ~bitmap();
        void set_value(u32_t index); 
        void clear_value(u32_t index);
        u32_t get_value(u32_t index);
        u32_t get_u8_value(u32_t index);
        u32_t ch_vid_to_bitmap_index(u32_t value);
        u32_t get_term_vert();
        u32_t get_start_vert();
        void memset_buffer();
        void print_binary(u32_t start, u32_t stop);

};

bitmap::bitmap(char * bitmap_buf_head_in, u32_t buf_len_bytes_in, u32_t buf_num_bits_in, 
        u32_t start_vert_in, u32_t term_vert_in, u32_t processor_id_in, u32_t num_processors_in)
    :bitmap_buf_head(bitmap_buf_head_in),
    bits_array(bitmap_buf_head_in),
    buf_len_bytes(buf_len_bytes_in),
    buf_num_bits(buf_num_bits_in),
    start_vert(start_vert_in), term_vert(term_vert_in), 
    processor_id(processor_id_in), num_processors(num_processors_in)
{
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
    if (bits_array[index >> BITS_SHIFT] == 0 )
        PRINT_ERROR("This vert is 0!, clear a non-exist value???\n");
    bits_array[index >> BITS_SHIFT] &= ~(1 << (index & BITS_MASK));
}

u32_t bitmap::get_value(u32_t value)
{
    u32_t index = ch_vid_to_bitmap_index(value);
    return bits_array[index >> BITS_SHIFT] & (1 << (index & BITS_MASK));
}

u32_t bitmap::get_u8_value(u32_t index)
{
    assert((index%(sizeof(char) * 8)) == 0);
    if (bits_array[index >> BITS_SHIFT] == 0)
        return 0;
    else 
        return 1;
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
    assert(value <= term_vert);
    assert(value >= start_vert);
    return index;
}

void bitmap::memset_buffer()
{
    memset(bitmap_buf_head, 0, buf_len_bytes);
}

u32_t bitmap::get_term_vert()
{
    return term_vert;
}
u32_t bitmap::get_start_vert()
{
    return start_vert;
}

void bitmap::print_binary(u32_t start, u32_t stop)
{
    for(u32_t j = start; j <= stop; j = j + num_processors)
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
