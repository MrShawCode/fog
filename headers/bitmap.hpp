/**************************************************************************************************
 * Authors: 
 *   Jian He
 *
 * Declaration:
 *   Bitmaps for targeted FOG engine
 *************************************************************************************************/

#ifndef __BITMAP_H__
#define __BITMAP_H__

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <cassert>

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
#endif
