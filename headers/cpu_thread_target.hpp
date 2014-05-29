#ifndef __CPU_THREAD_TARGET_HPP__
#define __CPU_THREAD_TARGET_HPP__

#include "cpu_thread.hpp"
#include "bitmap.hpp"
#include "disk_thread_target.hpp"
#include <cassert>

#include "config.hpp"
#include "print_debug.hpp"
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sstream> 
#include <fcntl.h>


/*enum fog_engine_state{
    INIT = 0,
    SCATTER,
    GATHER,
    TERM
};*/

//denotes the different status of cpu threads after they finished the given tasks.
// Note: these status are for scatter phase ONLY!
/*enum cpu_thread_status{
	UPDATE_BUF_FULL = 100,	//Cannot scatter more updates, since my update buffer is full
	NO_MORE_SCHED,			//I have no more sched tasks, but have updates in the auxiliary update buffer
	FINISHED_SCATTER		//I have no more sched tasks, and no updates in auxiliary update buffer.
							//	But still have updates in my strip update buffer. 
};*/

#define SCHED_BUFFER_LEN    1024

template <typename A, typename VA>
class cpu_thread_target;

/*class barrier {
    volatile unsigned long count[2];
    volatile unsigned long sense;
    unsigned long expected;
    public:
    barrier(unsigned long expected_in)
        :sense(0), expected(expected_in)
    {
        count[0] = 0;
        count[1] = 0;
    }

    void wait()
    {
        unsigned long sense_used = sense;
        unsigned long arrived =
            __sync_fetch_and_add(&count[sense_used], 1);
        if(arrived == (expected - 1)) {
            sense = 1 - sense_used; // Reverse sense
            count[sense_used] = 0;
        }
        while(count[sense_used] != 0);
        __sync_synchronize(); // Also clobber memory
    }
//    friend class cpu_thread_target<A,VA>;
};*/

struct init_param_target{
	char* attr_buf_head;
	u32_t start_vert_id;
	u32_t num_of_vertices;
    u32_t PHASE;
};

struct scatter_param_target{
	void* attr_array_head;
    u32_t old_signal;
    u32_t PHASE;
};

struct gather_param_target{
    void * attr_array_head;
    u32_t PHASE;
    u32_t strip_id;
    u32_t threshold;
};

struct callback_scatter_param_target
{
    u32_t strip_id;
    u32_t vertex_id;
    char * old_read_buf;
};

template <typename A, typename VA>
struct cpu_work_target{
	u32_t engine_state;
	void* state_param;
    static io_queue_target* thread_io_queue;

	cpu_work_target( u32_t state, void* state_param_in, io_queue_target * thread_io_queue_in )
		:engine_state(state), state_param(state_param_in)
	{
        thread_io_queue = thread_io_queue_in;
    }
	
         //get file name for current segment
     std::string get_current_bitmap_file_name(u32_t processor_id, u32_t set_id, u32_t strip_id)
     { 
         std::stringstream str_strip, str_procs, str_set;
         str_strip << strip_id;
         str_set << set_id;
         str_procs << processor_id;
         std::string current_file_name = "CPU"+str_procs.str()+"_"+str_set.str()+"_sched_strip"+str_strip.str();
         return current_file_name;
     }

    //for exp
     static void read_bitmap_file_to_buf(char * current_read_buf, std::string current_file_name, u32_t read_size )
     {   
         io_work_target * read_bitmap_io_work = NULL;
         read_bitmap_io_work = new io_work_target( current_file_name.c_str(),
             FILE_READ,
             current_read_buf,                                    
             0,                                                   
             read_size);
         
         thread_io_queue->add_io_task( read_bitmap_io_work );        
         if (read_bitmap_io_work != NULL)
         {
             thread_io_queue->wait_for_io_task( read_bitmap_io_work);    
             thread_io_queue->del_io_task( read_bitmap_io_work );        
             read_bitmap_io_work = NULL;
         }
         PRINT_DEBUG("true_size of current_read_buf is %d\n", (u32_t)*current_read_buf);
     }
	void operator() ( u32_t processor_id, barrier *sync, index_vert_array *vert_index, 
		segment_config<VA, sched_bitmap_manager>* seg_config, int *status, callback_scatter_param_target * callback_scatter_param)
	{
		u32_t local_start_vert_off, local_term_vert_off;
        sync->wait();
		
		switch( engine_state ){
			case INIT:
			{	//add {} to prevent "error: jump to case label" error. Cann't believe that!
				init_param_target* p_init_param = (init_param_target*) state_param;

				if( processor_id*seg_config->partition_cap > p_init_param->num_of_vertices ) break;

				//compute loca_start_vert_id and local_term_vert_id
				local_start_vert_off = processor_id*(seg_config->partition_cap);

				if ( ((processor_id+1)*seg_config->partition_cap-1) > p_init_param->num_of_vertices )
					local_term_vert_off = p_init_param->num_of_vertices - 1;
				else
					local_term_vert_off = local_start_vert_off + seg_config->partition_cap - 1;
			
//				PRINT_DEBUG( "processor:%d, vert start from %u, number:%u local start from vertex %u to %u\n", 
//					processor_id, 
//					p_init_param->start_vert_id, 
//					p_init_param->num_of_vertices, 
//					local_start_vert_off, 
//					local_term_vert_off );

				//Note: for A::init, the vertex id and VA* address does not mean the same offset!
				for (u32_t i=local_start_vert_off; i<=local_term_vert_off; i++ )
					A::init( p_init_param->start_vert_id + i, (VA*)(p_init_param->attr_buf_head) + i, p_init_param->PHASE);

				break;
			}
			case SCATTER:
			{
                scatter_param_target * p_scatter_param = (scatter_param_target *)state_param; 
                sched_bitmap_manager * my_sched_bitmap_manager;
                update_map_manager * my_update_map_manager;
                aux_update_buf_manager<VA> * my_aux_manager;
                u32_t my_strip_cap, per_cpu_strip_cap;
                u32_t * my_update_map_head;

                VA * attr_array_head;
                update<VA> * my_update_buf_head;

                //bitmap * p_bitmap;
                edge * t_edge;
                update<VA> * t_update;
                u32_t num_out_edges,  temp_laxity;
                u32_t strip_num, cpu_offset, map_value, update_buf_offset;

                //PRINT_DEBUG("processor : %d, parameter address:%llx\n", processor_id, (u64_t)p_scatter_param);

                my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->sched_manager;
                my_update_map_manager = seg_config->per_cpu_info_list[processor_id]->update_manager;
                my_aux_manager = seg_config->per_cpu_info_list[processor_id]->aux_manager;

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                per_cpu_strip_cap = my_strip_cap/gen_config.num_processors;
                my_update_map_head = my_update_map_manager->update_map_head;
                attr_array_head = (VA *)p_scatter_param->attr_array_head;
                my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[processor_id]->strip_buf_head);


                int current_bitmap_file_fd;
                u32_t old_strip_id;
                u32_t old_vertex_id;
                u32_t max_vert, min_vert;
                bitmap *new_read_bitmap;
                char * old_read_buf;
                char * current_read_buf;

                if (p_scatter_param->old_signal == 1)
                {
                    PRINT_DEBUG("We need to finish the last scatter of the bitmap_file:%s\n", 
                            get_current_bitmap_file_name(processor_id, p_scatter_param->PHASE, callback_scatter_param->strip_id).c_str());
                    old_read_buf = callback_scatter_param->old_read_buf;
                    old_strip_id = callback_scatter_param->strip_id;
                    old_vertex_id = callback_scatter_param->vertex_id;
                }
                else
                {
                    old_strip_id = 0;
                    old_vertex_id = 0;
                    old_read_buf = NULL;
                }

                for (u32_t strip_id = old_strip_id; strip_id < seg_config->num_segments; strip_id++)
                {
                    if (p_scatter_param->old_signal == 1)
                    {
                        current_read_buf = old_read_buf;
                        new_read_bitmap = new bitmap(
                                seg_config->partition_cap,
                                my_sched_bitmap_manager->bitmap_file_size,
                                START_VID(strip_id, processor_id), TERM_VID(strip_id, processor_id),
                                (u32_t)1,
                                (bitmap_t)old_read_buf);
                        
                        max_vert = new_read_bitmap->get_max_vert();
                        min_vert = old_vertex_id;
                    }
                    else
                    {
                        std::string current_bitmap_file_name = 
                            get_current_bitmap_file_name(processor_id, p_scatter_param->PHASE, strip_id);
                        //PRINT_DEBUG("current_bitmap_file_name = %s\n", current_bitmap_file_name.c_str());
                        struct stat st;

                        current_bitmap_file_fd = open(current_bitmap_file_name.c_str(), O_RDWR);
                        fstat(current_bitmap_file_fd, &st);
                        if (st.st_size == 0)
                            continue;
                    
                        current_read_buf = my_sched_bitmap_manager->sched_bitmap_head;
                        memset(current_read_buf, 0, 
                                my_sched_bitmap_manager->bitmap_file_size);
                        read_bitmap_file_to_buf(
                                current_read_buf, 
                                current_bitmap_file_name,
                                my_sched_bitmap_manager->bitmap_file_size);
                        new_read_bitmap = new bitmap(
                                seg_config->partition_cap,
                                my_sched_bitmap_manager->bitmap_file_size,
                                START_VID(strip_id, processor_id), TERM_VID(strip_id, processor_id),
                                (u32_t)1,
                                (bitmap_t)current_read_buf);
                        max_vert = new_read_bitmap->get_max_vert();
                        min_vert = new_read_bitmap->get_min_vert();
                        if (ftruncate(current_bitmap_file_fd, 0) < 0)
                        {
                            PRINT_ERROR("ftruncate bitmap_file error!\n");
                        }
                        close(current_bitmap_file_fd);
                    }

                    //new_read_bitmap->print_binary(0,100);            
                    //PRINT_DEBUG("STRIP_ID = %d, PROCESSOR_ID = %d\n", processor_id, strip_id);

                    //u32_t num_of_vert_to_scatter = new_read_bitmap->get_bits_true_size();
                    //PRINT_DEBUG("num_of_vert_to_scatter = %d\n", num_of_vert_to_scatter);

                    //a funny way to get the private value
                    //u32_t max_vert = *((u32_t *)new_read_bitmap + 5);
                    //u32_t min_vert = *((u32_t *)new_read_bitmap + 6);
                    //Traversal the bitmap to find ...
                    for (u32_t i = min_vert; i <= max_vert; i++)
                    {
                        if (new_read_bitmap->get_value(VID_TO_BITMAP_INDEX(i, seg_config->partition_cap)) == 0)
                            continue;

                        if (max_vert == min_vert)
                        {
                            PRINT_DEBUG("Only a vertex in this bitmap file!\n");
                            assert(new_read_bitmap->get_value(VID_TO_BITMAP_INDEX(i, seg_config->partition_cap)));
                        }
                        num_out_edges = vert_index->num_out_edges(i);
                        //PRINT_DEBUG("num_out_edges = %d\n", num_out_edges);
                        if (num_out_edges == 0 ) continue;

                        //tell if the remaining space in update buffer is enough to store the updates?
                        //since we add an auxiliary update buffer
                        temp_laxity = my_aux_manager->buf_cap - my_aux_manager->num_updates;

                        if (temp_laxity < num_out_edges)
                        {
                            PRINT_DEBUG("Processor %d: laxity=%u, current out edges=%u, i = %u\n", processor_id, temp_laxity, num_out_edges, i);
                            *status = UPDATE_BUF_FULL;
                            //need to supplement
                            callback_scatter_param->strip_id = strip_id;
                            callback_scatter_param->vertex_id = i; 
                            //store the memory address
                            callback_scatter_param->old_read_buf = current_read_buf ;
                            return;
                        }

                        //set the 0 to the i-th pos 
                        new_read_bitmap->clear_value(VID_TO_BITMAP_INDEX(i, seg_config->partition_cap));

                        //u32_t tmp;
                        for (u32_t j = 0; j < num_out_edges; j++)
                        {
                            t_edge = vert_index->out_edge(i, j);
                            assert(t_edge);//Make sure this edge existd!
                            t_update = A::scatter_one_edge(i, (VA *)&attr_array_head[i], num_out_edges, t_edge);
                            assert(t_update);

                            strip_num = VID_TO_SEGMENT(t_update->dest_vert);
                            cpu_offset = VID_TO_PARTITION(t_update->dest_vert );
                            assert(strip_num < seg_config->num_segments);
                            assert(cpu_offset < gen_config.num_processors);
                            map_value = *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset);

                            if (map_value < per_cpu_strip_cap)
                            {
                                update_buf_offset = strip_num * my_strip_cap + map_value * gen_config.num_processors + cpu_offset;
                                *(my_update_buf_head + update_buf_offset) = *t_update;
                                //update the map
                                map_value++;
                                *(my_update_map_head + strip_num * gen_config.num_processors + cpu_offset) = map_value;
                            }
                            else
                            {
                                //should add it to auxiliary update buffer
                                *(my_aux_manager->update_head + my_aux_manager->num_updates) = *t_update;
                                my_aux_manager->num_updates++;
                            }
                            delete t_edge;
                            delete t_update;
                            //tmp = j;
                        }
                        //PRINT_DEBUG("tmp = %d\n", tmp);
                    }
                    if (p_scatter_param->old_signal == 1)
                    {
                        p_scatter_param->old_signal = 0;
                        callback_scatter_param->strip_id = 0;
                        callback_scatter_param->vertex_id = 0; 
                        //store the memory address
                        callback_scatter_param->old_read_buf = NULL;
                    }
                            
                }
                    
                if (my_aux_manager->num_updates > 0)
                    *status = NO_MORE_SCHED;
                else 
                    *status = FINISHED_SCATTER;
                break;
			}
            case GATHER:
            {                
                gather_param_target * p_gather_param = (gather_param_target *)state_param; 
                //sched_bitmap_manager * my_sched_bitmap_manager;
                update_map_manager * my_update_map_manager;
                aux_update_buf_manager<VA> * tmp_aux_manager;
                u32_t my_strip_cap/*,per_cpu_strip_cap*/;
                u32_t * my_update_map_head;

                u32_t new_signal = 0;

                VA * attr_array_head;
                update<VA> * my_update_buf_head;

                update<VA> * t_update;
                u32_t map_value, update_buf_offset;
                u32_t dest_vert;
                u32_t strip_id;
                u32_t threshold;
                u32_t vert_index;

                //PRINT_DEBUG("processor : %d, parameter address:%llx\n", processor_id, (u64_t)p_gather_param);

                my_strip_cap = seg_config->per_cpu_info_list[processor_id]->strip_cap;
                attr_array_head = (VA *)p_gather_param->attr_array_head;
                strip_id = p_gather_param->strip_id;
                threshold = p_gather_param->threshold;



                //int current_bitmap_file_fd;
                //Traversal all the buffers of each cpu to find the corresponding UPDATES
                for (u32_t buf_id = 0; buf_id < gen_config.num_processors; buf_id++)
                {
                    //PRINT_DEBUG("I(%d) am checking the %d's buffer\n", processor_id, buf_id);
                    my_update_map_manager = seg_config->per_cpu_info_list[buf_id]->update_manager;
                    my_update_map_head = my_update_map_manager->update_map_head;
                    my_update_buf_head = (update<VA> *)(seg_config->per_cpu_info_list[buf_id]->strip_buf_head);
                    map_value = *(my_update_map_head + strip_id * gen_config.num_processors + processor_id);
                    if (map_value == 0)
                        continue;

                    //PRINT_DEBUG("I(%d) am checking the %d's buffer, and now in the %d strip,and the num of this strip is %d\n", 
                      //      processor_id, buf_id, strip_id, map_value);
                    for (u32_t update_id = 0; update_id < map_value; update_id++)
                    {
                        if (update_id == 0 )
                            new_signal = 0; //will  init a new buffer to store the gather info
                        else if (update_id == map_value - 1)
                            new_signal = 2; // will write the buffer to bitmap file
                        else
                            new_signal = 1; // will wirte info to buffer

                        update_buf_offset = strip_id * my_strip_cap + update_id * gen_config.num_processors + processor_id;
                        //PRINT_DEBUG("update_buf_offset = %d\n", update_buf_offset);
                        t_update = (my_update_buf_head + update_buf_offset);
                        //PRINT_DEBUG("dest_vert = %d\n", my_update_buf_head->dest_vert);
                        assert(t_update);
                        dest_vert = t_update->dest_vert;
                        if (threshold == 1) 
                            vert_index = dest_vert%seg_config->segment_cap;
                        else
                            vert_index = dest_vert;
                            
                        //PRINT_DEBUG("dest_vert = %d\n", dest_vert);
                        A::gather_one_update(dest_vert, 
                                (VA *)&attr_array_head[vert_index], 
                                t_update, 
                                p_gather_param->PHASE,
                                new_signal);


                    }
                    map_value = 0;
                    *(my_update_map_head + strip_id * gen_config.num_processors + processor_id) = 0;
                    //now, the thread has finished its own task of the strip_id 
                    if (strip_id == (seg_config->num_segments - 1))
                    {
                        //handle the aux_buffer 
                        for (u32_t tmp_processor_id = 0; tmp_processor_id < gen_config.num_processors; tmp_processor_id++)
                        {
                            tmp_aux_manager = seg_config->per_cpu_info_list[tmp_processor_id]->aux_manager;
                            if (tmp_aux_manager->num_updates > 0)
                            {
                                //we need to handle each process's buffer 
                            }
                        }
                    }
                }
                break;
            }
			default:
				printf( "Unknow fog engine state is encountered\n" );
		}

        sync->wait();
	}

/*
	//return value:
	//0: added successfully
	//-1: failed on adding
	int add_update_to_sched_update( 
				seg_config,
				map_head, 
				buf_head, 
				p_update, 
				per_cpu_strip_cap )
	{

		t_update = (update<VA>*)(my_aux_manager->update_head + i);
		strip_num = VID_TO_SEGMENT( t_update->dest_vert );
		cpu_offset = VID_TO_PARTITION( t_update->dest_vert );

//						assert( strip_num < seg_config->num_segments );
//						assert( cpu_offset < gen_config.num_processors );

		//update map layout
		//			cpu0				cpu1				.....
		//strip 0-> map_value(s0,c0)	map_value(s0, c1)	.....
		//strip 1-> map_value(s1,c0)	map_value(s1, c1)	.....
		//strip 2-> map_value(s2,c0)	map_value(s2, c1)	.....
		//		... ...
		map_value = *(my_update_map_head + strip_num*gen_config.num_processors + cpu_offset);

		if( map_value < per_cpu_strip_cap ){
			update_buf_offset = strip_num*per_cpu_strip_cap*gen_config.num_processors
				+ map_value*gen_config.num_processors 
				+ cpu_offset;

		*(my_update_buf_head + update_buf_offset) = *t_update;

		//update the map
		map_value ++;
	}
*/

    void show_update_map( int processor_id, segment_config<VA, sched_bitmap_manager>* seg_config, u32_t* map_head )
    {
        //print title
        PRINT_SHORT( "--------------- update map of CPU%d begin-----------------\n", processor_id );
        PRINT_SHORT( "\t" );
        for( u32_t i=0; i<gen_config.num_processors; i++ )
        PRINT_SHORT( "\tCPU%d", i );
        PRINT_SHORT( "\n" );

        for( u32_t i=0; i<seg_config->num_segments; i++ ){
            PRINT_SHORT( "Strip%d\t\t", i );
            for( u32_t j=0; j<gen_config.num_processors; j++ )
            PRINT_SHORT( "%d\t", *(map_head+i*(gen_config.num_processors)+j) );
            PRINT_SHORT( "\n" );
        }
        PRINT_SHORT( "--------------- update map of CPU%d end-----------------\n", processor_id );
    }

	//return current sched_task
	//return NULL when there is no more tasks
	static sched_task * get_sched_task( sched_list_manager* sched_manager )
	{
		if( sched_manager->sched_task_counter == 0 ) return NULL;

		return sched_manager->current;
	}

	//delete current sched_task
	//note: Must be the current sched_task!
	// After deletion, current pointer should move forward by one
	void del_sched_task( sched_list_manager* sched_manager )
	{
		assert( sched_manager->sched_task_counter > 0 );

		sched_manager->sched_task_counter --;

		//move forward current pointer
        if( sched_manager->current >= (sched_manager->sched_buf_head
                    + sched_manager->sched_buf_size) )
                sched_manager->current = sched_manager->sched_buf_head;
        else
                sched_manager->current++;

		//move forward head
		sched_manager->head = sched_manager->current;
	}
};

template <typename A, typename VA>
class cpu_thread_target {
public:
    const unsigned long processor_id; 
	index_vert_array* vert_index;
	segment_config<VA, sched_bitmap_manager>* seg_config;
	int status;
    callback_scatter_param_target* callback_scatter_param;

	//following members will be shared among all cpu threads
    static barrier *sync;
    static volatile bool terminate;
    static struct cpu_work_target<A,VA> * volatile work_to_do;

    cpu_thread_target(u32_t processor_id_in, index_vert_array * vert_index_in, segment_config<VA, sched_bitmap_manager>* seg_config_in )
    :processor_id(processor_id_in), vert_index(vert_index_in), seg_config(seg_config_in)
    {   
        if(sync == NULL) { //as it is shared, be created for one time
	        sync = new barrier(gen_config.num_processors);
        }
    }   

    void operator() ()
    {
        do{
            sync->wait();
            if(terminate) {
	            break;
            }
            else {
	            (*work_to_do)(processor_id, sync, vert_index, seg_config, &status, callback_scatter_param );

            sync->wait(); // Must synchronize before p0 exits (object is on stack)
            }
        }while(processor_id != 0);
    }

	sched_task* get_sched_task()
	{return NULL;}

	void browse_sched_list()
	{}

};

template <typename A, typename VA>
barrier * cpu_thread_target<A, VA>::sync;

template <typename A, typename VA>
volatile bool cpu_thread_target<A, VA>::terminate;

template <typename A, typename VA>
cpu_work_target<A,VA> * volatile cpu_thread_target<A, VA>::work_to_do;

template <typename A, typename VA>
io_queue_target * cpu_work_target<A, VA>::thread_io_queue;

#endif
