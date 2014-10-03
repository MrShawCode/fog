//fog_engine is defined for scced queries, such as SSSP.
// characters of fog_engine:
// 1) Schedule list with dynamic size
// 2) need to consider merging and (possibly) the scheduled tasks
// 3) As the schedule list may grow dramatically, the system may need to consider dump 
//	(partial) of the list to disk (to alieviate pressure on buffer).
//Also:
// 1) Schedule list with fixed size (defined by a MACRO)
// 2) donot need to sort and merge the scheduled tasks, just FIFO.
#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "bitmap.hpp"
#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"
#include "print_debug.hpp"

#define THRESHOLD 0.8
#define MMAP_THRESHOLD 0.02

enum global_target
{
    GLOBAL_ENGINE = 0,
    TARGET_ENGINE,
    BACKWARD_ENGINE
};
//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute
template <typename A, typename VA, typename U, typename T>
class fog_engine{

        //global variables
		static index_vert_array<T>* vert_index;

		static segment_config<VA> *seg_config;
        char * buf_for_write;

        static u32_t scatter_fog_engine_state;
        static u32_t gather_fog_engine_state;
        static u32_t init_fog_engine_state;
        static u32_t current_attr_segment;

        //io work queue
        static io_queue * fog_io_queue;

        cpu_thread<A,VA,U,T> ** pcpu_threads;
        boost::thread ** boost_pcpu_threads;

        u32_t * p_strip_count;

        //The reasons to use another mmaped file to access the attribute file (in SCATTER phase):
        //  It is really hard if not possible to arrange the attribute buffer by repeatively reading
        //  the attribute file in/replace, since there may be different status among the cpu threads.
        //  For ex., cpu0 may need to access segment 1, while other cpu threads need to access the 
        //  segment 2. 
        //  The other reason is that, since file reading and buffer replacing is done intermediatively,
        //  there will be (and must be) a waste at the last step. 
        //  Think about the case that cpu threads filled up their update buffer, and ready to finish
        //  their current SCATTER phase. But remember, at this time, there is another file reading 
        //  conducting on the background, which is useless and the following steps (i.e., GATHER)
        //  must wait till the completion of this background operation.
        int attr_fd;
        u64_t attr_file_length;
        VA *attr_array_header;

        int signal_of_partition_gather;
        u32_t global_or_target;


	public:

		fog_engine(u32_t global_target)
		{
            //create the index array for indexing the out-going edges
            vert_index = new index_vert_array<T>;

            //in_edge * t_edge;
            //for (u32_t i = 0; i <= gen_config.max_vert_id; i++)
           // {
            //    u32_t num_edges = vert_index->num_edges(i, IN_EDGE);
            //    std::cout << "num_edges = " << num_edges << std::endl;
            //    for (u32_t j = 0; j < num_edges; j++)
            //    {
            //        t_edge = vert_index->get_in_edge(i, j);
            //        std::cout << "src->dst, value " << t_edge->src_vert << "->" << i << std::endl;
            //    }
            //}
            //exit(-1);
            //verify_vertex_indexing();
            global_or_target = global_target;

            signal_of_partition_gather = 0;

            //allocate buffer for writting
            buf_for_write = (char *)map_anon_memory(gen_config.memory_size, true, true );

            //config the buffer for writting
            seg_config = new segment_config<VA>((const char *)buf_for_write);

            //create io queue
            fog_io_queue = new io_queue;

            //create cpu threads
            pcpu_threads = new cpu_thread<A,VA,U,T> *[gen_config.num_processors];
            boost_pcpu_threads = new boost::thread *[gen_config.num_processors];
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                pcpu_threads[i] = new cpu_thread<A,VA,U,T>(i, vert_index, seg_config);
                if(i > 0)
                    boost_pcpu_threads[i] = new boost::thread(boost::ref(*pcpu_threads[i]));
            }
            attr_fd = 0;
            if (global_or_target == TARGET_ENGINE || global_or_target == BACKWARD_ENGINE)
                target_init_sched_update_buf();
            else
            {
                assert(global_or_target == GLOBAL_ENGINE);
                global_init_sched_update_buf();
            }
        }
			
		~fog_engine()
		{
            reclaim_everything();
        }

        void operator() ()
		{
            
             assert(A::CONTEXT_PHASE == 0);
             assert(A::loop_counter == 0);
             int ret;
             //while loop to do iteration
             int glo_loop = 0;
             while(1)
             {
                 glo_loop++;
                 PRINT_DEBUG("The %d global-loop\n", glo_loop);
                 init_fog_engine_state = INIT;
                 PRINT_DEBUG("forward_backward : %d\n", A::forward_backward_phase);
                 init_phase(glo_loop);
                 if (global_or_target == TARGET_ENGINE)
                 {
                    A::num_tasks_to_sched = cal_true_bits_size(A::CONTEXT_PHASE);
                    if( A::num_tasks_to_sched == 0)
                         break;
                 }
                 if (global_or_target == GLOBAL_ENGINE)
                 {
                    scatter_fog_engine_state = GLOBAL_SCATTER;
                    gather_fog_engine_state = GLOBAL_GATHER;
                   //start iteration
                    while(1)
                    {
                        A::loop_counter++;
                        A::CONTEXT_PHASE = (A::loop_counter%2);
                        A::num_tasks_to_sched = gen_config.max_vert_id + 1;
                        A::before_iteration();
                        scatter_updates(1-A::CONTEXT_PHASE);
                        //cal_threshold();
                        gather_updates(A::CONTEXT_PHASE, -1);
                        //cal_threshold();
                        
                        ret = A::after_iteration();
                        if (ret == ITERATION_STOP)
                            break;
                        assert(ret == ITERATION_CONTINUE);
                    }
                    PRINT_DEBUG("Iteration finished!\n");
                 }
                 else
                 {
                    assert(global_or_target == TARGET_ENGINE);

                    scatter_fog_engine_state = TARGET_SCATTER;
                    gather_fog_engine_state = TARGET_GATHER;
                    while(1)
                    {
                        A::loop_counter++;
                        A::CONTEXT_PHASE = (A::loop_counter%2);
                        A::num_tasks_to_sched = cal_true_bits_size(1-A::CONTEXT_PHASE);
                        A::before_iteration();
                        //PRINT_DEBUG("before normal-scatter, num_vert_of_next_phase = %d\n", cal_true_bits_size(1-A::CONTEXT_PHASE));
                        scatter_updates(1-A::CONTEXT_PHASE);
                        //after scatter
                        //cal_threshold();
                        //PRINT_DEBUG("after normal-scatter and before normal-gather, num_vert_of_next_phase = %d\n", cal_true_bits_size(1-A::CONTEXT_PHASE));
                        gather_updates(A::CONTEXT_PHASE, -1);
                        //PRINT_DEBUG("after normal-gather, num_vert_of_next_phase = %d\n", cal_true_bits_size(A::CONTEXT_PHASE));

                        //after gather
                        A::num_tasks_to_sched = cal_true_bits_size(A::CONTEXT_PHASE);

                        ret = A::after_iteration();
                        if (ret == ITERATION_STOP)
                            break;
                        assert(ret == ITERATION_CONTINUE);
                    }
                    PRINT_DEBUG("Iteration finished!\n");
                 }
                 ret = A::finalize();
                 if (ret == ENGINE_STOP)
                 {
                     print_attr_result();
                     break;
                 }
                 assert(ret == ENGINE_CONTINUE);
             }
        }
             
        void print_attr_result()
        {
            u32_t i = 0;
            VA * attr_array_head = NULL;
            if (seg_config->num_attr_buf == 1)
            {
                attr_array_head =  (VA *)seg_config->attr_buf0;
            }
            else
            {
                if (remap_attr_file() < 0)
                {
                    PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                }  
                attr_array_head = (VA*)attr_array_header;
            }
             
            for (i = 0; i < 10000; i++)
            {
                A::print_result(i, (VA*)&attr_array_head[i]);
            }
        }

        u32_t cal_true_bits_size(u32_t CONTEXT_PHASE)
        {
            u32_t num_vert_of_next_phase = 0;
            struct context_data * my_context_data;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                my_context_data = CONTEXT_PHASE > 0 ? seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1
                    : seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0;
                num_vert_of_next_phase += my_context_data->per_bits_true_size; 
            }
            return num_vert_of_next_phase;
        }
        void show_all_sched_tasks()
        {
            sched_list_context_data * global_sched_manager;

            PRINT_DEBUG( "==========================    Browse all scheduled tasks  ==========================\n" );
            //browse all cpus
            for(u32_t i=0; i<gen_config.num_processors; i++){
                global_sched_manager = seg_config->per_cpu_info_list[i]->global_sched_manager;
                PRINT_DEBUG( "Processor %d: Number of scheduled tasks: %d, Details:\n", i,
                    global_sched_manager->num_vert_to_scatter);
                PRINT_DEBUG("normal_min_vert = %d, normal_max_vert = %d\n", global_sched_manager->normal_sched_min_vert,
                        global_sched_manager->normal_sched_max_vert);
            }
            PRINT_DEBUG( "==========================    That's All  ==========================\n" );
        }
        
		void init_phase(int global_loop)
		{
			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work<A,VA,U,T>* new_cpu_work = NULL;
			io_work * init_io_work = NULL;
			char * buf_to_dump = NULL;
			init_param * p_init_param=new init_param;
            //init_fog_engine_scc_state = SCC_INIT;

			//PRINT_DEBUG( "fog engine operator is called, conduct init phase for %d times.\n", seg_config->num_segments );

            p_strip_count = new u32_t[seg_config->num_segments];
            memset(p_strip_count, 0, sizeof(u32_t)*seg_config->num_segments);
			current_attr_segment = 0;
			for( u32_t i=0; i < seg_config->num_segments; i++ ){
				//which attribute buffer should be dumped to disk?
				if ( current_attr_segment%2== 0 ) buf_to_dump = (char*)seg_config->attr_buf0;
				else buf_to_dump = (char*)seg_config->attr_buf1;

                if (seg_config->num_segments > 1 && 
                        ((global_loop > 1) || (global_loop == 1 && A::forward_backward_phase == BACKWARD_TRAVERSAL)))
                {
                    io_work * read_io_work = NULL;
                    u64_t offset = 0, read_size = 0;
                    offset = (u64_t)i * (u64_t)seg_config->segment_cap * sizeof(VA);
                    if (i == (seg_config->num_segments - 1))
                    {
                        read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA);
                    }
                    else 
                        read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                    read_io_work = new io_work(gen_config.attr_file_name.c_str(),
                            FILE_READ, buf_to_dump, offset, read_size);
                    fog_io_queue->add_io_task(read_io_work);
                    fog_io_queue->wait_for_io_task(read_io_work);
                    fog_io_queue->del_io_task(read_io_work);
                    read_io_work = NULL;
                }
				//create cpu threads
				if( i != (seg_config->num_segments-1) ){
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = seg_config->segment_cap;
					new_cpu_work = new cpu_work<A,VA,U,T>( init_fog_engine_state, 
						(void*)p_init_param);
				}else{	//the last segment, should be smaller than a full segment
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap+1;
					new_cpu_work = new cpu_work<A,VA,U,T>( init_fog_engine_state, 
						(void*)p_init_param);
				}
				pcpu_threads[0]->work_to_do = new_cpu_work;
				(*pcpu_threads[0])();
				//cpu threads finished init current attr buffer
				delete new_cpu_work;
				new_cpu_work = NULL;
				
				//dump current attr buffer to disk
				if ( init_io_work != NULL ){
					//keep waiting till previous disk work is finished
					fog_io_queue->wait_for_io_task( init_io_work );
					fog_io_queue->del_io_task( init_io_work );
					init_io_work = NULL;
				}

				if( i != (seg_config->num_segments-1) ){
					init_io_work = new io_work( gen_config.attr_file_name.c_str(),
                        FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)seg_config->segment_cap*sizeof(VA) );
				}else{
					init_io_work = new io_work( gen_config.attr_file_name.c_str(),
                        FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA) );
				}

				//activate the disk thread
				fog_io_queue->add_io_task( init_io_work );

				current_attr_segment++;
			}
            if (global_or_target == GLOBAL_ENGINE)
            {
                sched_task *t_task = new sched_task;t_task->start = 0;
                t_task->term = gen_config.max_vert_id;
                add_all_task_to_cpu(t_task);
                show_all_sched_tasks();
            }
	
			//the INIT phase finished now, ALTHOUGH the last write is still on its way. 
			// Do NOT wait!

			//should add tasks here, when the disk is busy.

			//show_all_sched_tasks();
			
			//FOLLOWING BELONGS TO THE INIT PHASE! wait till the last write work is finished.
			fog_io_queue->wait_for_io_task( init_io_work );
			fog_io_queue->del_io_task( init_io_work );
			//PRINT_DEBUG( "fog engine finished initializing attribute files!\n" );
			//ABOVE BELONGS TO THE INIT PHASE! wait till the last write work is finished.
		}


        //scatter_updates:
        //scatter updates to update buffer, till update buffer filled, or no more sched_tasks
        //return values:
        //0:no more sched tasks
        //1:update buffer full
        //2:steal work
        //-1:failure
        void set_signal_to_scatter(u32_t signal, u32_t processor_id, u32_t CONTEXT_PHASE)
        {
            if (scatter_fog_engine_state == GLOBAL_SCATTER)
            {
                sched_list_context_data * my_context_data = seg_config->per_cpu_info_list[processor_id]->global_sched_manager;
                my_context_data->signal_to_scatter = signal;
            }
            else
            {
                sched_bitmap_manager * my_sched_bitmap_manager;
                struct context_data * my_context_data;
                my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
                my_context_data = CONTEXT_PHASE > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;
                my_context_data->signal_to_scatter = signal; 
            }
        }
        void set_signal_to_gather(u32_t signal, u32_t processor_id, u32_t CONTEXT_PHASE)
        {
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            my_sched_bitmap_manager = seg_config->per_cpu_info_list[processor_id]->target_sched_manager;
            my_context_data = CONTEXT_PHASE > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;
            my_context_data->signal_to_gather = signal; 
        }
        int scatter_updates(u32_t CONTEXT_PHASE)
        {
            int ret = 0, unemployed = 0;
            cpu_work<A, VA, U, T> * scatter_cpu_work = NULL;
            scatter_param * p_scatter_param = new scatter_param;

            int phase = 0;

            //for (u32_t i = 0; i < gen_config.num_processors; i++)
            //{
            //    edge * t_edge = vert_index->out_edge(i, 0);
            //    assert(t_edge);
            //    PRINT_DEBUG("edge->dest = %d, value = %f\n", t_edge->dest_vert, t_edge->edge_weight);
            //}
            //prepare attribute date
            /*if( seg_config->num_attr_buf == 1 ){
                p_scatter_param->attr_array_head = (void*)seg_config->attr_buf0;
                p_scatter_param->PHASE = CONTEXT_PHASE;
            }else{
                if( remap_attr_file() < 0 ){
                    PRINT_ERROR( "Fog_engine::scatter_updates failed!\n" );
                    return -1;
                }
                p_scatter_param->PHASE = CONTEXT_PHASE;
                p_scatter_param->attr_array_head = (void*)attr_array_header;
            }*/
            //if (scatter_fog_engine_state == SCC_FORWARD_SCATTER)
            //    gather_fog_engine_state = TARGET_GATHER;

            do{

                if( seg_config->num_attr_buf == 1 ){
                    p_scatter_param->attr_array_head = (void*)seg_config->attr_buf0;
                    p_scatter_param->PHASE = CONTEXT_PHASE;
                }else{
                    if( remap_attr_file() < 0 ){
                        PRINT_ERROR( "Fog_engine::scatter_updates failed!\n" );
                        return -1;
                    }
                    p_scatter_param->PHASE = CONTEXT_PHASE;
                    p_scatter_param->attr_array_head = (void*)attr_array_header;
                }

                PRINT_DEBUG("sub-iteration:%d\n", phase);
                //if (global_or_target != GLOBAL_ENGINE && ret == 1)
                    //PRINT_DEBUG("before context scatter, num_vert_of_next_phase = %d\n", cal_true_bits_size(CONTEXT_PHASE));
                scatter_cpu_work = new cpu_work<A, VA, U, T>(scatter_fog_engine_state, (void *)p_scatter_param);
                pcpu_threads[0]->work_to_do = scatter_cpu_work;
                (*pcpu_threads[0])();

                delete scatter_cpu_work;
                scatter_cpu_work = NULL;
                //PRINT_DEBUG("After scatter computation!\n");

                ret = 0;
                unemployed = 0; //assume there is no unemployed
                int cpu_unfinished[gen_config.num_processors];
                int num_unfinished = 0;
                for( u32_t i=0; i<gen_config.num_processors; i++ ){
                    //PRINT_DEBUG( "Processor %d status %d\n", i, pcpu_threads[i]->status );
                    if ( pcpu_threads[i]->status != FINISHED_SCATTER )
                    {
                        cpu_unfinished[num_unfinished] = i;
                        ret = 1;
                        num_unfinished++;
                        set_signal_to_scatter(CONTEXT_SCATTER, i, CONTEXT_PHASE);
                    }
                
                    if ( pcpu_threads[i]->status != UPDATE_BUF_FULL )
                    {
                        unemployed = 1;
                        set_signal_to_scatter(NORMAL_SCATTER, i, CONTEXT_PHASE);
                    }
                }
                if (ret == 0 && unemployed == 1)
                {
                    //PRINT_DEBUG("num_unfinished = %d\n", num_unfinished);
                    assert(num_unfinished == 0);
                    signal_of_partition_gather = NORMAL_GATHER;
                }
                else if (ret == 1 && unemployed == 0)
                {
                    assert(num_unfinished == (int)gen_config.num_processors);
                    signal_of_partition_gather = CONTEXT_GATHER;
                    //cal_threshold();
                    gather_updates(1-CONTEXT_PHASE, phase);
                    //cal_threshold();
                }
                if ((global_or_target == TARGET_ENGINE || global_or_target == BACKWARD_ENGINE ) && ret == 1 && unemployed == 1)
                {
                    if ((cal_true_bits_size(CONTEXT_PHASE)) < (gen_config.num_processors * 8))
                    {
                        //PRINT_DEBUG("special gather happens!\n");
                        signal_of_partition_gather = CONTEXT_GATHER;
                        //cal_threshold();
                        gather_updates(1-CONTEXT_PHASE, phase);
                    }
                    else 
                    {
                        assert(num_unfinished > 0);
                        assert((u32_t)num_unfinished < gen_config.num_processors);
                        signal_of_partition_gather = STEAL_GATHER;
                        //cal_threshold();
                        gather_updates(1-CONTEXT_PHASE, -1);
                        //loop for all unfinished-cpus
                        for (u32_t k = 0; k < (u32_t)num_unfinished; k++)
                        {
                            ret = 0;
                            u32_t ret_value = rebalance_sched_bitmap(cpu_unfinished[k], CONTEXT_PHASE);
                            if (ret_value == 2)
                            {
                                //PRINT_DEBUG("cpu-%d has so few bits to steal!To be continued\n", cpu_unfinished[k]);
                                /*scatter_cpu_work = new cpu_work<A, VA>(scatter_fog_engine_state, (void *)p_scatter_param);
                                pcpu_threads[0]->work_to_do = scatter_cpu_work;
                                (*pcpu_threads[0])();

                                delete scatter_cpu_work;
                                scatter_cpu_work = NULL;

                                PRINT_DEBUG("After steal scatter!\n");
                                for( u32_t i=0; i<gen_config.num_processors; i++ ){
                                    PRINT_DEBUG( "Processor %d status %d\n", i, pcpu_threads[i]->status );
                                    if ( pcpu_threads[i]->status != FINISHED_SCATTER )
                                    {
                                        PRINT_ERROR("You need to gave more memory!~~\n");
                                    }
                                    if ( pcpu_threads[i]->status != UPDATE_BUF_FULL )
                                    {
                                        PRINT_DEBUG("gooD!\n");
                                    }
                                }*/
                                //continue;
                            }
                            u32_t special_signal;
                            do
                            {
                                special_signal = 0;
                                scatter_cpu_work = new cpu_work<A, VA, U, T>(scatter_fog_engine_state, (void *)p_scatter_param);
                                pcpu_threads[0]->work_to_do = scatter_cpu_work;
                                (*pcpu_threads[0])();

                                delete scatter_cpu_work;
                                scatter_cpu_work = NULL;

                                //PRINT_DEBUG("After steal scatter!\n");
                                for (u32_t i = 0; i < gen_config.num_processors; i++)
                                {
                                    context_data * context_data_steal = CONTEXT_PHASE > 0 ? 
                                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1: 
                                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0;
                                    context_data * context_data_not_finished = CONTEXT_PHASE > 0 ?
                                        seg_config->per_cpu_info_list[cpu_unfinished[k]]->target_sched_manager->p_context_data1: 
                                        seg_config->per_cpu_info_list[cpu_unfinished[k]]->target_sched_manager->p_context_data0;
                                    /*if (scatter_fog_engine_state == SCC_BACKWARD_SCATTER)
                                    {
                                        context_data * next_context_data_not_finished =(1- CONTEXT_PHASE) > 0 ?
                                            seg_config->per_cpu_info_list[cpu_unfinished[k]]->target_sched_manager->p_context_data1: 
                                            seg_config->per_cpu_info_list[cpu_unfinished[k]]->target_sched_manager->p_context_data0;
                                        next_context_data_not_finished->per_bits_true_size += context_data_steal->next_steal_bits_true_size;
                                        context_data_steal->next_steal_bits_true_size = 0;
                                        if (next_context_data_not_finished->per_min_vert_id > 
                                                context_data_steal->next_steal_min_vert_id)
                                            next_context_data_not_finished->per_min_vert_id = context_data_steal->next_steal_min_vert_id;
                                        if (next_context_data_not_finished->per_max_vert_id < 
                                                context_data_steal->next_steal_max_vert_id)
                                            next_context_data_not_finished->per_max_vert_id = context_data_steal->next_steal_max_vert_id;
                                    }*/
                                    
                                    context_data_not_finished->per_bits_true_size -= context_data_steal->steal_bits_true_size;
                                    //PRINT_DEBUG("per_bits_not_continued = %d\n", 
                                      //      context_data_not_finished->per_bits_true_size);
                                    context_data_steal->steal_bits_true_size = 0;
                                    if(pcpu_threads[i]->status != FINISHED_SCATTER)
                                    {
                                        //PRINT_DEBUG("In steal-mode, processor:%d has not finished scatter!Not very good!\n", i);
                                        special_signal = 1;
                                        set_signal_to_scatter(SPECIAL_STEAL_SCATTER, i, CONTEXT_PHASE);
                                    }
                                    if (pcpu_threads[i]->status != UPDATE_BUF_FULL )
                                    {
                                        //PRINT_DEBUG("Steal round %d, Processor: %d has finished scatter, and the update buffer is not full!\n", k, i);
                                        set_signal_to_scatter(STEAL_SCATTER, i, CONTEXT_PHASE);
                                        context_data_steal->steal_min_vert_id = 0;
                                        context_data_steal->steal_max_vert_id = 0;
                                        //context_data_steal->steal_context_edge_id = 0;
                                    }
                                }
                                if (special_signal == 1)
                                {
                                    signal_of_partition_gather = STEAL_GATHER;
                                    //cal_threshold();
                                    gather_updates(1-CONTEXT_PHASE, -1);
                                    //cal_threshold();
                                }
                            }while(special_signal == 1);
                        }
                        //PRINT_DEBUG("After steal!\n");
                        //cal_threshold();
                        //PRINT_DEBUG("after normal-scatter and before normal-gather, num_vert_of_next_phase = %d\n", cal_true_bits_size(1-CONTEXT_PHASE));
                        //if (scatter_fog_engine_state == SCC_BACKWARD_SCATTER)
                        gather_fog_engine_state = TARGET_GATHER;
                        gather_updates(1-CONTEXT_PHASE, -1);
                        ret = 0;
                    }
                }
                else if (global_or_target == GLOBAL_ENGINE && ret == 1 && unemployed == 1)
                {
                    assert(num_unfinished > 0);
                    assert((u32_t)num_unfinished < gen_config.num_processors);
                    signal_of_partition_gather = STEAL_GATHER;
                    //cal_threshold();
                    gather_updates(1-CONTEXT_PHASE, -1);

                    //loop for all unfinished-cpus
                    for (u32_t k = 0; k < (u32_t)num_unfinished; k++)
                    {
                        ret = 0; 
                        rebalance_sched_tasks(cpu_unfinished[k], CONTEXT_PHASE);
                        u32_t special_signal;
                        do{
                            special_signal = 0;
                            scatter_cpu_work = new cpu_work<A,VA, U, T>( scatter_fog_engine_state, (void*)p_scatter_param );

                            pcpu_threads[0]->work_to_do = scatter_cpu_work;
                            (*pcpu_threads[0])();

                            //cpu threads return
                            delete scatter_cpu_work;
                            scatter_cpu_work = NULL;

                            //after computation, check the status of cpu threads, and return
                            //PRINT_DEBUG( "After scatter computation\n" );
                            for( u32_t i=0; i<gen_config.num_processors; i++ ){
                                //PRINT_DEBUG( "Processor %d status %d\n", i, pcpu_threads[i]->status );
                                sched_list_context_data * context_data_steal = 
                                    seg_config->per_cpu_info_list[i]->global_sched_manager;
                                sched_list_context_data * context_data_unfinished = 
                                    seg_config->per_cpu_info_list[cpu_unfinished[k]]->global_sched_manager;

                                context_data_unfinished->num_vert_to_scatter -= context_data_steal->context_steal_num_vert;
                                //PRINT_DEBUG("unfinished->num_vert_to_scatter = %d\n", 
                                    //    context_data_unfinished->num_vert_to_scatter);
                                //PRINT_DEBUG("context_data_steal->context_steal_num_vert = %d\n", 
                                     //   context_data_steal->context_steal_num_vert);
                                context_data_steal->context_steal_num_vert = 0;
                                if ( pcpu_threads[i]->status != FINISHED_SCATTER )
                                {
                                    //PRINT_DEBUG("In steal-mode, processor:%d has not finished scatter, not very good!\n", i);
                                    special_signal = 1;
                                    set_signal_to_scatter(SPECIAL_STEAL_SCATTER, i, CONTEXT_PHASE);
                                }
                                if ( pcpu_threads[i]->status != UPDATE_BUF_FULL )
                                {
                                    //PRINT_DEBUG("Steal round %d. Processor %d has finished scatter!\n", k, i);
                                    set_signal_to_scatter(STEAL_SCATTER, i, CONTEXT_PHASE);
                                    context_data_steal->context_steal_min_vert = 
                                        context_data_steal->context_steal_max_vert = 0;
                                }
                            }
                            if (special_signal == 1)
                            {
                                signal_of_partition_gather = STEAL_GATHER;
                                //cal_threshold();
                                gather_updates(1-CONTEXT_PHASE, -1);
                            }
           
                        }while(special_signal == 1);
                    }
                    //PRINT_DEBUG("After steal!\n");
                    ret = 0;
                }
                phase++; 
            }while(ret == 1);
            //return ret;
            signal_of_partition_gather = 0;
            //PRINT_DEBUG("After all the phase in this scatter!\n");
            memset(p_strip_count, 0, sizeof(u32_t) * seg_config->num_segments);
            if (global_or_target == GLOBAL_ENGINE)
                reset_global_manager(CONTEXT_PHASE);
            else
                reset_target_manager(CONTEXT_PHASE);
            return ret;
        }

        void reset_target_manager(u32_t CONTEXT_PHASE)
        {
            context_data * my_context_data;
            for (u32_t i = 0 ; i < gen_config.num_processors; i++ )
            {
                my_context_data = CONTEXT_PHASE > 0 ? seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1
                    : seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0;
                if(my_context_data->per_bits_true_size != 0)
                    PRINT_ERROR("Per_bits_true_size != 0, impossible!\n");
                    //PRINT_ERROR("Per_bits_true_size != 0, impossible!\n");
                if (A::set_forward_backward == true && A::forward_backward_phase == FORWARD_TRAVERSAL)
                {
                    my_context_data->per_bits_true_size = my_context_data->alg_per_bits_true_size;
                    my_context_data->per_min_vert_id = my_context_data->alg_per_min_vert_id;
                    my_context_data->per_max_vert_id = my_context_data->alg_per_max_vert_id;
                }
                else
                    my_context_data->per_bits_true_size = 0;

                my_context_data->steal_min_vert_id = 0;
                my_context_data->steal_max_vert_id = 0;
                my_context_data->signal_to_scatter = 0;
                my_context_data->p_bitmap_steal = NULL;
                my_context_data->steal_virt_cpu_id = 0;
                my_context_data->steal_num_virt_cpus = 0;
                my_context_data->steal_bits_true_size = 0;
                my_context_data->steal_context_edge_id = 0;

                my_context_data->partition_gather_signal = 0;
                my_context_data->partition_gather_strip_id = -1;

                //scc using
                my_context_data->will_be_updated = false;
                my_context_data->next_p_bitmap_steal = NULL;
                my_context_data->next_steal_max_vert_id = my_context_data->p_bitmap->get_start_vert();
                my_context_data->next_steal_min_vert_id = my_context_data->p_bitmap->get_term_vert();

                //my_context_data->p_bitmap->memset_buffer();

            }
            //PRINT_DEBUG("Finish reset!\n");
        }

        void reset_global_manager(u32_t CONTEXT_PHASE)
        {
            sched_list_context_data * my_context_data;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                my_context_data = seg_config->per_cpu_info_list[i]->global_sched_manager;
                if (my_context_data->num_vert_to_scatter != 0)
                    PRINT_ERROR("num_vert != 0;\n");
                my_context_data->num_vert_to_scatter = my_context_data->normal_sched_vert_to_scatter;

                my_context_data->context_vert_id =
                my_context_data->context_edge_id = 0;

                my_context_data->context_steal_max_vert = 
                my_context_data->context_steal_min_vert = 0;
                my_context_data->context_steal_num_vert = 0;
                my_context_data->signal_to_scatter = 0;
            }
        }

        //return value: 
        //1:success
        //2: this (not_finished_cpu) cpu has few bits
        u32_t rebalance_sched_bitmap(u32_t cpu_not_finished_id, u32_t CONTEXT_PHASE)
        {

            u32_t cpu_id;
            context_data * context_data_not_finished = CONTEXT_PHASE > 0 ? 
                seg_config->per_cpu_info_list[cpu_not_finished_id]->target_sched_manager->p_context_data1 
                : seg_config->per_cpu_info_list[cpu_not_finished_id]->target_sched_manager->p_context_data0;
            context_data * next_context_data_not_finished = (1-CONTEXT_PHASE) > 0 ? 
                seg_config->per_cpu_info_list[cpu_not_finished_id]->target_sched_manager->p_context_data1 
                : seg_config->per_cpu_info_list[cpu_not_finished_id]->target_sched_manager->p_context_data0;
            u32_t min_vert = context_data_not_finished->per_min_vert_id;
            u32_t max_vert = context_data_not_finished->per_max_vert_id;
            u32_t average_num = (max_vert - min_vert + 1)/(gen_config.num_processors);
            //PRINT_DEBUG("In cpu %d, min_vert = %d, max_vert = %d, average_num = %d\n",
              //              cpu_not_finished_id, min_vert, max_vert, average_num);
                
            if ((average_num/8) == 0)
            {
                return 2;
            }
            u32_t tmp_min_vert = 0;
            u32_t tmp_max_vert = 0;
            u32_t tmp_index = 0;
            bool special_signal = false;
            for (cpu_id = 0; cpu_id < gen_config.num_processors; cpu_id++)
            {
                //set STEAL signal to next scatter
                set_signal_to_scatter(STEAL_SCATTER, cpu_id, CONTEXT_PHASE);
                context_data * context_data_steal = CONTEXT_PHASE > 0 ? seg_config->per_cpu_info_list[cpu_id]->target_sched_manager->p_context_data1 : seg_config->per_cpu_info_list[cpu_id]->target_sched_manager->p_context_data0;
                context_data_steal->steal_virt_cpu_id = cpu_not_finished_id;
                context_data_steal->p_bitmap_steal = context_data_not_finished->p_bitmap;
                context_data_steal->next_p_bitmap_steal = next_context_data_not_finished->p_bitmap;
                context_data_steal->next_steal_max_vert_id = context_data_not_finished->p_bitmap->get_start_vert();
                context_data_steal->next_steal_min_vert_id = context_data_not_finished->p_bitmap->get_term_vert();
                if (special_signal == true)
                {
                    context_data_steal->steal_min_vert_id = min_vert; 
                    context_data_steal->steal_max_vert_id = min_vert;
                    context_data_steal->steal_special_signal = true;
                    continue;
                }
                if (special_signal == false)
                    context_data_steal->steal_special_signal = false;

                if (cpu_id == 0)
                {
                    tmp_min_vert = min_vert;
                }
                else
                {
                    tmp_min_vert = tmp_max_vert + gen_config.num_processors;
                }
                context_data_steal->steal_min_vert_id = tmp_min_vert; 
                //PRINT_DEBUG("Processor-%d's steal_min_vert = %d\n", cpu_id,tmp_min_vert);

                if (cpu_id == gen_config.num_processors - 1)
                {
                    tmp_max_vert = max_vert;
                }
                else
                {
                    tmp_max_vert = tmp_min_vert + average_num;
                    tmp_index = (tmp_max_vert - cpu_not_finished_id)/(gen_config.num_processors);

                    if ((tmp_index % 8) == 0)
                        tmp_index--;
                    else
                    {
                        u32_t tmp_val = tmp_index%8;
                        tmp_index += 8 - tmp_val - 1;
                    }
                    tmp_max_vert = tmp_index*(gen_config.num_processors) + cpu_not_finished_id;
                    if (tmp_max_vert > max_vert)
                    {
                        assert(tmp_min_vert < max_vert);
                        tmp_max_vert = max_vert;
                        special_signal = true;
                    }
                }
                context_data_steal->steal_max_vert_id = tmp_max_vert;
                //PRINT_DEBUG("Processor-%d's steal_max_vert = %d\n", cpu_id, tmp_max_vert);
            }
            return 1;
        }

        void rebalance_sched_tasks(u32_t cpu_unfinished_id, u32_t CONTEXT_PHASE)
        {
            sched_list_context_data * my_context_data = seg_config->per_cpu_info_list[cpu_unfinished_id]->global_sched_manager;
            u32_t min_vert = my_context_data->context_vert_id;
            u32_t max_vert = my_context_data->normal_sched_max_vert;
            assert(((max_vert-min_vert)/gen_config.num_processors + 1) == 
                    my_context_data->num_vert_to_scatter);
            if (my_context_data->num_vert_to_scatter < gen_config.num_processors)
            {
                //PRINT_DEBUG("There are only %d vertices to scatter in processor %d\n", 
                        //my_context_data->num_vert_to_scatter, cpu_unfinished_id);
                for (u32_t i = 0; i < my_context_data->num_vert_to_scatter; i++)
                {
                    set_signal_to_scatter(STEAL_SCATTER, i, CONTEXT_PHASE);
                    seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_max_vert = 
                        seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_min_vert = 
                            min_vert + i * gen_config.num_processors;
                    //seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_num_vert = 1;

                    //PRINT_DEBUG("Processor-%d's context_steal_min_vert = %d, max = %d\n",
                      //      i, seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_min_vert,
                        //    seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_max_vert);
                }
            }
            else
            {
                //PRINT_DEBUG("There are %d vertices to scatter in processor %d\n", 
                  //      my_context_data->num_vert_to_scatter, cpu_unfinished_id);
                u32_t average_num = my_context_data->num_vert_to_scatter/(gen_config.num_processors);

                for (u32_t i = 0; i < gen_config.num_processors; i++)
                {
                    set_signal_to_scatter(STEAL_SCATTER, i, CONTEXT_PHASE);
                    seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_min_vert = 
                        min_vert + i * average_num * gen_config.num_processors;
                    if (i == gen_config.num_processors - 1)
                        seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_max_vert = max_vert;
                    else 
                        seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_max_vert = 
                            seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_min_vert + 
                            (average_num-1) * gen_config.num_processors;
                    //PRINT_DEBUG("Processor-%d's context_steal_min_vert = %d, max = %d\n",
                      //      i, seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_min_vert,
                        //    seg_config->per_cpu_info_list[i]->global_sched_manager->context_steal_max_vert);
                }
            }
        }
        
        //gather all updates in the update buffer.
        void gather_updates(u32_t CONTEXT_PHASE, int phase)
        {
            cpu_work<A,VA,U,T>* gather_cpu_work = NULL;
            io_work* one_io_work = NULL;
            char * next_buffer = NULL, *read_buf = NULL;
            char * write_buf = NULL; 
            u64_t offset, read_size;
            gather_param * p_gather_param = new gather_param;
            u32_t ret = 0;

            if (seg_config->num_attr_buf == 1)
            {
                p_gather_param->attr_array_head = (void*)seg_config->attr_buf0;
                p_gather_param->threshold = 0;
                p_gather_param->strip_id = 0;

                gather_cpu_work = new cpu_work<A, VA, U,T>(gather_fog_engine_state, (void *)p_gather_param);
                pcpu_threads[0]->work_to_do = gather_cpu_work;
                (*pcpu_threads[0])();

                delete gather_cpu_work;
                gather_cpu_work = NULL;
            }
            else
            {
                //for (u32_t x = 0; x < gen_config.num_processors; x++)
                //    set_signal_to_gather(signal_of_partition_gather, x, CONTEXT_PHASE );
            
                if (signal_of_partition_gather == NORMAL_GATHER || signal_of_partition_gather == STEAL_GATHER)
                {
                    //if (signal_of_partition_gather == NORMAL_GATHER)
                    //    PRINT_DEBUG("Normal gather starts!\n");
                    //else if (signal_of_partition_gather == STEAL_GATHER)
                     //   PRINT_DEBUG("Steal gather starts!\n");
                    /*
                     * Because there are seg_config->num_attr_buf segments(if nessary) which have been used
                     * in context_gather, so we need to gather them before  other segments.
                     * In our current version, we design a dual buffer program model to handle all segments.
                     */
                    u32_t buf_index = 0;
                    //check in debug by Hejian
                    assert(seg_config->num_attr_buf == 2);
                    for (u32_t i = 0; i < seg_config->num_attr_buf; i++)
                    {
                        u32_t tmp_strip_id;
                        //First segment
                        if (i == 0)
                        {
                            if (seg_config->buf0_holder == -1)
                                continue;
                            else
                            {
                                read_buf = (char *)seg_config->attr_buf0;
                                tmp_strip_id = seg_config->buf0_holder;
                            }
                        }
                        else
                        {
                            if (seg_config->buf1_holder == -1)
                                continue;
                            else
                            {
                                read_buf = (char *)seg_config->attr_buf1;
                                tmp_strip_id = seg_config->buf1_holder;
                            }
                        }
                        //Set up param to call cpu-thread .
                        //threshold = 1 means cpu_thread will use attr-buf to handle attr data but MMAP
                        p_gather_param->threshold = 1;
                        //strip_id will tell the cpu-thread to handle which strip
                        p_gather_param->strip_id = (int)tmp_strip_id;

                        p_gather_param->attr_array_head = (void *)read_buf;
                        //PRINT_DEBUG("Earlier gather strip %d\n", tmp_strip_id);

                        gather_cpu_work = new cpu_work<A, VA, U, T>(gather_fog_engine_state, (void *)p_gather_param);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                        //we need to write the attr_buf to attr_file 
                        if (one_io_work != NULL)
                        {
                            fog_io_queue->wait_for_io_task(one_io_work);
                            fog_io_queue->del_io_task(one_io_work);
                            one_io_work = NULL;
                        }

                        if (tmp_strip_id != (seg_config->num_segments-1) )
                        {
                            one_io_work = new io_work(gen_config.attr_file_name.c_str(),
                                    FILE_WRITE, read_buf, 
                                    (u64_t)tmp_strip_id * seg_config->segment_cap*sizeof(VA), (u64_t)seg_config->segment_cap*sizeof(VA));
                        }
                        else
                        {
                            one_io_work = new io_work(gen_config.attr_file_name.c_str(),
                                    FILE_WRITE, read_buf, 
                                    (u64_t)tmp_strip_id * seg_config->segment_cap*sizeof(VA),
                                    (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA));
                        }

                        fog_io_queue->add_io_task(one_io_work);
                        if (one_io_work != NULL)
                        {
                            fog_io_queue->wait_for_io_task(one_io_work);
                            fog_io_queue->del_io_task(one_io_work);
                            one_io_work = NULL;
                        }
                    }
                    //After gather the first two segments
                    if (one_io_work != NULL)
                    {
                        fog_io_queue->wait_for_io_task(one_io_work);
                        fog_io_queue->del_io_task(one_io_work);
                        one_io_work = NULL;
                    }
                    /*
                     * buf_index is used for normal-gather, to avoid IO-conflict while 
                     * reading or writing data of the same attr_buf
                     */
                    if (seg_config->buf0_holder != -1 && seg_config->buf1_holder == -1)
                        buf_index = 1;
                    else
                        buf_index = 0;

                    int mmap_ret = -1;                
                    for (u32_t i = 0; i < seg_config->num_segments; i++)
                    {
                        mmap_ret = cal_strip_size(i,1,1);
                        if (mmap_ret == 0)
                            break;
                    }
                    if (mmap_ret == 0)
                        if (remap_write_attr_file() < 0)
                        {
                            PRINT_ERROR("FOG_ENGINE::scatter_updates failed!\n");
                        }

                    io_work* write_io_work = NULL;

                    for(u32_t i = 0; i < seg_config->num_segments; i++)
                    {
                        //check if this strip is zero or has been early-gather
                        ret = cal_strip_size(i, 0, 0);
                        if (ret == 0 || (int)i == seg_config->buf0_holder  || (int)i == seg_config->buf1_holder)
                            continue;
                        //PRINT_DEBUG( "i=%d, buf_index = %d, one_io_work=%llx\n", i, buf_index, (u64_t)one_io_work );

                        mmap_ret = cal_strip_size(i, 1, 1);

                        //this strip will be gather by mmap
                        if (mmap_ret == 0)
                        {
                            //PRINT_DEBUG("for strip %d, mmap_gather starts!\n", i);
                            p_gather_param->attr_array_head = (void *)attr_array_header;
                            p_gather_param->threshold = 0;
                            p_gather_param->strip_id = i;
                        
                        }
                        else if (mmap_ret == 1)
                        {
                            if (buf_index%2) read_buf = (char *)seg_config->attr_buf1;
                            else read_buf = (char *)seg_config->attr_buf0;

                            offset = (u64_t)i * (u64_t)seg_config->segment_cap * sizeof(VA);
                            if (i == (seg_config->num_segments - 1))
                            {
                                read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA);
                            }
                            else 
                                read_size = (u64_t)(seg_config->segment_cap*sizeof(VA)); 

                            one_io_work = new io_work(gen_config.attr_file_name.c_str(),
                                    FILE_READ, read_buf, offset, read_size);
                            fog_io_queue->add_io_task(one_io_work);
                            fog_io_queue->wait_for_io_task(one_io_work);
                            fog_io_queue->del_io_task(one_io_work);
                            one_io_work = NULL;
                            //PRINT_DEBUG("Finish reading the first segment!\n");
                            p_gather_param->threshold = 1;
                            p_gather_param->strip_id = (int)i;
                            p_gather_param->attr_array_head = (void *)read_buf;

                            buf_index++;
                        }
                        else 
                            PRINT_ERROR("return value is false!\n");

                        gather_cpu_work = new cpu_work<A, VA, U, T>(gather_fog_engine_state, (void *)p_gather_param);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                        //we need to write the attr_buf to attr_file 
                        if (mmap_ret == 1)
                        {
                            if (write_io_work != NULL)
                            {
                                fog_io_queue->wait_for_io_task(write_io_work);
                                fog_io_queue->del_io_task(write_io_work);
                                write_io_work = NULL;
                            }

                            if (i != (seg_config->num_segments-1) )
                            {
                                write_io_work = new io_work(gen_config.attr_file_name.c_str(),
                                        FILE_WRITE, read_buf, 
                                        (u64_t)i * seg_config->segment_cap*sizeof(VA), (u64_t)seg_config->segment_cap*sizeof(VA));
                            }
                            else
                            {
                                write_io_work = new io_work(gen_config.attr_file_name.c_str(),
                                        FILE_WRITE, read_buf, 
                                        (u64_t)i * seg_config->segment_cap*sizeof(VA),
                                        (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA));
                            }

                            fog_io_queue->add_io_task(write_io_work);
                        }
                    }
                    if (write_io_work != NULL)
                    {
                        fog_io_queue->wait_for_io_task(write_io_work);
                        fog_io_queue->del_io_task(write_io_work);
                    }
                    seg_config->buf0_holder = seg_config->buf1_holder = -1;
                }
                else if (signal_of_partition_gather == CONTEXT_GATHER) // means ALL cpus's buffer are FULL
                {
                    //PRINT_DEBUG("CONTEXT gather starts!\n");
                    u32_t num_hits = 0;
                    int partition_gather_array[(gen_config.num_processors)];
                    u32_t tmp_num = 0;
                    /*
                     * If all cpus's update-buf are full, each will return a strip_id.
                     * But the id may be different with each other.
                     * So we need to check which id(segment) will be gather.
                     * Thus, we can remove the repeat one.
                     * As is known to all, cpu-threads will return in random order, 
                     * However, we have used LRU-replacement strategy to handle CONTEXT_GATHER,
                     * so it is important to sort the ids in order to improve the hit-rate
                     */                    
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                    {
                        int tmp_strip_id = -1;
                        if (gather_fog_engine_state == GLOBAL_GATHER)
                        {
                            sched_list_context_data * my_context_data = 
                            seg_config->per_cpu_info_list[i]->global_sched_manager;
                            tmp_strip_id = my_context_data->partition_gather_strip_id;                             
                        }
                        else
                        {
                            assert(gather_fog_engine_state == TARGET_GATHER);
                            context_data * my_context_data = (1-CONTEXT_PHASE) > 0 ? 
                                seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1
                                : seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0;
                            tmp_strip_id = my_context_data->partition_gather_strip_id;                           
                        }


                        //PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                        if (tmp_strip_id == -1)
                            continue;
                        u32_t out_signal = 0;
                        int tmp_id = -1;
                        int ret = -1;

                        if (i > 0)
                        {
                            for (u32_t j = 0; j < i; j++)
                            {
                                if (partition_gather_array[j] == tmp_strip_id)
                                {
                                    out_signal = 1;
                                    break;
                                }
                            }
                            if (out_signal == 0)
                            {
                                ret = lru_hit_target(tmp_strip_id);
                                if (ret == 1)
                                {
                                    if (num_hits == 0)
                                    {
                                        tmp_id = partition_gather_array[0];
                                        partition_gather_array[0] = tmp_strip_id;
                                        partition_gather_array[tmp_num] = tmp_id;
                                        tmp_num++;
                                        num_hits++;
                                    }
                                    else if (num_hits == 1)
                                    {
                                        if (tmp_num == 1)
                                        {
                                            partition_gather_array[1] = tmp_strip_id;
                                            tmp_num++;
                                            num_hits++;
                                        }
                                        else//i > 1
                                        {
                                            tmp_id = partition_gather_array[num_hits];
                                            partition_gather_array[num_hits] = tmp_strip_id;
                                            partition_gather_array[tmp_num] = tmp_id;
                                            tmp_num++;
                                            num_hits++;
                                        }
                                    }
                                    else
                                        PRINT_ERROR("Impossible~\n");
                                }
                                else if (ret == 0)
                                {
                                    partition_gather_array[tmp_num] = tmp_strip_id;
                                    tmp_num++;
                                }
                                else
                                    PRINT_ERROR("error happens here!\n");
                            }
                        }
                        if (i == 0)
                        {
                            assert(out_signal == 0);
                            partition_gather_array[0] = tmp_strip_id;
                            tmp_num++;
                            ret = lru_hit_target(tmp_strip_id);
                            if (ret == 1)
                                num_hits++;
                        }
                    }
                    //for (u32_t i = 0; i < tmp_num; i++)
                    //    PRINT_DEBUG("tmp_strip_id[%d] = %d\n",i ,partition_gather_array[i]);
                    //PRINT_DEBUG("In phase:%d, tmp_num = %d, num_hits = %d\n", phase, tmp_num, num_hits);

                    //gather every strip in this for-loop
                    for (u32_t i = 0; i < tmp_num; i++)
                    {
                        int tmp_strip_id = partition_gather_array[i];
                        //PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                        //PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                        //PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                        int ret = -1;
                        ret = lru_hit_target(tmp_strip_id);
                        if (i > 0)
                        {
                            assert(ret == 1);
                        }
                        if (ret == 1)
                        {
                            //this strip_id hits target
                            //PRINT_DEBUG("strip:%d hits target, no need to do read_io_work~!\n", tmp_strip_id);
                            read_buf = get_target_buf_addr(tmp_strip_id);
                            if (read_buf == NULL)
                                PRINT_ERROR("strip:%d hits target, but something error happen!\n", tmp_strip_id);
                        }
                        else if(ret == 0)
                        {
                            if (i > 0)
                                PRINT_ERROR("It is impossible~!\n");
                            if (phase == (int)0 && i == 0)
                            {
                                assert(seg_config->buf0_holder == -1);
                                assert(seg_config->buf1_holder == -1);
                                seg_config->buf0_holder = tmp_strip_id;//
                                assert(one_io_work == NULL); 
                                read_buf = (char *)seg_config->attr_buf0;
                                if (one_io_work != NULL)
                                {
                                    fog_io_queue->wait_for_io_task(one_io_work);
                                    fog_io_queue->del_io_task(one_io_work);
                                    one_io_work = NULL;
                                }
                                u64_t offset, size;
                                offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                {
                                    size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                }
                                else
                                    size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                        FILE_READ, read_buf, offset, size);
                                fog_io_queue->add_io_task(one_io_work);
                                if (one_io_work != NULL)
                                {
                                    fog_io_queue->wait_for_io_task(one_io_work);
                                    fog_io_queue->del_io_task(one_io_work);
                                    one_io_work = NULL;
                                }
                            }
                            else //if (phase > 0 && i == 0)
                            {
                                assert(i == 0);
                                assert(phase > 0);
                                u32_t num_free_bufs = get_free_buf_num();
                                //PRINT_DEBUG("num_free_bufs = %d\n", num_free_bufs);
                                if (num_free_bufs == 0)
                                {
                                    //PRINT_DEBUG("this is the strip:%d, need to change 1 segment out~!\n", tmp_strip_id);
                                    int changed_strip_id = -1;
                                    //seg_config->buf1_holder will be changed out
                                    //PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                                    //PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                                    if (p_strip_count[seg_config->buf0_holder] >= p_strip_count[seg_config->buf1_holder])
                                    {
                                        changed_strip_id = seg_config->buf1_holder;
                                        seg_config->buf1_holder = tmp_strip_id;
                                    }
                                    else// seg_config->buf0_holder will be changed out
                                    {
                                        changed_strip_id = seg_config->buf0_holder;
                                        seg_config->buf0_holder = tmp_strip_id;
                                    }
                                    //when changing the strip happens
                                    //first, we shold store the changed_strip
                                    write_buf = get_target_buf_addr(tmp_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)changed_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (changed_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_WRITE, write_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    //PRINT_DEBUG("Strip:%d has been changed out!\n", changed_strip_id);

                                    //must wait before next read
                                    read_buf = get_target_buf_addr(tmp_strip_id);
                                    if (read_buf == NULL)
                                        PRINT_ERROR("caOCACOAOCAO\n");
                                    //PRINT_DEBUG("tmp_strip_id = %d\n", tmp_strip_id);
                                    //PRINT_DEBUG("seg_config->buf0_holder = %d, seg_config->buf1_holder = %d\n", seg_config->buf0_holder, seg_config->buf1_holder);
                                    //PRINT_DEBUG("count_seg_config->buf0_holder = %d, count_seg_config->buf1_holder = %d\n", p_strip_count[seg_config->buf0_holder], p_strip_count[seg_config->buf1_holder]);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, read_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (num_free_bufs == 1)
                                {
                                    if (get_free_buf_id() == 1)
                                    {
                                        seg_config->buf0_holder = tmp_strip_id;
                                    }
                                    else if (get_free_buf_id() == 2)
                                    {
                                        seg_config->buf1_holder = tmp_strip_id;
                                    }
                                    read_buf = get_target_buf_addr(tmp_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)tmp_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (tmp_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, read_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (num_free_bufs == 2)
                                    PRINT_ERROR("it is impossible!\n");
                                else
                                    PRINT_ERROR("it is also impossible!\n");
                            }
                        }
                        p_gather_param->attr_array_head = (void *)read_buf;
                        p_strip_count[tmp_strip_id]++;
                        //read next buffer if necessary
                        if ((i+1) < tmp_num )
                        {
                            int next_strip_id = partition_gather_array[i+1];
                            //PRINT_DEBUG("next_strip_id = %d\n", next_strip_id);
                            ret = lru_hit_target(next_strip_id);
                            if (ret == 0)
                            {
                                //PRINT_DEBUG("next buffer need to be loaded~!\n");
                                u32_t tmp_free_buf_nums = get_free_buf_num();
                                if (tmp_free_buf_nums == 2)
                                    PRINT_ERROR("tmp_free_buf_nums = 2, nonono\n");
                                else if (tmp_free_buf_nums == 1)
                                {
                                    if (get_free_buf_id() == 1)
                                    {
                                        seg_config->buf0_holder = next_strip_id;
                                    }
                                    else if (get_free_buf_id() == 2)
                                    {
                                        seg_config->buf1_holder = next_strip_id;
                                    }
                                    else if (get_free_buf_id() == 0)
                                        PRINT_ERROR("Impossible~\n");
                                    
                                    next_buffer = get_target_buf_addr(next_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)next_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (next_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, next_buffer, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                }
                                else if (tmp_free_buf_nums == 0)
                                {
                                    //need to changed out some strips
                                    int changed_strip_id = -1;
                                    if (seg_config->buf0_holder == (int)tmp_strip_id)
                                    {
                                        changed_strip_id = seg_config->buf1_holder;
                                        seg_config->buf1_holder = next_strip_id;
                                    }
                                    else if (seg_config->buf1_holder == (int)tmp_strip_id )
                                    {
                                        changed_strip_id = seg_config->buf0_holder;
                                        seg_config->buf0_holder = next_strip_id;
                                    }
                                    else 
                                        PRINT_ERROR("no strip to change out~!\n");
                                    //PRINT_DEBUG("next_strip_id = %d, need to changed strip:%d out\n", next_strip_id, changed_strip_id);
                                    
                                    //first, we shold store the changed_strip
                                    write_buf = get_target_buf_addr(next_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    u64_t offset, size;
                                    offset = (u64_t)changed_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (changed_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_WRITE, write_buf, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);

                                    next_buffer = get_target_buf_addr(next_strip_id);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }
                                    offset = (u64_t)next_strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
                                    if (next_strip_id == (int)(seg_config->num_segments - 1))
                                    {
                                        size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
                                    }
                                    else
                                        size = (u64_t)(seg_config->segment_cap*sizeof(VA));
                                    one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                                            FILE_READ, next_buffer, offset, size);
                                    fog_io_queue->add_io_task(one_io_work);
                                    if (one_io_work != NULL)
                                    {
                                        fog_io_queue->wait_for_io_task(one_io_work);
                                        fog_io_queue->del_io_task(one_io_work);
                                        one_io_work = NULL;
                                    }

                                }
                            }
                        }
                        p_gather_param->threshold = 1;
                        p_gather_param->strip_id = tmp_strip_id;

                        gather_cpu_work = new cpu_work<A, VA, U, T>(gather_fog_engine_state, (void *)p_gather_param);
                        pcpu_threads[0]->work_to_do = gather_cpu_work;
                        (*pcpu_threads[0])();

                        delete gather_cpu_work;
                        gather_cpu_work = NULL;
                    }
                } 
            }
            //PRINT_DEBUG("After Gather!\n");
        }

        int lru_hit_target(int strip_id)
        {
            if (seg_config->buf0_holder == strip_id || seg_config->buf1_holder == strip_id)
                return 1;
            else
                return 0;
        }

        u32_t get_free_buf_num()
        {
            if (seg_config->buf0_holder == -1 && seg_config->buf1_holder == -1)
                return 2;
            else if (seg_config->buf0_holder != -1 && seg_config->buf1_holder != -1)
                return 0;
            else
                return 1;
        }

        //ret 0:none or two 
        //ret 1:seg_config->buf0_holder
        //ret 2:seg_config->buf1_holder
        u32_t get_free_buf_id()
        {
            if (seg_config->buf0_holder == -1 && seg_config->buf1_holder != -1)
                return 1;
            else if (seg_config->buf0_holder != -1 && seg_config->buf1_holder == -1)
                return 2;
            else 
                return 0;
        }

        char * get_target_buf_addr(int strip_id)
        {
            if (strip_id == -1)
                PRINT_ERROR("Onononono\n");
            if (strip_id == seg_config->buf0_holder)
                return (char *)seg_config->attr_buf0;
            else if (strip_id == seg_config->buf1_holder)
                return (char *)seg_config->attr_buf1;
            else
                return NULL;
        }

        void do_io_work(int strip_id, u32_t operation, char * io_buf, io_work* one_io_work)
        {
            if (one_io_work != NULL)
            {
                fog_io_queue->wait_for_io_task(one_io_work);
                fog_io_queue->del_io_task(one_io_work);
                one_io_work = NULL;
            }
            assert(io_buf != NULL);
            u64_t offset, size;
            offset = (u64_t)strip_id * (u64_t)seg_config->segment_cap * sizeof(VA);
            if (strip_id == (int)(seg_config->num_segments - 1))
            {
                size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap+1)*sizeof(VA); 
            }
            else
                size = (u64_t)(seg_config->segment_cap*sizeof(VA));
            one_io_work = new io_work(gen_config.attr_file_name.c_str(), 
                    operation, io_buf, offset, size);
            fog_io_queue->add_io_task(one_io_work);
        }

        //return:
        //0:The strip_id-buffer of all cpus are ZERO
        //1:some buffer is not ZERO
        u32_t cal_strip_size(int strip_id, u32_t util_rate_signal, u32_t signal_threshold)
        {
            update_map_manager * map_manager;
            u32_t * map_head;
            u32_t strip_cap;
            u32_t total_updates;
            u32_t util_ret = 0; 
            u32_t strip_ret = 0;
            u32_t threshold_ret = 0;
            double util_rate;

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                if (strip_id >= 0 && signal_threshold == 0)
                {
                    for (u32_t j = 0; j < gen_config.num_processors; j++)
                    {
                        if ((*(map_head + strip_id * gen_config.num_processors + j)) != 0)
                        {
                            //PRINT_DEBUG("CPU = %d, strip_id = %d, map_value = %d\n", j, strip_id, 
                                //(*(map_head + strip_id * gen_config.num_processors + j))); 
                            strip_ret = 1;
                            return strip_ret;
                        }
                    }
                }

                if (util_rate_signal == 1 && signal_threshold == 0)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t j = 0; j < (seg_config->num_segments*gen_config.num_processors); j++)
                        total_updates += *(map_head+j);
                    util_rate = (double)total_updates/((double)strip_cap*seg_config->num_segments);
                    //PRINT_DEBUG("THere are %u update in processor %d, utilization rate is %f\n", total_updates, i, util_rate);
                    if (util_rate > THRESHOLD)
                    {
                        util_ret = 1;
                        return util_ret;
                    }
                    
                }

                if (strip_id >= 0  && signal_threshold == 1)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                    {
                            total_updates += (*(map_head + strip_id * gen_config.num_processors + i));
                    }
                    util_rate = (double)total_updates/((double)strip_cap);
                    if (util_rate >= THRESHOLD)
                    {
                        threshold_ret = 1;
                        return threshold_ret;
                    }
                }

                if (strip_id >= 0 && util_rate_signal == 1 && signal_threshold == 1)
                {
                    strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                    total_updates = 0;
                    for (u32_t i = 0; i < gen_config.num_processors; i++)
                        total_updates += (*(map_head + strip_id * gen_config.num_processors + i));
                    util_rate = (double)total_updates/((double)strip_cap);
                    if (util_rate >= MMAP_THRESHOLD)
                        return 1;
                }
            }
            return 0;
        }
        u32_t global_return()
        {
            update_map_manager * map_manager;
            u32_t * map_head;
            u32_t total_updates;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                total_updates = 0;
                for (u32_t j = 0; j < (seg_config->num_segments*gen_config.num_processors); j++)
                    total_updates += *(map_head+j);
                if (total_updates > 0)
                    return 1;
            }
            return 0;
        }

        u32_t cal_threshold()
        {
            update_map_manager * map_manager;
            u32_t * map_head;
            u32_t strip_cap;
            u32_t total_updates;
            u32_t ret = 0; 
            double util_rate;

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                show_update_map(i, map_head);
            }

            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                map_manager = seg_config->per_cpu_info_list[i]->update_manager;
                map_head = map_manager->update_map_head;
                strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;
                total_updates = 0;
                for (u32_t j = 0; j < (seg_config->num_segments*gen_config.num_processors); j++)
                    total_updates += *(map_head+j);

                util_rate = (double)total_updates/((double)strip_cap*seg_config->num_segments);
                //PRINT_DEBUG("THere are %u update in processor %d, utilization rate is %f\n", total_updates, i, util_rate);

                if (util_rate > THRESHOLD)
                {
                    ret = 1;
                }
            }
            return ret=0?1:0;
        }

        void show_update_map(int processor_id, u32_t * map_head)
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

        //map the attribute file
        //return value:
        //0 means success
        //-1 means failure
        int map_attr_file()
        {
            struct stat st;
            char *memblock;

            attr_fd = open(gen_config.attr_file_name.c_str(),O_RDONLY );
            if (attr_fd < 0)
            {
                PRINT_ERROR("fog_engine_scc::map_attr_file cannot open attribute file!\n");
                return -1;
            }
            fstat(attr_fd, &st);
            attr_file_length = (u64_t)st.st_size;

            //memblock = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE|MAP_NORESERVE, attr_fd, 0);
            memblock = (char *)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, attr_fd, 0);
            if (MAP_FAILED == memblock)
            {
                close(attr_fd);
                PRINT_ERROR("index file mapping failed!\n");
                exit(-1);
            }
            attr_array_header = (VA*)memblock;
            return 0;
        }

        //munmap the attribute file
        //return value:
        //0 means success;
        //-1 means failure
        int unmap_attr_file()
        {
            if (munmap((void*)attr_array_header, attr_file_length) == -1)
                PRINT_ERROR("unmap_filer error!\n");
            close(attr_fd);
            return 0;
        }

        int map_write_attr_file()
        {
            struct stat st;
            char *memblock;

            attr_fd = open(gen_config.attr_file_name.c_str(),O_RDWR, S_IRUSR|S_IWUSR);
            if (attr_fd < 0)
            {
                PRINT_ERROR("fog_engine_scc::map_attr_file cannot open attribute file!\n");
                return -1;
            }
            fstat(attr_fd, &st);
            attr_file_length = (u64_t)st.st_size;

            memblock = (char *)mmap(NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, attr_fd, 0);
            if (MAP_FAILED == memblock)
            {
                close(attr_fd);
                PRINT_ERROR("index file mapping failed!\n");
                exit(-1);
            }
            attr_array_header = (VA*)memblock;
            return 0;
        }




        int remap_write_attr_file()
        {
            int ret;
            if (attr_fd)
                if ((ret = unmap_attr_file()) < 0) return ret;
            if ((ret = map_write_attr_file()) < 0) return ret;

            return 0;
        }
        //remap
        int remap_attr_file()
        {
            int ret;
            //PRINT_DEBUG("attr_fd = %d\n", attr_fd);

            if (attr_fd)
                if ( (ret = unmap_attr_file()) < 0 ) return ret;
            if ((ret = map_attr_file()) < 0 ) return ret;

            return 0;
        }

        static void add_schedule(u32_t task_vid, u32_t CONTEXT_PHASE)
        {
            u32_t  partition_id;
            bitmap * current_bitmap = NULL ;
            u32_t max_vert = 0;
            u32_t min_vert = 0;
            int schedule_signal = 0;

            partition_id = VID_TO_PARTITION(task_vid);
            //if (task_vid > gen_config.max_vert_id)
            //    PRINT_DEBUG("task_vid = %d\n", task_vid);
            assert(task_vid <= gen_config.max_vert_id);
            assert(partition_id < gen_config.num_processors);
            sched_bitmap_manager * my_sched_bitmap_manager;
            struct context_data * my_context_data;
            struct context_data * old_context_data;//on scatter!
            bitmap * old_bitmap = NULL;

            my_sched_bitmap_manager = seg_config->per_cpu_info_list[partition_id]->target_sched_manager;
            my_context_data = CONTEXT_PHASE > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;
            u32_t old_phase = 1 - CONTEXT_PHASE;
            old_context_data = old_phase > 0 ? my_sched_bitmap_manager->p_context_data1 : my_sched_bitmap_manager->p_context_data0;

            if (A::set_forward_backward == false && old_context_data->per_bits_true_size > 0)
            {
                assert(old_context_data->per_bits_true_size > 0);
                old_bitmap = old_context_data->p_bitmap;
                //if (old_bitmap->get_value(task_vid) == 1 && task_vid != (old_context_data->per_min_vert_id))
                if (old_bitmap->get_value(task_vid) != 0 && task_vid != (old_context_data->per_min_vert_id))
                    schedule_signal = 1;
            }

            max_vert = my_context_data->per_max_vert_id;
            min_vert = my_context_data->per_min_vert_id;
            current_bitmap = my_context_data->p_bitmap;

            if (current_bitmap->get_value(task_vid) == 0 && schedule_signal == 0)
            {
                my_context_data->per_bits_true_size++;
                current_bitmap->set_value(task_vid);
                if (task_vid <= min_vert)
                {
                    min_vert = task_vid;
                    my_context_data->per_min_vert_id = min_vert;
                }
                if (task_vid >= max_vert)
                {
                    max_vert = task_vid;
                    my_context_data->per_max_vert_id = max_vert;
                }
            }
        }
		void reclaim_everything()
		{
			PRINT_DEBUG( "begin to reclaim everything\n" );
			//reclaim pre-allocated space

            if (global_or_target != GLOBAL_ENGINE)
                for(u32_t i=0; i<gen_config.num_processors; i++)
                {
                    delete seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->p_bitmap;
                    delete seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->p_bitmap;
                    PRINT_DEBUG("Delete bitmap!\n");
                }

			munlock( buf_for_write, gen_config.memory_size );
			munmap( buf_for_write, gen_config.memory_size );

			//terminate the cpu threads
			pcpu_threads[0]->terminate = true;
			pcpu_threads[0]->work_to_do = NULL;
			(*pcpu_threads[0])();

			for(u32_t i=1; i<gen_config.num_processors; i++) {
		        boost_pcpu_threads[i]->join();
			}
			for(u32_t i=0; i<gen_config.num_processors; i++)
				delete pcpu_threads[i];


			//terminate the disk thread
			delete fog_io_queue;

			//destroy the vertices mapping
			delete vert_index;

			delete seg_config;

			PRINT_DEBUG( "everything reclaimed!\n" );
		}

		//will browse all sched_lists and list the tasks.
		/*void show_all_sched_tasks()
		{
			sched_bitmap_manager* target_sched_manager;

			PRINT_DEBUG( "==========================	Browse all scheduled tasks	==========================\n" );
			//browse all cpus
			for(u32_t i=0; i<gen_config.num_processors; i++){
				target_sched_manager = seg_config->per_cpu_info_list[i]->target_sched_manager;
				PRINT_DEBUG( "Processor %d: Number of scheduled tasks: %d, Details:\n", i,
					target_sched_manager->sched_task_counter );
				show_sched_list_tasks( target_sched_manager );
			}
			PRINT_DEBUG( "==========================	That's All	==========================\n" );
		}*/
	


		//Note:
		// Should show this configuration information at each run, especially when
		//	it is not debugging.
		// TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.
		void show_target_sched_update_buf()
		{
			PRINT_DEBUG( "===============\tsched_update buffer for each CPU begin\t=============\n" );
			PRINT_DEBUG( "CPU\tSched_bitmap_manger\tUpdate_map_manager\n");
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG("%d\t0x%llx\t0x%llx\n",
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->target_sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager
                    );

			PRINT_DEBUG( "------------------\tschedule bitmap manager\t---------------\n" );
			PRINT_DEBUG( "------------------\tcontext data0\t---------------\n" );
			PRINT_DEBUG( "CPU\tbitmap_buf_head\tper_bitmap_buf_size per_bits_true_size edges max_id min_id\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t\t%d\t%d\t%d\t%d\t%d\n", i,
                        (u64_t)(seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bitmap_buf_head),
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bitmap_buf_size, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bits_true_size, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_num_edges, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_max_vert_id, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_min_vert_id 
                        );
			PRINT_DEBUG( "------------------\tcontext data1\t---------------\n" );
			PRINT_DEBUG( "CPU\tbitmap_buf_head\tper_bitmap_buf_size per_bits_true_size edges max_id min_id\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t\t%d\t%d\t%d\t%d\t%d\n", i,
                        (u64_t)(seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bitmap_buf_head),
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bitmap_buf_size, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bits_true_size, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_num_edges, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_max_vert_id, 
                        seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_min_vert_id 
                        );

			PRINT_DEBUG( "------------------\tupdate manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tUpdate_map_address\tUpdate_map_size\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_head,
					(u32_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_size );

			/*PRINT_DEBUG( "------------------\tauxiliary update buffer manager\t------------\n" );
			PRINT_DEBUG( "CPU\tBuffer_begin\tBuffer_size\tUpdate_head\tBuf_cap\tNum_updates\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_size,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->update_head,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_cap,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->num_updates );*/

			PRINT_DEBUG( "------------------\tstrip buffer\t------------\n" );
			PRINT_DEBUG( "CPU\tStrip_buffer\tBuffer_size\tStrip_cap\tPer_cpu_strip_cap\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->strip_buf_len,
					seg_config->per_cpu_info_list[i]->strip_cap,
					seg_config->per_cpu_info_list[i]->strip_cap/gen_config.num_processors );

			PRINT_DEBUG( "===============\tsched_update buffer for each CPU end\t=============\n" );
		}


        //LATOUT of the management data structure of fog_engine_scc at the beginning of sched_update buffer;
        //designed by hejian
		//	(ordered by logical address)
		//	----------------------
		//	| sched_bitmap_manager          |
        //	---------------------------------
        //	| sched_list_manager(scc)       | add for scc
        //	----------------------------
		//	| update_map_manager	        |
		//	----------------------------
		//	| update_map(update_map_size)   |
		//	------------------------------
        //	| bitmap0                       |
        //	-------------------------------
        //	| bitmap1                       |
        //	-------------------------------
		//	| update_buffer(strips)		    |
		//	-------------------------------
		

		void target_init_sched_update_buf()
		{
            u32_t update_map_size, total_header_len;
            u64_t strip_buf_size, strip_size/*, aux_update_buf_len*/;
            u32_t strip_cap;
            u32_t bitmap_buf_size; //bitmap_max_size;
            //io_work* init_bitmap_io_work = NULL;
            u32_t total_num_vertices;
            u32_t per_bitmap_buf_size;


			update_map_size = seg_config->num_segments 
					* gen_config.num_processors 
					* sizeof(u32_t);
            total_num_vertices = gen_config.max_vert_id + 1;

            bitmap_buf_size = (u32_t)((ROUND_UP(total_num_vertices, 8))/8);
            per_bitmap_buf_size = (u32_t)(ROUND_UP(((ROUND_UP(total_num_vertices, 8))/8), gen_config.num_processors)/gen_config.num_processors);
            bitmap_buf_size = (u32_t)(per_bitmap_buf_size * gen_config.num_processors);
            PRINT_DEBUG("num_vertices is %d, the origin bitmap_buf_size is %lf\n", total_num_vertices, (double)(total_num_vertices)/8);
            PRINT_DEBUG("After round up, the ROUND_UP(total_num_verttices,8) = %d\n", ROUND_UP(total_num_vertices, 8));
            PRINT_DEBUG("the bitmap_buf_size is %d, per_bitmap_buf_size is %d\n", bitmap_buf_size, per_bitmap_buf_size);

            //PRINT_DEBUG("the bitmap_file_size is %d\n", bitmap_file_size);
            //PRINT_DEBUG("the sched_bitmap_size is %d\n", sched_bitmap_size);

			total_header_len = sizeof(sched_bitmap_manager) 
             //       + sizeof(sched_list_context_data)
                    + sizeof(context_data)*2
					+ sizeof(update_map_manager)
					+ update_map_size
					;

			//PRINT_DEBUG( "init_sched_update_buffer of fog_engine_scc of sched_bitmap_manager:%lu,  size of update map manager:%lu\n", 
			//	sizeof(sched_bitmap_manager), sizeof(update_map_manager));
			//PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u, bitmap_buf_size:%u, total_head_len:%u\n", 
			//	update_map_size, bitmap_buf_size, total_header_len );
            //PRINT_DEBUG("sizeof(context_data) = %ld\n", sizeof(context_data));

			//total_header_length should be round up according to the size of updates.
			total_header_len = ROUND_UP( total_header_len, sizeof(update<U>) );

			//populate the buffer managers
			for(u32_t i=0; i<gen_config.num_processors; i++)
            {
                seg_config->per_cpu_info_list[i]->target_sched_manager = 
                    (sched_bitmap_manager*)seg_config->per_cpu_info_list[i]->buf_head;
            
                seg_config->per_cpu_info_list[i]->global_sched_manager = NULL;
                    //(sched_list_context_data *)((u64_t)seg_config->per_cpu_info_list[i]->buf_head
                    //    +sizeof(sched_bitmap_manager));
                seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0 = 
                    (context_data *)((u64_t)seg_config->per_cpu_info_list[i]->buf_head
                        +sizeof(sched_bitmap_manager));
          
            //context data address
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1 = 
                (context_data *)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + sizeof(sched_bitmap_manager)
                        + sizeof(context_data));

            seg_config->per_cpu_info_list[i]->update_manager = 
                (update_map_manager*)(
                            (u64_t)seg_config->per_cpu_info_list[i]->buf_head + sizeof(sched_bitmap_manager)
                            + 2*sizeof(context_data) );
            //populate the update map manager, refer to types.hpp
            seg_config->per_cpu_info_list[i]->update_manager->update_map_head = 
                            (u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + sizeof(sched_bitmap_manager)
                            + 2*sizeof(context_data)
                            + sizeof(update_map_manager) );

            seg_config->per_cpu_info_list[i]->update_manager->update_map_size =
                update_map_size;
            //populate target_sched_manager, refer to types.hpp, new struct for fog_engine_scc hejian
            //also initilization the bitmap buffer
            //bitmap address
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bitmap_buf_head = 
                (char*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len);
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bitmap_buf_head = 
                (char*)((u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len + per_bitmap_buf_size);

            //build the strips
            seg_config->per_cpu_info_list[i]->strip_buf_head = //the first strip
                (char*)(ROUND_UP( 
                    (u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len
                    + per_bitmap_buf_size * 2,
                    sizeof(update<U>) ));

            strip_buf_size = seg_config->per_cpu_info_list[i]->buf_size - total_header_len - per_bitmap_buf_size * 2 ;

            //divide the update buffer to "strip"s
            //round down strip size
            strip_size = ROUND_DOWN( strip_buf_size / seg_config->num_segments, 
                (sizeof(update<U>)*gen_config.num_processors) );
            strip_cap = (u32_t)(strip_size / sizeof(update<U>));

            seg_config->per_cpu_info_list[i]->strip_buf_len = strip_size;
            seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;

            
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bitmap_buf_size = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bitmap_buf_size = per_bitmap_buf_size;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bits_true_size = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bits_true_size = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_min_vert_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_min_vert_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_max_vert_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_max_vert_id = 0;

            //u32_t tmp_min_offset = gen_config.min_vert_id%gen_config.num_processors;
            //PRINT_DEBUG("tmp_min_offset = %d\n", tmp_min_offset);
            u32_t tmp_max_offset = gen_config.max_vert_id%gen_config.num_processors;
            //PRINT_DEBUG("tmp_max_offset = %d\n", tmp_max_offset);
            //u32_t tmp_min_value;
            u32_t tmp_max_value;

            //if (i == tmp_min_offset)
            //    tmp_min_value = gen_config.min_vert_id;
            //else if (i < tmp_min_offset)
            //    tmp_min_value = gen_config.min_vert_id + gen_config.num_processors - tmp_min_offset + i;
            //else //(i > tmp_min_offset)
            //    tmp_min_value = gen_config.min_vert_id - tmp_min_offset + i;

            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_max_vert_id =
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_max_vert_id = i;

            if (i == tmp_max_offset)
                tmp_max_value = gen_config.max_vert_id;
            else if(i < tmp_max_offset)
                tmp_max_value = gen_config.max_vert_id - tmp_max_offset + i;
            else //(i > tmp_max_offset)
                tmp_max_value = gen_config.max_vert_id - gen_config.num_processors - tmp_max_offset + i;

            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_min_vert_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_min_vert_id = tmp_max_value;

            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_num_edges = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_num_edges = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->signal_to_scatter = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->signal_to_scatter = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->p_bitmap_steal = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->p_bitmap_steal = NULL;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_virt_cpu_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_virt_cpu_id = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_num_virt_cpus = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_num_virt_cpus = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_bits_true_size = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_bits_true_size = 0;

            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->steal_context_edge_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->steal_context_edge_id = 0;

            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->partition_gather_signal = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->partition_gather_signal = 0;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->partition_gather_strip_id = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->partition_gather_strip_id = -1;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->will_be_updated = false;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->next_p_bitmap_steal = NULL;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->will_be_updated = false;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->next_p_bitmap_steal = NULL;
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->next_steal_bits_true_size = 
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->next_steal_bits_true_size = 0;


            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->p_bitmap = new bitmap(
                    seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data0->per_bitmap_buf_head, 
                    per_bitmap_buf_size,
                    per_bitmap_buf_size*8,
                    i,
                    tmp_max_value,
                    i,
                    gen_config.num_processors);
            seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->p_bitmap = new bitmap(
                    seg_config->per_cpu_info_list[i]->target_sched_manager->p_context_data1->per_bitmap_buf_head, 
                    per_bitmap_buf_size,
                    per_bitmap_buf_size*8,
                    i,
                    tmp_max_value,
                    i,
                    gen_config.num_processors);
        }
     
			show_target_sched_update_buf();
		}
        

        //global-function
        //Note:
        // Should show this configuration information at each run, especially when
        //  it is not debugging.
        // TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.
        void show_global_sched_update_buf()
        {
            PRINT_DEBUG( "===============\tsched_update buffer for each CPU begin\t=============\n" );
            PRINT_DEBUG( "CPU\tSched_list_man\tUpdate_map_man\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ )
                PRINT_DEBUG( "%d\t0x%llx\t0x%llx\n", 
                    i,
                    (u64_t)seg_config->per_cpu_info_list[i]->global_sched_manager,
                    (u64_t)seg_config->per_cpu_info_list[i]->update_manager);

            /*PRINT_DEBUG( "------------------\tschedule manager\t---------------\n" );
            PRINT_DEBUG( "CPU\tSched_list_head\tSched_list_tail\tSched_list_current\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ )
                PRINT_DEBUG( "%d\t0x%llx\n", i,
                    (u64_t)seg_config->per_cpu_info_list[i]->sched_manager->head);
                    //(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->tail,
                    //(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->current);*/

            PRINT_DEBUG( "------------------\tupdate manager\t---------------\n" );
            PRINT_DEBUG( "CPU\tUpdate_map_address\tUpdate_map_size\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ )
                PRINT_DEBUG( "%d\t0x%llx\t%u\n", 
                    i,
                    (u64_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_head,
                    (u32_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_size );

            /*PRINT_DEBUG( "------------------\tauxiliary update buffer manager\t------------\n" );
            PRINT_DEBUG( "CPU\tBuffer_begin\tBuffer_size\tUpdate_head\tBuf_cap\tNum_updates\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ )
                PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t%u\t%u\n", 
                    i,
                    (u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
                    (u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_size,
                    (u64_t)seg_config->per_cpu_info_list[i]->aux_manager->update_head,
                    (u32_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_cap,
                    (u32_t)seg_config->per_cpu_info_list[i]->aux_manager->num_updates );*/

            PRINT_DEBUG( "------------------\tstrip buffer\t------------\n" );
            PRINT_DEBUG( "CPU\tStrip_buffer\tBuffer_size\tStrip_cap\tPer_cpu_strip_cap\n" );
            for( u32_t i=0; i<gen_config.num_processors; i++ )
                PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t%u\t%u\n", 
                    i,
                    (u64_t)seg_config->per_cpu_info_list[i]->strip_buf_head,
                    (u64_t)seg_config->per_cpu_info_list[i]->strip_buf_len,
                    seg_config->per_cpu_info_list[i]->strip_cap,
                    seg_config->per_cpu_info_list[i]->strip_cap/gen_config.num_processors );

            PRINT_DEBUG( "===============\tsched_update buffer for each CPU end\t=============\n" );
        }

        //Initialize the sched_update buffer for all processors.
        //Embed the data structure in the buffer, 
        //  seg_config->per_cpu_info_list[i] only stores the pointers of the 
        //  managment data structure, which is stored at the beginning of
        //  the sched_update buffer.
        //Layout of the management data structure at the beginning of sched_update buffer:
        //  (ordered by logical address)
        //  ---------------------------------  new version by hejian @ 2014-8-11
        //  | sched_list_manager            |  sched_list_context_data
        //  ---------------------------------
        //  | update_map_manager            |
        //  ---------------------------------
        //  | update_map(update_map_size)   |
        //  ---------------------------------
        //  | sched_list(sched_list_size)   | NULL
        //  ---------------------------------
        //  | update_buffer(strips)         |
        //  ---------------------------------
        
        void global_init_sched_update_buf()
        {
            u32_t update_map_size,  total_header_len;
            u64_t strip_buf_size, strip_size;//, aux_update_buf_len;
            u32_t strip_cap;

            update_map_size = seg_config->num_segments 
                    * gen_config.num_processors 
                    * sizeof(u32_t);

            total_header_len = sizeof(sched_list_context_data) 
                    + sizeof(update_map_manager)
                    + update_map_size;

            //PRINT_DEBUG( "init_sched_update_buffer--size of sched_list_context_data:%lu, size of update map manager:%lu\n", 
            //    sizeof(sched_list_context_data), sizeof(update_map_manager)/*, sizeof(aux_update_buf_manager<VA>)*/ );
            //PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u,  total_head_len:%u\n", 
             //   update_map_size, total_header_len );
            //populate the buffer managers
            for(u32_t i=0; i<gen_config.num_processors; i++){
                //headers
                seg_config->per_cpu_info_list[i]->global_sched_manager = 
                    (sched_list_context_data *)seg_config->per_cpu_info_list[i]->buf_head;

                seg_config->per_cpu_info_list[i]->update_manager = 
                    (update_map_manager*)(
                                (u64_t)seg_config->per_cpu_info_list[i]->buf_head 
                                + sizeof(sched_list_context_data) );

                //populate the update map manager, refer to types.hpp
                seg_config->per_cpu_info_list[i]->update_manager->update_map_head = 
                    (u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->update_manager
                        + sizeof(update_map_manager));

                seg_config->per_cpu_info_list[i]->update_manager->update_map_size =
                    update_map_size;

                //populate sched_manager
                memset(seg_config->per_cpu_info_list[i]->global_sched_manager, 0, sizeof(sched_list_context_data));

                //zero out the update buffer and sched list buffer
                memset( seg_config->per_cpu_info_list[i]->update_manager->update_map_head, 
                    0, 
                    update_map_size );

                //build the strips
                seg_config->per_cpu_info_list[i]->strip_buf_head = //the first strip
                    (char*)(ROUND_UP( 
                        (u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len,
                        sizeof(update<U>) ));

                strip_buf_size = seg_config->per_cpu_info_list[i]->buf_size - total_header_len;

                //divide the update buffer to "strip"s
                //round down strip size
                strip_size = ROUND_DOWN( strip_buf_size / seg_config->num_segments, 
                    (sizeof(update<U>)*gen_config.num_processors) );
                strip_cap = (u32_t)(strip_size / sizeof(update<U>));

                seg_config->per_cpu_info_list[i]->strip_buf_len = strip_size;
                seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;
            }
            show_global_sched_update_buf();
        }

        void add_sched_task_to_processor( u32_t processor_id, sched_task *task, u32_t task_len )
        {
            //assert *task point to a valid task
            assert(processor_id < gen_config.num_processors );

            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->normal_sched_min_vert = task->start;
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->normal_sched_max_vert = task->term;
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->normal_sched_vert_to_scatter = task_len;
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->num_vert_to_scatter = task_len;

            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->context_vert_id =
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->context_edge_id = 0;

            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->context_steal_max_vert = 
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->context_steal_min_vert = 0;
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->context_steal_num_vert = 0;
            seg_config->per_cpu_info_list[processor_id]->global_sched_manager->signal_to_scatter = 0;

            delete task;
        }
        
        //The "task" may be very huge, e.g., [0, max_vert_id],
        //re-written by hejian
        //divide tasks by partition,namely:
        //cpu0 : 0 4 8 ...
        //cpu1 : 1 5 9 ...
        //cpu2 : 2 6 10 ...
        //cpu3 : 3 7 11 ...
        void add_all_task_to_cpu( sched_task * task )
        {
            //PRINT_DEBUG( "first adding all tasks to all cpu :task from %d to %d.\n", task->start, task->term );
            
            if( task->term == 0 ){
                //PRINT_DEBUG("task->term == 0;\n");
                assert( task->start <= gen_config.max_vert_id );
                add_sched_task_to_processor( VID_TO_PARTITION(task->start), task, 1 );
                return;
            }
            assert( task->start <= task->term );
            assert( task->term <= gen_config.max_vert_id );

            u32_t start_remain = (task->start)%gen_config.num_processors;
            u32_t end_remain = (task->term)%gen_config.num_processors;
            //PRINT_DEBUG("start_remain = %d, end_remain = %d\n", start_remain, end_remain);
            sched_task* p_task;
            for (u32_t i = 0; i < gen_config.num_processors; i++)
            {
                p_task = new sched_task;
                if (i == start_remain)
                    p_task->start = task->start;
                else if (i < start_remain)
                    p_task->start = task->start + gen_config.num_processors + i - start_remain;
                else if (i > start_remain)
                    p_task->start = task->start + i - start_remain;

               // PRINT_DEBUG("the start of processor %d is %d\n", i, p_task->start);
                if (i == end_remain)
                    p_task->term = task->term;
                else if (i < end_remain)
                    p_task->term = task->term - end_remain + i;
                else if(i > end_remain)
                    p_task->term = task->term - gen_config.num_processors - end_remain + i;
               // PRINT_DEBUG("the term of processor %d is %d\n", i, p_task->term);
                assert(((p_task->term - i)%gen_config.num_processors) == 0);
                assert(((p_task->start - i)%gen_config.num_processors) == 0);
                u32_t p_task_len = (p_task->term - p_task->start)/gen_config.num_processors + 1;
               // PRINT_DEBUG("task length of processor %d is %d\n", i, p_task_len);

                add_sched_task_to_processor( i, p_task, p_task_len );
            }
            delete task;
            return;
        }

		static void *map_anon_memory( u64_t size,
                             bool mlocked,
                             bool zero = false)
		{
			void *space = mmap(NULL, size > 0 ? size:4096,
                     PROT_READ|PROT_WRITE,
                     MAP_ANONYMOUS|MAP_SHARED, -1, 0);
			PRINT_DEBUG( "Engine::map_anon_memory had allocated 0x%llx bytes at %llx\n", size, (u64_t)space);

			if(space == MAP_FAILED) {
			    std::cerr << "mmap_anon_mem -- allocation " << "Error!\n";
			    exit(-1);
			}
		  	if(mlocked) {
				if(mlock(space, size) < 0) {
			        std::cerr << "mmap_anon_mem -- mlock " << "Error!\n";
			    }
		  	}
			if(zero) {
		    	memset(space, 0, size);
		  	}
			return space;
        }
	
};
template <typename A, typename VA, typename U, typename T>
index_vert_array<T> * fog_engine<A, VA, U, T>::vert_index;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::scatter_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::init_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::gather_fog_engine_state;

template <typename A, typename VA, typename U, typename T>
u32_t fog_engine<A, VA, U, T>::current_attr_segment;

template <typename A, typename VA, typename U, typename T>
segment_config<VA> * fog_engine<A, VA, U, T>::seg_config;

template <typename A, typename VA, typename U, typename T>
io_queue * fog_engine<A, VA, U, T>::fog_io_queue;

#endif
