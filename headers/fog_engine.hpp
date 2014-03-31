//declaration of fog_engine class
// The features of this class:
// 1) Schedule list with fixed size (defined by a MACRO)
// 2) donot need to sort and merge the scheduled tasks, just FIFO.

#ifndef __FOG_ENGINE_H__
#define __FOG_ENGINE_H__

#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <cassert>

#include "config.hpp"
#include "index_vert_array.hpp"
#include "disk_thread.hpp"
#include "cpu_thread.hpp"
#include "print_debug.hpp"

#define SCHED_BUFFER_LEN	1024

//A stands for the algorithm (i.e., ???_program)
//VA stands for the vertex attribute structure
template <typename A, typename VA>
class fog_engine{
		//global variables
		static segment_config<VA> *seg_config;
		static index_vert_array* vert_index;
		char* buf_for_write;

		static u32_t fog_engine_state;
		static u32_t current_attr_segment;

		//io work queue
		io_queue* fog_io_queue;

		cpu_thread<A,VA> ** pcpu_threads;
		boost::thread ** boost_pcpu_threads;
		
		//The reasons to use another mmaped file to access the attribute file (in SCATTER phase):
		//	It is really hard if not possible to arrange the attribute buffer by repeatively reading
		//	the attribute file in/replace, since there may be different status among the cpu threads.
		//	For ex., cpu0 may need to access segment 1, while other cpu threads need to access the 
		//	segment 2. 
		//	The other reason is that, since file reading and buffer replacing is done intermediatively,
		//	there will be (and must be) a waste at the last step. 
		//	Think about the case that cpu threads filled up their update buffer, and ready to finish
		//	their current SCATTER phase. But remember, at this time, there is another file reading 
		//	conducting on the background, which is useless and the following steps (i.e., GATHER)
		//	must wait till the completion of this background operation.
		int attr_fd;
		u64_t attr_file_length;
		VA *attr_array_header;

	public:

		fog_engine()
		{
			//create the index array for indexing the out-going edges
			vert_index = new index_vert_array;
			verify_vertex_indexing();

			//allocate buffer for writting
			buf_for_write = (char*)map_anon_memory( gen_config.memory_size, true, true );

			//config the buffer for writting
			seg_config = new segment_config<VA>( (const char*)buf_for_write );

			//create io queue
			fog_io_queue = new io_queue;

			//assume buffer_for_write is aligned, although it is possible that it is NOT! TODO

			//create cpu threads
			pcpu_threads = new cpu_thread<A,VA> *[gen_config.num_processors];
			boost_pcpu_threads = new boost::thread *[gen_config.num_processors];
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				pcpu_threads[i] = new cpu_thread<A,VA>(i, vert_index, seg_config);
				if( i>0 )	//Do not forget, "this->" is thread 0
					boost_pcpu_threads[i] = new boost::thread( boost::ref(*pcpu_threads[i]) );
			}

			attr_fd = 0;

			init_sched_update_buf();
		}
			
		~fog_engine(){
			reclaim_everything();
		}

		void operator() ()
		{
			int ret;

			init_phase();
			//start scatter & gather
			while(1){
				ret = scatter_updates();

				show_update_buf_util();
				clear_update_buf_all_cpu();

				gather_updates();

				if( ret == 0 ) break;
			}

		}

		void init_phase()
		{
			//initilization loop, loop for seg_config->num_segment times,
			// each time, invoke cpu threads that called A::init to initialize
			// the value in the attribute buffer, dump the content to file after done,
			// then swap the attribute buffer (between 0 and 1)
			cpu_work<A,VA>* new_cpu_work = NULL;
			io_work* init_io_work = NULL;
			char * buf_to_dump = NULL;
			init_param* p_init_param=new init_param;

			PRINT_DEBUG( "fog engine operator is called, conduct init phase for %d times.\n", seg_config->num_segments );
			current_attr_segment = 0;
			fog_engine_state = INIT;
			for( u32_t i=0; i < seg_config->num_segments; i++ ){
				//which attribute buffer should be dumped to disk?
				if ( current_attr_segment%2== 0 ) buf_to_dump = (char*)seg_config->attr_buf0;
				else buf_to_dump = (char*)seg_config->attr_buf1;
		
				//create cpu threads
				if( i != (seg_config->num_segments-1) ){
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = seg_config->segment_cap;
					new_cpu_work = new cpu_work<A,VA>( INIT, 
						(void*)p_init_param );
				}else{	//the last segment, should be smaller than a full segment
					p_init_param->attr_buf_head = buf_to_dump;
					p_init_param->start_vert_id = seg_config->segment_cap*i;
					p_init_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap;
					new_cpu_work = new cpu_work<A,VA>( INIT, 
						(void*)p_init_param );
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
					init_io_work = new io_work( FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)seg_config->segment_cap*sizeof(VA) );
				}else{
					init_io_work = new io_work( FILE_WRITE, 
						buf_to_dump, 
						(u64_t)i*seg_config->segment_cap*sizeof(VA),
						(u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA) );
				}

				//activate the disk thread
				fog_io_queue->add_io_task( init_io_work );

				current_attr_segment++;
			}
	
			//the INIT phase finished now, ALTHOUGH the last write is still on its way. 
			// Do NOT wait!

			//should add tasks here, when the disk is busy.
            sched_task *t_task = new sched_task;
 
            t_task->start = 0;
            t_task->term = gen_config.max_vert_id;
 
            add_schedule_balance( t_task );

			show_all_sched_tasks();
			
			//FOLLOWING BELONGS TO THE INIT PHASE! wait till the last write work is finished.
			fog_io_queue->wait_for_io_task( init_io_work );
			fog_io_queue->del_io_task( init_io_work );
			PRINT_DEBUG( "fog engine finished initializing attribute files!\n" );
			//ABOVE BELONGS TO THE INIT PHASE! wait till the last write work is finished.
		}

		//scater_updates: 
		// scatter updates to update buffer, till update buffer filled, or no more sched tasks
		// return value:
		// 0: no more sched tasks
		// 1: update buffer full.
		// -1: failure
		int scatter_updates()
		{
			int ret;
			cpu_work<A,VA>* scatter_cpu_work = NULL;
			scatter_param* p_scatter_param=new scatter_param;

			if( remap_attr_file() < 0 ){
				PRINT_ERROR( "Fog_engine::scatter_updates failed!\n" );
				return -1;
			}

			p_scatter_param->attr_array_head = (void*)attr_array_header;

			scatter_cpu_work = new cpu_work<A,VA>( SCATTER, (void*)p_scatter_param );

			pcpu_threads[0]->work_to_do = scatter_cpu_work;
			(*pcpu_threads[0])();
			//cpu threads finished init current attr buffer
			delete scatter_cpu_work;
			scatter_cpu_work = NULL;

			//after computation, check the status of cpu threads, and return
			PRINT_DEBUG( "After scatter computation\n" );
			ret = 0;
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				PRINT_DEBUG( "Processor %d status %d\n", i, pcpu_threads[i]->status );
				if ( pcpu_threads[i]->status != NO_MORE_SCHED )
					ret = 1;
			}

			return ret;

		}

		//gather_updates:
		// gather all updates in the update buffer.
		// this function is rather simple, therefore, no return results.
		void gather_updates()
		{
/*
			cpu_work<A,VA>* scatter_cpu_work = NULL;
			io_work * read_io_work = NULL;
			char * next_buffer = NULL, *read_buf = NULL;
			u32_t begin_with;
			u64_t offset, read_size;
			scatter_param* p_scatter_param=new scatter_param;

			begin_with = find_min_sched_segment();
			assert( begin_with < seg_config->num_segments );

			PRINT_DEBUG( "seg_config->num_segments = %d\n", seg_config->num_segments );
			for( u32_t i=begin_with; i<seg_config->num_segments; i++ ){
				PRINT_DEBUG( "i=%d, read_io_work=%llx\n", i, read_io_work );

				if (i%2) read_buf = (char*)seg_config->attr_buf1;
				else read_buf = (char*)seg_config->attr_buf0;

				if ( read_io_work == NULL ){

					offset = (u64_t)i*(u64_t)seg_config->segment_cap*sizeof(VA);
					if( i!=(seg_config->num_segments-1) )
						read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA);
					else
						read_size = (u64_t)seg_config->segment_cap*sizeof(VA);

					read_io_work = new io_work( FILE_READ, 
						read_buf, 
						offset,
						read_size );
					fog_io_queue->add_io_task( read_io_work );

					fog_io_queue->wait_for_io_task( read_io_work );
					fog_io_queue->del_io_task( read_io_work );
					PRINT_DEBUG( "fog engine finished reading the first segment!\n" );
				}else{
					//wait the completion of previously issued io work, 
					// and issue another one
					fog_io_queue->wait_for_io_task( read_io_work );
					fog_io_queue->del_io_task( read_io_work );
					
					read_buf = next_buf;
				}
	
				//issue another io work if applicable
				if( (i+1) < seg_config->num_segments ){
					//next_buf MUST be different from read_buf
					if((i+1)%2) next_buf = (char*)seg_config->attr_buf1;
					else next_buf = (char*)seg_config->attr_buf0;

					offset = (u64_t)(i+1)*(u64_t)seg_config->segment_cap*sizeof(VA);
					if( (i+1)!=(seg_config->num_segments-1) )
						read_size = (u64_t)seg_config->segment_cap*sizeof(VA);
					else
						read_size = (u64_t)(gen_config.max_vert_id%seg_config->segment_cap)*sizeof(VA);

					read_io_work = new io_work( FILE_READ, 
						next_buf, 
						offset,
						read_size );
					fog_io_queue->add_io_task( read_io_work );
					PRINT_DEBUG( "will invoke next read io work, offset %llu, size:%llu\n", offset, read_size );
				}

				//now begin computation to overlap with the undergoing reading...
				//Remember to ONLY use read_buf!
				p_scatter_param->start_vert_id = seg_config->segment_cap*i;
				p_scatter_param->start_vert_id = seg_config->segment_cap*i;
				if( i != (seg_config->num_segments-1) )
					p_scatter_param->num_of_vertices = seg_config->segment_cap;
				else	//the last segment, should be smaller than a full segment
					p_scatter_param->num_of_vertices = gen_config.max_vert_id%seg_config->segment_cap;

				scatter_cpu_work = new cpu_work<A,VA>( SCATTER, (void*)p_scatter_param );

				pcpu_threads[0]->work_to_do = scatter_cpu_work;
				(*pcpu_threads[0])();
				//cpu threads finished init current attr buffer
				delete scatter_cpu_work;
				scatter_cpu_work = NULL;

				//after computation, check the status of cpu threads, and return
				PRINT_DEBUG( "After scatter computation\n" );
				for( u32_t i=0; i<gen_config.num_processors; i++ )
					PRINT_DEBUG( "Processor %d status %d\n", i, pcpu_threads[i]->status );

				//before returning, make sure to delete the previously issued io read task!
			}
			return 0;
*/
		}

		//compute the utilization rate of update buffers (i.e., all processors)
		void show_update_buf_util( void )
		{
			update_map_manager* map_manager;
			u32_t* map_head;
			u32_t strip_cap;
			u32_t total_updates;

			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				map_manager = seg_config->per_cpu_info_list[i]->update_manager;
				map_head = map_manager->update_map_head;
				strip_cap = seg_config->per_cpu_info_list[i]->strip_cap;

				total_updates = 0;
				for( u32_t j=0; j<(seg_config->num_segments*gen_config.num_processors); j++ )
					total_updates += *(map_head+j);

				//print the utilization status
				PRINT_DEBUG( "There are %u update in processor %d, utilization rate is:%f\n", 
					total_updates, i, (double)total_updates/((double)strip_cap*seg_config->num_segments) );
			}

			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				map_manager = seg_config->per_cpu_info_list[i]->update_manager;
				map_head = map_manager->update_map_head;
				show_update_map( i, map_head );
			}
		}

	    void show_update_map( int processor_id, u32_t* map_head )
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

		//clear the update buffer for all processors,
		//	this should be done after gathering.
		void clear_update_buf_all_cpu( void )
		{
			update_map_manager* map_manager;
			u32_t* map_head;

			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				map_manager = seg_config->per_cpu_info_list[i]->update_manager;
				map_head = map_manager->update_map_head;

				memset( map_head, 0, map_manager->update_map_size );
			}
		}

		//map the attribute file
		//return value:
		// 0 means success;
		// -1 means failure.
		int map_attr_file()
		{
		    struct stat st;
    		char * memblock;

			attr_fd = open( gen_config.attr_file_name.c_str(), O_RDONLY );
			if( attr_fd < 0 ){
				PRINT_ERROR( "Fog_engine::map_attr_file Cannot open attribute file!\n" );
				return -1;
			}
	
	    	fstat( attr_fd, &st );
	    	attr_file_length = (u64_t) st.st_size;

		    memblock = (char*) mmap( NULL, st.st_size, PROT_READ|PROT_WRITE, MAP_PRIVATE | MAP_NORESERVE, attr_fd, 0 );
		    if( memblock == MAP_FAILED ){
		        PRINT_ERROR( "index file mapping failed!\n" );
		        exit( -1 );
		    }

			attr_array_header = (VA*) memblock;
			return 0;
		}

		//map the attribute file
		//return value:
		// 0 means success;
		// -1 means failure.
		int unmap_attr_file()
		{
			munmap( (void*)attr_array_header, attr_file_length );
			close( attr_fd );
			return 0;
		}

		//remap the attribute file
		//return value:
		// 0 means success;
		// -1 means failure.
		int remap_attr_file()
		{
			int ret;

			if( attr_fd )
				if( (ret = unmap_attr_file()) < 0 ) return ret;

			if( (ret = map_attr_file()) < 0 ) return ret;

			return 0;
		}

		void reclaim_everything()
		{
			PRINT_DEBUG( "begin to reclaim everything\n" );
			//reclaim pre-allocated space
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

		//Note:
		// Should show this configuration information at each run, especially when
		//	it is not debugging.
		// TODO: replace PRINT_DEBUG with PRINT_WARNING or something always show output.
		void show_sched_update_buf()
		{
			PRINT_DEBUG( "===============\tsched_update buffer for each CPU begin\t=============\n" );
			PRINT_DEBUG( "CPU\tSched_list_man\tUpdate_map_man\tAux_update_man\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager );

			PRINT_DEBUG( "------------------\tschedule manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tSched_list_head\tSched_list_tail\tSched_list_current\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\n", i,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->head,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->tail,
					(u64_t)seg_config->per_cpu_info_list[i]->sched_manager->current);

			PRINT_DEBUG( "------------------\tupdate manager\t---------------\n" );
			PRINT_DEBUG( "CPU\tUpdate_map_address\tUpdate_map_size\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_head,
					(u32_t)seg_config->per_cpu_info_list[i]->update_manager->update_map_size );

			PRINT_DEBUG( "------------------\tauxiliary update buffer manager\t------------\n" );
			PRINT_DEBUG( "CPU\tBuffer_begin\tBuffer_size\tUpdate_head\tBuf_cap\tNum_updates\n" );
			for( u32_t i=0; i<gen_config.num_processors; i++ )
				PRINT_DEBUG( "%d\t0x%llx\t0x%llx\t0x%llx\t%u\t%u\n", 
					i,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_size,
					(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->update_head,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_cap,
					(u32_t)seg_config->per_cpu_info_list[i]->aux_manager->num_updates );

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
		//	seg_config->per_cpu_info_list[i] only stores the pointers of the 
		//	managment data structure, which is stored at the beginning of
		//	the sched_update buffer.
		//Layout of the management data structure at the beginning of sched_update buffer:
		//	(ordered by logical address)
		//	---------------------------------
		//	| sched_list_manager			|
		//	---------------------------------
		//	| update_map_manager			|
		//	---------------------------------
		//	| aux_update_buf_manager		|
		//	---------------------------------
		//	| update_map(update_map_size)	|
		//	---------------------------------
		//	| sched_list(sched_list_size)	|
		//	---------------------------------
		//	| update_buffer(strips)			|
		//	---------------------------------
		
		void init_sched_update_buf()
		{
			u32_t update_map_size, sched_list_size, total_header_len;
			u64_t strip_buf_size, strip_size, aux_update_buf_len;
			u32_t strip_cap;

			update_map_size = seg_config->num_segments 
					* gen_config.num_processors 
					* sizeof(u32_t);

			sched_list_size = SCHED_BUFFER_LEN * sizeof(sched_task);
			
			total_header_len = sizeof(sched_list_manager) 
					+ sizeof(update_map_manager)
					+ sizeof(aux_update_buf_manager<VA>)
					+ update_map_size
					+ sched_list_size;

			PRINT_DEBUG( "init_sched_update_buffer--size of sched_list_manager:%lu, size of update map manager:%lu, size of aux_update buffer manager:%lu\n", 
				sizeof(sched_list_manager), sizeof(update_map_manager), sizeof(aux_update_buf_manager<VA>) );
			PRINT_DEBUG( "init_sched_update_buffer--update_map_size:%u, sched_list_size:%u, total_head_len:%u\n", 
				update_map_size, sched_list_size, total_header_len );
/*
			//total_header_length should be round up according to the size of updates.
			total_header_len = ROUND_UP( total_header_len, sizeof(update<VA>) );

			//	CPU0 should always exist!
			strip_buf_size = seg_config->per_cpu_info_list[0]->buf_size - total_header_len;

			//divide the update buffer to "strip"s
			strip_size = strip_buf_size / seg_config->num_segments;
			//round down strip size
			strip_size = ROUND_DOWN( strip_size, (sizeof(update<VA>)*gen_config.num_processors) );
			strip_cap = (u32_t)(strip_size / sizeof(update<VA>));

*/
			aux_update_buf_len = seg_config->aux_update_buf_len / gen_config.num_processors;
			//populate the buffer managers
			for(u32_t i=0; i<gen_config.num_processors; i++){
				//headers
				seg_config->per_cpu_info_list[i]->sched_manager = 
					(sched_list_manager*)seg_config->per_cpu_info_list[i]->buf_head;

				seg_config->per_cpu_info_list[i]->update_manager = 
					(update_map_manager*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_list_manager) );

				seg_config->per_cpu_info_list[i]->aux_manager = 
					(aux_update_buf_manager<VA>*)(
								(u64_t)seg_config->per_cpu_info_list[i]->buf_head 
								+ sizeof(sched_list_manager)
								+ sizeof(update_map_manager) );

				//build the strips
				seg_config->per_cpu_info_list[i]->strip_buf_head = //the first strip
					(char*)(ROUND_UP( 
						(u64_t)seg_config->per_cpu_info_list[i]->buf_head + total_header_len,
						sizeof(update<VA>) ));

				strip_buf_size = seg_config->per_cpu_info_list[i]->buf_size - total_header_len;

				//divide the update buffer to "strip"s
				//round down strip size
				strip_size = ROUND_DOWN( strip_buf_size / seg_config->num_segments, 
					(sizeof(update<VA>)*gen_config.num_processors) );
				strip_cap = (u32_t)(strip_size / sizeof(update<VA>));

				seg_config->per_cpu_info_list[i]->strip_buf_len = strip_size;
				seg_config->per_cpu_info_list[i]->strip_cap = strip_cap;

				//populate the update map manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->update_manager->update_map_head = 
					(u32_t*)((u64_t)seg_config->per_cpu_info_list[i]->aux_manager 
						+ sizeof(aux_update_buf_manager<VA>));

				seg_config->per_cpu_info_list[i]->update_manager->update_map_size =
					update_map_size;
	
				//populate sched_manager, refer to types.hpp
				seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_head =
					(sched_task*)(
								(u64_t)seg_config->per_cpu_info_list[i]->aux_manager
								+ sizeof(aux_update_buf_manager<VA>) 
								+ update_map_size );

				seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_size = 
								SCHED_BUFFER_LEN;

				seg_config->per_cpu_info_list[i]->sched_manager->sched_task_counter = 0;
				seg_config->per_cpu_info_list[i]->sched_manager->head = 
				seg_config->per_cpu_info_list[i]->sched_manager->tail = 
				seg_config->per_cpu_info_list[i]->sched_manager->current = 
					seg_config->per_cpu_info_list[i]->sched_manager->sched_buf_head;

				//zero out the update buffer and sched list buffer
				memset( seg_config->per_cpu_info_list[i]->update_manager->update_map_head, 
					0, 
					update_map_size + sched_list_size );
	
				//populate the auxiliary update buffer manager
				seg_config->per_cpu_info_list[i]->aux_manager->buf_head =
					seg_config->aux_update_buf + i* aux_update_buf_len;
						
				seg_config->per_cpu_info_list[i]->aux_manager->buf_size =
					aux_update_buf_len;
					//should round down&up to make the buffer fitable for updates
				seg_config->per_cpu_info_list[i]->aux_manager->update_head = 
					(update<VA>*)ROUND_UP( 
								(u64_t)seg_config->per_cpu_info_list[i]->aux_manager->buf_head,
								sizeof( update<VA> ) );
				seg_config->per_cpu_info_list[i]->aux_manager->buf_cap =
					(u32_t)(aux_update_buf_len/sizeof( update<VA> ));
				seg_config->per_cpu_info_list[i]->aux_manager->num_updates = 0;
			}
			show_sched_update_buf();
		}

		//find the minimal segment (to begin reading) from the sched lists of each PCPU
		u32_t find_min_sched_segment()
		{
			u32_t ret=0, start_with;
			for( u32_t i=0; i<gen_config.num_processors; i++ ){
				start_with = seg_config->per_cpu_info_list[i]->sched_manager->current->start;

				if( VID_TO_SEGMENT( start_with ) < ret )
					ret = VID_TO_SEGMENT( start_with );
			}
			return ret;
		}

		void show_sched_list_tasks( sched_list_manager *sched_manager )
		{
			sched_task* head = sched_manager->head;
			sched_task* p_task = head;

			for( u32_t i=0; i<sched_manager->sched_task_counter; i++ ){
				PRINT_DEBUG( "Task %d: Start from :%d to:%d\n", i, p_task->start, p_task->term );
				if( (p_task - sched_manager->sched_buf_head) > sched_manager->sched_buf_size )
					p_task = sched_manager->sched_buf_head;
				else
					p_task++;
			}
		}

		//will browse all sched_lists and list the tasks.
		void show_all_sched_tasks()
		{
			sched_list_manager* sched_manager;

			PRINT_DEBUG( "==========================	Browse all scheduled tasks	==========================\n" );
			//browse all cpus
			for(u32_t i=0; i<gen_config.num_processors; i++){
				sched_manager = seg_config->per_cpu_info_list[i]->sched_manager;
				PRINT_DEBUG( "Processor %d: Number of scheduled tasks: %d, Details:\n", i,
					sched_manager->sched_task_counter );
				show_sched_list_tasks( sched_manager );
			}
			PRINT_DEBUG( "==========================	That's All	==========================\n" );
		}
	
		void test_add_sched_tasks( )
		{
			sched_task* t_task=new sched_task;
			u32_t temp_vid;
			t_task->start = gen_config.max_vert_id/4;
			t_task->term = gen_config.max_vert_id/2;
			PRINT_DEBUG( "test_add_schedule: will add a new task, starts %u term %u. cpu number:%u\n", 
				t_task->start, t_task->term, gen_config.num_processors );

			temp_vid = 1073741824;
			PRINT_DEBUG( "VID = %u, the SEGMENT ID=%d, CPU = %d\n", temp_vid, VID_TO_SEGMENT(temp_vid), VID_TO_PARTITION(temp_vid) );
					
			//KNOWN PROBLEM: assert does NOT work!
			//test if "assert" works in add_sched_task_to_processor
			//add_sched_task_to_processor( 6, &t_task );

			add_schedule_balance( t_task );

			//will show all scheduled tasks of all processors
			show_all_sched_tasks();
		}

		//this member function is NOT re-enterable! since memory state will change
		//returns: true means successful, false means failure
		//note: do NOT delete the task object when exiting
		bool add_sched_task_to_ring_buf( sched_list_manager* sched_manager, sched_task* task )
		{
/*			PRINT_DEBUG( "add sched task to ring buffer, with sched_list_manager:%llx\t", 
				(u64_t) sched_manager );
			PRINT_DEBUG( "head:0x%llx,tail:0x%llx,current:0x%llx",
				(u64_t)sched_manager->head, (u64_t)sched_manager->tail, 
				(u64_t)sched_manager->current );
			PRINT_DEBUG( "number of schedule list counter:%u, sched_buf_size:%u",
				sched_manager->sched_task_counter, sched_manager->sched_buf_size );
*/
			if( (sched_manager->sched_task_counter+1) > sched_manager->sched_buf_size )
				return false;

			*(sched_manager->tail) = *task;
			//moving rounds
			//remember: sched_manager->pointers(sched_buf_head, tail,etc) are sched_tasks
			if( sched_manager->tail >= (sched_manager->sched_buf_head 
						+ sched_manager->sched_buf_size) )
					sched_manager->tail = sched_manager->sched_buf_head;
			else
					sched_manager->tail++;

			sched_manager->sched_task_counter++;
			return true;
		}

		void add_sched_task_to_processor( u32_t processor_id, sched_task *task )
		{
			//assert *task point to a valid task
			assert(processor_id < gen_config.num_processors );

			//now add the task
//			PRINT_DEBUG( "add_sched_task_to_processor: will add sched task from:%u to:%u at CPU:%u", task->start, task->term, processor_id );

			if( !add_sched_task_to_ring_buf( seg_config->per_cpu_info_list[processor_id]->sched_manager, task ) )
				PRINT_ERROR( "add_sched_task_to_processor failed on adding the task to ring buffer!\n" );
			delete task;
		}

		//The "task" may be very huge, e.g., [0, max_vert_id],
		// should fragment it before adding it to the sched_list of processors
		void add_schedule_balance( sched_task * task )
		{
			PRINT_DEBUG( "add_schedule_balance:Will add schedule from %d to %d.\n", task->start, task->term );
			
			if( task->term == 0 ){
				assert( task->start <= gen_config.max_vert_id );
				add_sched_task_to_processor( VID_TO_PARTITION(task->start), task );
				return;
			}

			assert( task->start <= task->term );
			assert( task->term <= gen_config.max_vert_id );
			u32_t task_len = task->term - task->start;
			double factor = (double)task_len/gen_config.num_processors;
			u32_t per_cpu_task_len = (u32_t)factor + 1;

			sched_task* p_task;
			for( u32_t i=0; i<gen_config.num_processors; i++ ){	
				p_task = new sched_task;
				p_task->start = task->start + per_cpu_task_len * i;
				if( i == (gen_config.num_processors -1) )
					p_task->term = task->term;
				else
					p_task->term = p_task->start + per_cpu_task_len - 1;

				add_sched_task_to_processor( i, p_task );
			}
			delete task;
			return;
		}

		//The "task" may be very huge, e.g., [0, max_vert_id],
		// should fragment it before adding it to the sched_list of processors
		void add_schedule_parted( sched_task * task )
		{
			PRINT_DEBUG( "add_schedule_parted:Will add schedule from %d to %d.\n", task->start, task->term );
			
			if( task->term == 0 ){
				assert( task->start <= gen_config.max_vert_id );
				add_sched_task_to_processor( VID_TO_PARTITION(task->start), task );
				return;
			}

			assert( task->start <= task->term );
			assert( task->term <= gen_config.max_vert_id );
			u32_t i, j;
			sched_task* p_task;

//			PRINT_DEBUG( "segment start:%u, term:%u, cpu start:%u, term:%u", VID_TO_SEGMENT(task->start), VID_TO_SEGMENT(task->term), VID_TO_PARTITION(task->start), VID_TO_PARTITION(task->term) );
			for( i=VID_TO_SEGMENT(task->start); i<=VID_TO_SEGMENT(task->term); i++ ){	
				//loop for segment times
				//handle the task from task->start to the end of segment i (i.e., seg_config->segment_cap*i).
				for( j=0; j<gen_config.num_processors; j++ ){ //loop for number of processors
					//just find common parts between [task->start, task->term] and
					//[START_VID(i,j), TERM_VID(i,j)]
					if( TERM_VID(i,j) < task->start ) continue;
					if( START_VID(i,j) > task->term ) continue;

					//there will be common part(s)
					p_task = new sched_task;
					p_task->start = (task->start>START_VID(i,j))?task->start:START_VID(i,j);
					p_task->term = (task->term>TERM_VID(i,j))?TERM_VID(i,j):task->term;
					add_sched_task_to_processor( j, p_task );
				}
			}
			delete task;
			return;
		}

		//==================================	follow functions handle update buffer	===========================

		//==================================	auxiliary functions below	===========================
		//this is a self-proving member function, just make sure vert_index is correctly set.
		void verify_vertex_indexing()
		{
			//TODO:
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

template <typename A, typename VA>
index_vert_array* fog_engine<A,VA>::vert_index;

template <typename A, typename VA>
u32_t fog_engine<A,VA>::fog_engine_state;

template <typename A, typename VA>
u32_t fog_engine<A,VA>::current_attr_segment;

template <typename A, typename VA>
segment_config<VA> * fog_engine<A,VA>::seg_config;

#endif
