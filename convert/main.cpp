/*
 * The purpose of this small utility program is to convert existing snap (edge list) file to binary format.
 * More specifically, the binary type1 format, which is actually called "type1 compact" format.
--------------------------------------------------------------------------------
2        | Only edge lists. Entries are tuples of the form: 
COMPACT  | <4 byte src, 4 byte dst, 4bytes weight>
--------------------------------------------------------------------------------
 * This format is used, since most available "big" graph data files from Internet today are in snap format,
 * and the range of vertices are [0, 4G], which is true for even very big snap files.
 * Therefore, I use a pair of unsigned int to represent each of the vertices.
 */

#include "options_utils.h"
#include <cassert>
#include <fstream>

boost::program_options::options_description desc;
boost::program_options::variables_map vm;

//statistic data below
unsigned int min_vertex_id=100000, max_vertex_id=0;
unsigned long long num_edges=0;
unsigned long tmp_max_out = 0;
//vert_gap indicates how many vertices are actually missing
unsigned int vert_gap=0;
std::ofstream desc_file;
std::ofstream desc_file1;

extern void begin_process(const char* ,const char* ,const char*, const char * , const char *, const char *);

int main( int argc, const char**argv)
{
	unsigned int pos;
	std::string input_file_name, short_name, temp;
	std::string out_dir, out_type1_file_name, 
		out_index_file_name, out_desc_file_name, out_desc_file1_name;
    std::string  vertex_buffer_file_name ;
    std::string  vertex_state_buffer_file_name0 ;
    std::string  vertex_state_buffer_file_name1 ;

	//setup options
	setup_options( argc, argv );

	input_file_name = temp = vm["graph"].as<std::string>();
	pos = temp.find_last_of( "/" );
	temp = temp.substr( pos+1 );
	pos = temp.find_last_of( "." );
	short_name = temp.substr(0,pos);
	temp = temp.substr( pos+1 );
	std::cout << "remaining:" << temp << "\n";
	assert( temp.compare("type1") != 0 );

	out_dir = vm["destination"].as<std::string>();
	out_type1_file_name = out_dir+short_name+".type1";
	out_index_file_name = out_dir+short_name+".index";
    vertex_buffer_file_name = out_dir+short_name + ".buf";
    vertex_state_buffer_file_name0 = out_dir+short_name + ".state0";
    vertex_state_buffer_file_name1 = out_dir+short_name + ".state1";
	out_desc_file_name = out_dir+short_name+".desc";
	out_desc_file1_name = out_type1_file_name + ".ini";

	std::cout << "Input file:" << input_file_name << "\n";
	std::cout << "Output type1 file:" << out_type1_file_name  << "\n";
	std::cout << "Output index file:" << out_index_file_name  << "\n";
	std::cout << "Output desc file:" << out_desc_file_name  << "\n";
	std::cout << "Output desc file:" << out_desc_file1_name  << "\n";
	std::cout << "vertex_buffer_file_name :" << vertex_buffer_file_name << "\n";
	std::cout << "vertex_state_buffer_file_name0 :" << vertex_state_buffer_file_name0 << "\n";

	begin_process( input_file_name.c_str(), out_type1_file_name.c_str(), out_index_file_name.c_str(), vertex_buffer_file_name.c_str() ,
            vertex_state_buffer_file_name0.c_str(), vertex_state_buffer_file_name1.c_str());
    


	desc_file.open( out_desc_file_name.c_str() );
	desc_file << "[description]\n";
	desc_file << "min_vertex_id = " << min_vertex_id << "\n";
	desc_file << "max_vertex_id = " << max_vertex_id << "\n";
	desc_file << "num_of_edges = " << num_edges << "\n";
	desc_file << "tot_vertex_gap = " << vert_gap << "\n";
	desc_file << "max_out_edges = " << tmp_max_out << "\n";
    desc_file.close();
    desc_file1.open( out_desc_file1_name.c_str() );
    desc_file1 << "[graph]\n";
    desc_file1 << "type = 1 " << "\n";
    desc_file1 << "name = " << (out_type1_file_name) << "\n";
    desc_file1 << "vertices = " << (max_vertex_id - min_vertex_id + 1) << "\n";
    desc_file1 << "edges = " << num_edges << "\n";
	desc_file1.close();
}


