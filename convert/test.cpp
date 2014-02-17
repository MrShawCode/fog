/*
	this is a temporary test program source
*/

#include <iostream>

#include <cassert>
#include <fstream>

#include "type.h"
#include "stdlib.h"
#include "stdio.h"

int main( int argc, const char**argv)
{
	edge t_edge;
	vertex_index t_vertex;
	
	printf( "sizeof int is:%d\n", sizeof(unsigned int) );
	printf( "sizeof double is:%d\n", sizeof(double) );
	printf( "sizeof long long is:%d\n", sizeof(long long) );

	std::cout << "test program\n";
	std::cout << "sizeof(edge) = " << sizeof(edge) << "\n";
	std::cout << "sizeof(vertex) = " << sizeof(vertex_index) << "\n";
}


