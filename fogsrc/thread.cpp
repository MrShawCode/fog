#include "thread.hpp"
#include "stdio.h"
#include "stdlib.h"

void fog_thread::operator()()
{
	int i;
	for( i=0; i<3; i++ )
		printf( "I am thread %d\n", thread_id );
}

