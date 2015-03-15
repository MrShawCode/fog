/**************************************************************************************************
 * Authors: 
 *   Jian He, Huiming Lv
 *
 * Declaration:
 *   For debuggings and message output.
 *
 * Notes:
 *   1.remove the LOG_FILE_NAME by Huiming Lv   2014/12/21
 *   2.add PRINT_DEBUG_LOG by Huiming Lv   2014/12/25
 *************************************************************************************************/

#ifndef __PRINT_DEBUG__HPP__
#define __PRINT_DEBUG__HPP__

#include "types.hpp"
//Variadic Macros By hejian.

#include <stdio.h>  
#include <stdlib.h>  
#include <sys/types.h>

#include <sys/stat.h>
#include <fcntl.h>
  

//#define LOG_FILE_NAME  "print.log"
#define __PRINT__DEBUG__  2
/*
 * the values for __PRINT_DEBUG__
 * 0 means printing the debug information to the stderr
 * 1 means writing the debug information to the log_file.
 * 2 means both of them.
 */

/*
 * for example:
 * PRINT_DEBUG("The speed of light in a vacuum in octal: %c = %om/s", 'c', 299792458);
 * will export the statement : File: macro_print.cpp, Line: 00031: The speed of light in a vacuum in octal: c = 2167474112m/s
 */

#if __PRINT__DEBUG__ == 0
extern FILE *log_file;
#define PRINT_DEBUG(format,...) do {fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;}while(0)
#define PRINT_SHORT(format,...) do {fprintf(stderr, ""format"", ##__VA_ARGS__) ;}while(0)
#define PRINT_ERROR(format,...) do {fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
                                    fprintf(stderr, "This process is killed by yourself! Beacuse you have used PRINT_ERROR");\
                                    exit(-1);\
}while(0)
#define PRINT_WARNING(format,...) do {fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;}while(0)

#elif __PRINT__DEBUG__ == 1
extern FILE *log_file;
#define PRINT_DEBUG(format,...) do {fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;}while(0)
#define PRINT_SHORT(format,...) do {fprintf(log_file, ""format"",##__VA_ARGS__) ;}while(0)

#define PRINT_ERROR(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "This process is killed by yourself! Beacuse you have used PRINT_ERROR");\
    exit(-1);\
}while(0)

#define PRINT_WARNING(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
}while(0)

#elif __PRINT__DEBUG__ == 2
extern FILE *log_file;
extern FILE *test_log_file;
extern FILE *cv_log_file;
#define PRINT_DEBUG(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
}while(0)
#define PRINT_SHORT(format,...) do { \
    fprintf(log_file, ""format"", ##__VA_ARGS__) ;\
    fprintf(stderr, ""format"", ##__VA_ARGS__) ;\
}while(0)


#define PRINT_ERROR(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "This process is killed by yourself! Beacuse you have used PRINT_ERROR");\
    exit(-1);\
}while(0)
 
#define PRINT_WARNING(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
    fprintf(stderr, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
}while(0)

#define PRINT_DEBUG_LOG(format,...) do { \
    fprintf(log_file, "File: "__FILE__", Line: %05d: "format"", __LINE__, ##__VA_ARGS__) ;\
}while(0)

#define PRINT_DEBUG_TEST_LOG(format,...) do { \
    fprintf(test_log_file, format, ##__VA_ARGS__) ;\
}while(0)

#define PRINT_DEBUG_CV_LOG(format,...) do { \
    fprintf(cv_log_file, format, ##__VA_ARGS__) ;\
}while(0)

#else  
#define PRINT_DEBUG(format,...)  
#define PRINT_WARNING(format,...)  
#endif // the endif of "if __PRINT__DEBUG__ == 0 && if __PRINT__DEBUG__ == 1"

  
// the main bellow will show you how to use this macro
/*int main(int argc, char **argv) {  
    if (!(log_file = fopen(LOG_FILE, "w"))) //open file for mode
    {
        printf("failed to open %s.\n", LOG_FILE);
        exit(666);
    }
    char str[]="Hello World";  
    PRINT_DEBUG("A ha, check me: %s",str);  
    PRINT_DEBUG("This is the %dnd test.", 2);
    PRINT_DEBUG("The two smallest primes are %d and %i", 2, 3);
    PRINT_DEBUG("The speed of light in a vacuum in octal: %c = %om/s", 'c', 299792458);
    PRINT_DEBUG("This is all PRINT_DEBUGged to %s", LOG_FILE);
    PRINT_DEBUG("Of course, we can use further formatting--pi is close to %2.2f", 3.14159);
    PRINT_DEBUG("Avogadro's number is %e, or if you prefer %G", 6.02257e23, 6.02257e23);
    return 0;  
}*/

#endif//end of #ifndef __CONFIG_H__
