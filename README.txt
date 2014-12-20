This project contains two programs: "convert" and "fog"

"convert": The program that converts SNAP (edgelist and adjlist) files to binary forms to be 
	further	processed by fog.
"fog": The FOG engine. You can program and execute graph algorithms with it. 

---------------------------------------  Prerequisites for building  ------------------------------
Linux (Ubuntu 12.04.4 LTS, 3.5.0-54-generic kernel, x86_64 for our case);
basic build tools (e.g., stdlibc, gcc, etc);
g++ (4.6.3 for our case);
libboost and libboost-dev 1.46.1 (our case) or higher.

---------------------------------------  Explanations to convert  ---------------------------------
1) Where is the source code?
	see {Project root}/convert/

2) How to build it?
	{Project root}$ make convert

3) How to use it?
	There are some parameters for "convert":

    -h Help messages

    -g The original SNAP file (in edgelist or adjlist format). 
        They are assumed to be sorted by source vertex ID.

    -t The type of the original SNAP file, i.e., two possibilities: edgelist or adjlist

    -d Destination folder. Remember to add a slash at the end of your dest folder:
        e.g., "/home/yourname/data/" or "./data/"
                                  ^            ^

    -o The output file type. FOG defines two type of files as X-Stream. 
        The type1 file includes a float type weight, WHICH IS A RANDOM VALUE BETWEEN 1 AND 10.
        Type2 file will NOT include the weight value.  

    -i Will you need the in-edge array? two possiblities: true(1) and false(0). 
        If true(1), the in-edge array will be generated. 

4) Example usage:
    {Project root}$ sudo convert -g ../source-graph/twitter_rv.net -t edgelist -d ../dest-graph/ 
        -o type1
    #This command will generate 4 files in "../dest-graph/" folder:
    twitter_rv.net.desc  --  this is the description file
    twitter_rv.net.index --  this is the out-edge-idx file
    twitter_rv.net.edge  --  this is the out-edge-array file (WITH RANDOM WEIGHT!) 
    twitter_rv.net.attr  --  this is the vertex attribute array

    {Project root}$ sudo convert -g ../source-graph/twitter_rv.net -t edgelist -d ../dest-graph/ 
        -o type2 -i 1
    #This command will generate 6 files in the "../dest-graph/" folder:
    twitter_rv.net.desc  --  this is the description file
    twitter_rv.net.index --  this is the out-edge-idx file
    twitter_rv.net.edge  --  this is the out-edge-array file (NO WEIGHT!)
    twitter_rv.net.attr  --  this is the vertex attribute array 
    twitter_rv.net.in-index --  this is the in-edge-idx file
    twitter_rv.net.in-edge  --  this is the in-edge-array file

---------------------------------------  Explanations to "fog"  ---------------------------------
1) Where is the source code?
    see {Project root}/fogsrc/

2) How to build it?
	{Project root}$ make

3) How to add my code?
    You can refer to the examples in {Project root}/application directory,and write your own code
  in a new file (say, foo.hpp) at the same directory. After that, revise {Project root}/fogsrc/main.cpp
  at line 23 to include foo.hpp, and the tail part of the "start_engine" function to add your algorithm.

4) How to use it?
	There are some parameters for "fog":

    -h Help messages

    -g The name of the graph to be processed (Note, pls give the .desc file)

    -a The application algorithm to be executed. Example programs 
        example apps include: SSSP(sssp), PageRank(pagerank), WCC(cc), SpMV(spmv), BFS(bfs) and SCC(scc)

    -i With or without in-edges. Currently, only WCC(cc) and SCC(scc) needs the in-edge file. 
        If not needed, give "-i 0", or simply not give "-i" parameter.

    -m The memory provision (in MBs). Note, this is the size of statically allocated memory. 
        FOG (OS actually) will use all remaining system memory to read the out/in edges as well as the 
        vertex attributes from disk. In order to prevent swap-out the statically allocated memory, you
        need the "sudo" rights!

    -p The number of processors participating the computation. 

    -d The number of disk threads to be invoked during execution.

    --pagerank::niters This parameter only takes effect when execut PageRank.

    --sssp::source This parameter only takes effect when execute SSSP.

    --bfs::bfs-root This parameter only takes effect when execute BFS.

5) Example use:

    {Project root}$ sudo fog -g ../dest-graph/twitter_rv.desc -a sssp --sssp::source 12 -m 1280

    #This program will conduct SSSP algorithm on previously generated twitter graph. 
     The single source is vertex 12, and the size of the statically allocated memory is 1280MB. 
     Four CPU threads and two IO threads (default value) will be invoked during the execution. 

    {Project root}$ sudo fog -g /home/ssd/cook-data/twitter_rv.desc -a cc -i 1 -m 4096 -p 8 -d 4

    #This program will conduct WCC algorithm on previously generated twitter graph. 
     The size of statical generated memory is 4096MB (i.e., 4GB). Eight CPU threads and four IO 
     threads will be invoked during execution. This program needs to traverse the graph by in-edges.

---------------------------------------------------------------------
The end. enjoy!

