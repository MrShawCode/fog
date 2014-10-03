Readme
---------------------------------------------------------------------
Two programs are included in this software package: 
"convert" and "fog"

---------------------------------------------------------------------
Prerequisites:
basic build tools (e.g., stdlibc, gcc, etc);
g++;
libboost and libboost-dev 1.46.1 or higher.

Our test machine:
Ubuntu 12.04.4 LTS, 3.5.0-54-generic kernel, x86_64

---------------------------------------------------------------------
1) "convert"
"convert" is used to convert the SNAP files into 
binary files that are going to be used by "fog".

a) How to get it?
$ make convert

b) Parameters:
-h Help messages

-g The original SNAP file (in edgelist or adjlist format). 
They are assumed to  be sorted by source vertex ID.

-t The type of the original SNAP file, i.e., two possibilities: 
edgelist or adjlist

-d Destination folder. Remember to add a slash at the end of 
your dest folder:
e.g., "/home/yourname/data/", "./data/"

-o The output file type. FOG defines two type of files as X-Stream. 
The type1 file includes a float type weight, which is a random 
value between 1 and 10.
Type2 file will NOT include the weight value. Note, the randomly 
generated weight value will be with the outedge array only.

-i Will you need the in-edge array? two possiblities:
true and false. If true, the in-edge array will be generated. 

c) Example usage:
$ convert -g ../source-graph/twitter_rv.net -t edgelist 
 -d ../dest-graph/ -o type1

#This command will generate 4 files in the "../dest-graph/" folder:
twitter_rv.net.desc  --  this is the description file
twitter_rv.net.index --  this is the out-edge-idx file
twitter_rv.net.edge  --  this is the out-edge-array file(with weight)
twitter_rv.net.attr     --  this is the vertex attribute array

$ convert -g ../source-graph/twitter_rv.net -t edgelist 
-d ../dest-graph/ -o type2 -i true

#This command will generate 6 files in the "../dest-graph/" folder:
twitter_rv.net.desc  --  this is the description file
twitter_rv.net.index --  this is the out-edge-idx file
twitter_rv.net.edge  --  this is the out-edge-array file(no weight)
twitter_rv.net.attr  --  this is the vertex attribute array
twitter_rv.net.in-index --  this is the in-edge-idx file
twitter_rv.net.in-edge  --  this is the in-edge-array file

---------------------------------------------------------------------
2) "fog"

"fog" is the main program that process your graph data

a) How to get it?
$ make

b) Parameters
-h Help messages

-g The name of the graph to be processed (Note, pls give the .desc file)

-a The application algorithm to be executed. Example programs 
include: SSSP, PageRank, WCC, SpMV, SCC

-i With or without in-edges. Currently, only WCC and SCC needs the 
in-edge. If not needed, give "-i false"

-m The memory provision (in MBs). Note, this is the size of statically 
allocated memory. FOG (OS actually) will use all remaining system 
memory to read the out/in edges as well as the vertex attributes from
disk.

-p The number of processors to be used during the computation. 

-d The number of disk threads to be invoked during execution.

--pagerank::niters This parameter only takes effect when execut PageRank.

--sssp::source This parameter only takes effect when execute SSSP.

c) Example use:

$ sudo fog -g ../dest-graph/twitter_rv.desc -a sssp --sssp::source 12 -m 1280

#This program will conduct SSSP algorithm on previously generated (the 
first command) twitter graph. The single source is vertex 12, and the 
size of the statically allocated memory is 1280MB. 
Four CPU threads and two IO threads (default value) will be invoked 
during the execution. 

$ sudo fog -g /home/ssd/cook-data/twitter_rv.desc -a scc -i true -m 4096

#This program will conduct SCC algorithm on previously generated (the 
second command) twitter graph. The size of statical generated memory is
4096MB. Four CPU threads and two IO threads (default value) will be invoked
during execution. This program needs to traverse the graph by in-edges.

#Note, as fog will lock the statically allocated memory to prevent replace-
ment, it will be better for an algorithm to run by "sudo" rights.

---------------------------------------------------------------------
End of README, enjoy!

