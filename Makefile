# path definition
OBJECT_DIR = obj
CONVERT_PATH = convert
FOGLIB_PATH = foglib
BINARY_PATH = bin
HEADERS_PATH = headers

#compile options
SYSLIBS = -L/usr/local/lib -lboost_system -lboost_program_options -lboost_thread -lz -lrt
CXX?= g++
CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -I./$(HEADERS_PATH)
CXXFLAGS+= -Wfatal-errors

# make selections
CONVERT_SRC = main.o read_lines.o
CONVERT_OBJS= $(addprefix $(OBJECT_DIR)/, $(CONVERT_SRC))
CONVERT_TARGET=$(BINARY_PATH)/convert

TEST_SRC = convert.o
TEST_OBJS= $(addprefix $(OBJECT_DIR)/, $(TEST_SRC))
TEST_TARGET=$(BINARY_PATH)/test

FOGLIB_SRC = program.o
FOGLIB_OBJS= $(addprefix $(OBJECT_DIR)/, $(FOGLIB_SRC))
FOGLIB_TARGET= $(FOGLIB_PATH)/libfog.a

all: $(CONVERT_TARGET) $(FOGLIB_TARGET) $(TEST_TARGET)

#following lines defined for convert
$(OBJECT_DIR)/main.o:convert/main.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/read_lines.o:convert/read_lines.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_PATH)/convert: $(CONVERT_OBJS)
	$(CXX) -o $@ $(CONVERT_OBJS) $(SYSLIBS)

#following lines defined for testing
$(OBJECT_DIR)/convert.o:convert/convert.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_PATH)/test: $(TEST_OBJS)
	$(CXX) -o $@ $(TEST_OBJS) $(SYSLIBS)

#following lines defined for the library
$(OBJECT_DIR)/program.o:fogsrc/program.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(FOGLIB_PATH)/libfog.a: $(FOGLIB_OBJS)
	ar rcs $@ $(FOGLIB_OBJS)

#following lines defined for applications
PAGERANK_SRC = pagerank.o
PAGERANK_OBJS= $(addprefix $(OBJECT_DIR)/, $(PAGERANK_SRC))
PAGERANK_TARGET= $(BINARY_PATH)/pagerank

$(OBJECT_DIR)/pagerank.o:application/pagerank.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_PATH)/pagerank: $(PAGERANK_OBJS)
	$(CXX) -o $@ $(PAGERANK_OBJS) $(SYSLIBS) -lfog -L./$(FOGLIB_PATH)


.PHONY: convert

convert: $(CONVERT_TARGET)

foglib: $(FOGLIB_TARGET)

test: $(TEST_TARGET)

# applications
pagerank: $(PAGERANK_TARGET)

# utilities
cscope:
	find ./ -name "*.cpp" > cscope.files
	find ./ -name "*.c" >> cscope.files
	find ./ -name "*.h" >> cscope.files
	find ./ -name "*.hpp" >> cscope.files
	cscope -bqk

clean: 
	rm -f cscope.*
	rm -f $(CONVERT_TARGET) $(CONVERT_OBJS)
	rm -f $(TEST_TARGET) $(TEST_OBJS)
	rm -f $(FOGLIB_TARGET) $(FOGLIB_OBJS)

