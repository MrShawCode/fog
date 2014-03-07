# paths
OBJECT_DIR = obj
BINARY_DIR = bin
HEADERS_PATH = headers

#compile/link options
SYSLIBS = -L/usr/local/lib -lboost_system -lboost_program_options -lboost_thread -lz -lrt -lboost_thread-mt
CXX?= g++
CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -I./$(HEADERS_PATH)
CXXFLAGS+= -Wfatal-errors

# make selections
CONVERT_SRC = convert.o process_edgelist.o process_adjlist.o
CONVERT_OBJS= $(addprefix $(OBJECT_DIR)/, $(CONVERT_SRC))
CONVERT_TARGET=$(BINARY_DIR)/convert

TEST_SRC = test.o
TEST_OBJS= $(addprefix $(OBJECT_DIR)/, $(TEST_SRC))
TEST_TARGET=$(BINARY_DIR)/test

FOG_SRC = main.o
FOG_HEADERS = types.hpp config.hpp disk_thread.hpp cpu_thread.hpp index_vert_array.hpp fog_engine.hpp options_utils.h config_parse.h
FOG_REL_HEADERS = $(addprefix $(HEADERS_PATH)/, $(FOG_HEADERS))
FOG_OBJS= $(addprefix $(OBJECT_DIR)/, $(FOG_SRC))
FOG_TARGET = $(BINARY_DIR)/fog

all: $(FOG_TARGET)
#all: $(CONVERT_TARGET) $(FOG_TARGET) $(TEST_TARGET)

#dependencies
$(OBJECT_DIR):
	mkdir ./obj

$(BINARY_DIR):
	mkdir ./bin

#following lines defined for convert
$(OBJECT_DIR)/convert.o:convert/convert.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/process_edgelist.o:convert/process_edgelist.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/process_adjlist.o:convert/process_adjlist.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_DIR)/convert: $(CONVERT_OBJS)
	$(CXX) -o $@ $(CONVERT_OBJS) $(SYSLIBS)

$(CONVERT_OBJS): |$(OBJECT_DIR)
$(CONVERT_TARGET): |$(BINARY_DIR)

#following lines defined for testing
$(OBJECT_DIR)/test.o:convert/test.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_DIR)/test: $(TEST_OBJS)
	$(CXX) -o $@ $(TEST_OBJS) $(SYSLIBS)

$(TEST_OBJS): |$(OBJECT_DIR)
$(TEST_TARGET): |$(BINARY_DIR)

#following lines defined for fog
$(OBJECT_DIR)/main.o:fogsrc/main.cpp $(FOG_REL_HEADERS)
	$(CXX) $(CXXFLAGS) -c -o $@ fogsrc/main.cpp

$(BINARY_DIR)/fog: $(FOG_OBJS)
	$(CXX) -o $@ $(FOG_OBJS) $(SYSLIBS)

$(FOG_OBJS): |$(OBJECT_DIR)
$(FOG_TARGET): |$(BINARY_DIR)

.PHONY:

convert: $(CONVERT_TARGET)

test: $(TEST_TARGET)

fog: $(FOG_TARGET)

# utilities
cscope:
	find ./ -name "*.cpp" > cscope.files
	find ./ -name "*.c" >> cscope.files
	find ./ -name "*.h" >> cscope.files
	find ./ -name "*.hpp" >> cscope.files
	cscope -bqk

clean: 
	rm -f $(CONVERT_TARGET) $(CONVERT_OBJS)
	rm -f $(TEST_TARGET) $(TEST_OBJS)
	rm -f $(FOG_TARGET) $(FOG_OBJS)
	rm -f $(BINARY_DIR)/*
	rm -f cscope.*

