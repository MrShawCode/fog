# path definition
OBJECT_DIR = obj
FOGLIB_DIR = foglib
BINARY_DIR = bin
HEADERS_PATH = headers

#compile options
SYSLIBS = -L/usr/lib -lboost_system -lboost_program_options -lboost_thread -lz -lrt -lboost_thread-mt
CXX?= g++
CXXFLAGS?= -O3 -DNDEBUG -Wall -Wno-unused-function -I./$(HEADERS_PATH)
CXXFLAGS+= -Wfatal-errors

# make selections
CONVERT_SRC = main.o read_lines.o
CONVERT_OBJS= $(addprefix $(OBJECT_DIR)/, $(CONVERT_SRC))
CONVERT_TARGET=$(BINARY_DIR)/convert

TEST_SRC = test.o
TEST_OBJS= $(addprefix $(OBJECT_DIR)/, $(TEST_SRC))
TEST_TARGET=$(BINARY_DIR)/test

FOG_SRC = program.o fogengine.o thread.o sssp.o
FOG_OBJS= $(addprefix $(OBJECT_DIR)/, $(FOG_SRC))
FOG_TARGET = $(BINARY_DIR)/sssp

all: $(FOG_TARGET)
#all: $(CONVERT_TARGET) $(FOG_TARGET) $(TEST_TARGET)

#following lines defined for convert
$(OBJECT_DIR)/main.o:convert/main.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/read_lines.o:convert/read_lines.cpp 
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

#following lines defined for final program
$(OBJECT_DIR)/program.o:fogsrc/program.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/fogengine.o:fogsrc/fogengine.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/thread.o:fogsrc/thread.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(OBJECT_DIR)/sssp.o:application/sssp.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_DIR)/sssp: $(FOG_OBJS)
	$(CXX) -o $@ $(FOG_OBJS) $(SYSLIBS)

.PHONY:

convert: $(CONVERT_TARGET)

test: $(TEST_TARGET)

sssp: $(FOG_TARGET)

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

