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

$(OBJECT_DIR)/main.o:convert/main.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<
#	$(CXX) -MM -MT '$(OBJECT_DIR)/main.o' $< > $(@:.o=.d)

$(OBJECT_DIR)/read_lines.o:convert/read_lines.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<
#	$(CXX) -MM -MT '$(OBJECT_DIR)/read_lines.o' $< > $(@:.o=.d)

$(BINARY_PATH)/convert: $(CONVERT_OBJS)
	$(CXX) -o $@ $(CONVERT_OBJS) $(SYSLIBS)

$(OBJECT_DIR)/convert.o:convert/convert.cpp 
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(BINARY_PATH)/test: $(TEST_OBJS)
	$(CXX) -o $@ $(TEST_OBJS) $(SYSLIBS)


convert: $(CONVERT_TARGET)

test: $(TEST_TARGET)

cscope:
	find ./ -name "*.cpp" > cscope.files
	find ./ -name "*.c" >> cscope.files
	find ./ -name "*.h" >> cscope.files
	find ./ -name "*.hpp" >> cscope.files
	cscope -bqk

all: $(CONVERT_TARGET) $(TEST_TARGET)

# foglib

clean: 
	echo $(CONVERT_OBJS)
	rm -f cscope.*
	rm -f $(CONVERT_TARGET) $(OBJECT_DIR)/*

