CXX             = g++ -std=c++11
SRCS            = $(wildcard *.cpp) $(wildcard *.cc)
OBJS            = $(SRCS:.cpp=.o)
TARGET          = ocmsort2
LIBS			= -lpthread -ltbb

all : $(TARGET)
	$(CXX) -o $(TARGET) $(OBJS) $(INC) $(LIB_DIRS) $(LIBS)

$(TARGET) :
	$(CXX) -c $(SRCS) $(INC) $(LIB_DIRS) $(LIBS)

clean :
	rm -f $(TARGET)
	rm -f *.o
	


