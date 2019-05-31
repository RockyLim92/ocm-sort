CXX             = g++ -g
SRCS            = $(wildcard *.cpp) $(wildcard *.cc)
OBJS            = $(SRCS:.cpp=.o)
TARGET          = ocmsort
LIBS			= -lpthread -ltbb

all : $(TARGET)
	$(CXX) -std=c++11 -o $(TARGET) $(OBJS) $(INC) $(LIB_DIRS) $(LIBS)

$(TARGET) :
	$(CXX) -std=c++11 -c $(SRCS) $(INC) $(LIB_DIRS) $(LIBS)

clean :
	rm -f $(TARGET)
	rm -f *.o


