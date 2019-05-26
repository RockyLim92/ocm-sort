CXX             = g++ 
SRCS            = $(wildcard *.cpp) $(wildcard *.cc)
OBJS            = $(SRCS:.cpp=.o)
TARGET          = ocmsort
LIBS			= -lpthread

all : $(TARGET)
	$(CXX) -o $(TARGET) $(OBJS) $(INC) $(LIB_DIRS) $(LIBS)

$(TARGET) :
	$(CXX) -c $(SRCS) $(INC) $(LIB_DIRS) $(LIBS)

clean :
	rm -f $(TARGET)
	rm -f *.o


