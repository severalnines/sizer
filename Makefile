TARGET = sizer
SRCS = $(TARGET).cpp
OBJS = $(TARGET).o
CXX = g++
CFLAGS = -c -Wall -O2 -fno-rtti -fno-exceptions
CXXFLAGS = 
DEBUG = 
LFLAGS = -Wall
MYSQL_BASEDIR = /usr/
LIB_DIR = -L$(MYSQL_BASEDIR)/lib 
SYS_LIB = 

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) $(LFLAGS) $(LIB_DIR) $(OBJS) -lmysqlclient_r -lndbclient $(SYS_LIB) -o $(TARGET)

$(TARGET).o: $(SRCS)
	$(CXX) $(CFLAGS) -I$(MYSQL_BASEDIR)/include -I$(INCLUDE_DIR) -I$(MYSQL_BASEDIR)/include/storage/ndb  -I$(MYSQL_BASEDIR)/include -I$(MYSQL_BASEDIR)/include/mysql -I$(MYSQL_BASEDIR)/include/mysql/storage/ndb  -I$(MYSQL_BASEDIR)/include/mysql/storage/ndb/ndbapi  $(SRCS)

clean:
	rm -f *.o $(TARGET)

