# Makefile, Ben Gruher and Priyanka Patil, Seattle University, CPSC5300, Spring 2021
 
CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

OBJS = sql5300.o heap_storage.o

sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

sql5300.o : heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h

%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

clean:
	rm -f sql5300 *.o
