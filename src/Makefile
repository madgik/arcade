#g++-10  -std=c++20  "-fcoroutines" -o test2  test.cpp reader.cpp cache.cpp process.cpp writer.cpp -lsnappy -O3 -freorder-blocks-algorithm=simple
#CDC = /usr/bin/g++-10 # Flag for implicit rules
CPP = g++

CPPFLAGS = -O3 -std=c++20  -Isnappy/third_party/googletest/googletest/include/ -fcoroutines -freorder-blocks-algorithm=simple -fPIC # Flag for implicit rules. Turn on debug info

# Implicit rule #1: blah is built via the C linker implicit rule
# Implicit rule #2: blah.o is built via the C compilation implicit rule, because blah.c exists

#DEPS = hellomake.h

%.o: %.c
	$(CPP) -c -o $@ $< $(CPPFLAGS) snappy/build/libsnappy.a snappy/build/lib/libgtest.a

libarcade.a:  reader.o cache.o process.o writer.o
	ar rcs libarcade.a reader.o cache.o process.o writer.o snappy/build/libsnappy.a snappy/build/lib/libgtest.a

depend:
	git clone https://github.com/google/snappy.git
	cd snappy; git submodule update --init; mkdir build; cd build && cmake ../ && make
	
runner:  ./libarcade.a runner.cpp
	$(CPP) -O3 -std=c++20 -fcoroutines -freorder-blocks-algorithm=simple -o runner runner.cpp -L. -larcade snappy/build/libsnappy.a snappy/build/lib/libgtest.a -Isnappy/third_party/googletest/googletest/include/

clean:  reader.o cache.o process.o writer.o libarcade.a
	rm -f *.o
