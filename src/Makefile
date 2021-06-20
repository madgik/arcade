CPPFLAGS = -O3 -std=c++20  -Isnappy -Isnappy/build -Isnappy/third_party/googletest/googletest/include/ -fcoroutines -freorder-blocks-algorithm=simple -fPIC # Flag for implicit rules. Turn on debug info


%.o: %.cpp
	$(CXX) -c -o $@ $< $(CPPFLAGS) snappy/build/libsnappy.a snappy/build/lib/libgtest.a

libarcade.a: reader.o cache.o process.o writer.o
	ar rcs libarcade.a reader.o cache.o process.o writer.o snappy/build/libsnappy.a snappy/build/lib/libgtest.a

depend:
	git clone https://github.com/google/snappy.git
	cd snappy; git submodule update --init; mkdir build; cd build && cmake ../ && make
	
runner:  ./libarcade.a runner.cpp
	$(CXX) -O3 -std=c++20 -fcoroutines -freorder-blocks-algorithm=simple -o runner runner.cpp -L. -larcade snappy/build/libsnappy.a snappy/build/lib/libgtest.a -Isnappy -Isnappy/build -Isnappy/third_party/googletest/googletest/include/

clean:  reader.o cache.o process.o writer.o libarcade.a
	rm -f *.o
