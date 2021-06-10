#include <iostream>
#include <sstream>
#include <unistd.h>
#include <iterator>
#include <sys/time.h>
#include <math.h>
#include <cassert>
#include <fstream>
#include <memory>
#include "snappy.h"
#include <getopt.h>
#include <string>
#include <memory>
#include "gtest/gtest.h"
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <cmath>
#include <coroutine>
#include "generator.h"
#include "process.h"
using namespace std;


namespace Arcade{
int print_columns(char*** &, int, int);

class ArcadeReader{
  Caches mcaches;
  size_t result;
  Processing processing;
  
  public: 
    Generator <int> equi_filter(char*, char*** &, int , char* , int* , int );
    Generator <int> random_access(char*, char*** &, int* , int ,  int* , int );
    Generator <int> scan(char*, char*** &, int*, int);
  
};

class ArcadeWriter{
    int BLOCKSIZE = 65535;
    bool SNAPPY;
    public:
    int compress(char*, char*, int, int, int*, int);
};

}