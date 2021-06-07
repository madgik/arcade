#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <msgpack.hpp>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <map>
#include <iterator>
#include <sys/time.h>
#include <math.h>
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
#include "snappy.h"
#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>
#include <gtest/gtest.h>
#include "hps/hps.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <cmath>
#include <iostream>
#include <vector>
#include "hps/hps.h"
#include <coroutine>

#include "generator.h"
using namespace std;
#include "process.h"


int print_columns(string*** &, int, int);


class ArcadeReader{
  Caches mcaches;
  size_t result;
  Processing processing;
  
  public: 
    Generator <int> equi_filter(char*, char*** &, int &, char* &, int* &, int &, bool &);
    Generator <int> random_access(char*, char*** &, int* &, int &,  int* &, int &, bool &);
    Generator <int> scan(char*, char*** &, int* &, int &, bool &);
  
};