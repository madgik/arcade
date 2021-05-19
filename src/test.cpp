#include "reader.h"
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
//#include <msgpack.hpp>
#include <iostream>
#include <numeric>      // std::iota
#include <algorithm>
#include <sstream>
#include <unistd.h>
#include <map>
#include <iterator>
#include <sqlite3.h>
#include <sstream>
#include <thread>
#include <fstream>
#include <cstring>
#include <cmath>
#include <sstream>
#include <string>
#include <fstream>
#include "bloom/bloom_filter.hpp"
#include "hps/hps.h"


int main(){
char* filename = new char[100];
int col_num;
char* val = new char[200];
char* retcols = new char[65536*2];

while (1){
    cin >> filename >> col_num >> val >> retcols;
    //vector <vector <string>> cols;
    double duration = 0.0;
    std::clock_t start = std::clock();
    vector <vector <string>> cols;
    auto gen = equi_filter(filename,col_num, val, retcols);
    while (gen)
        cols = gen();
        //print_columns(gen());
    /*equi_filter(filename,col_num, val, retcols);
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration << endl;
    start = std::clock();
    equi_filter(filename,col_num, val, retcols);*/
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration << endl;
    //;
}

return 1;
}