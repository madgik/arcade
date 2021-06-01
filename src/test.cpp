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


int extractattributes(std::string s, int*& columns) {
  
  string parsed;
  stringstream input_stringstream(s);
  int c = 0;
  while (getline(input_stringstream,parsed,',')){
     if (parsed == "count") break; 
     columns[c] = atoi(&parsed[0]);
     c++;
}  
  return c;
}


int main(){
char* filename = new char[100];
int col_num;
int count_rows;
char* val = new char[200];
char* retcols = new char[65536*2];

int* retcolumns = new int[65536];
char* rids = new char[65536*2];
int* rowids = new int[65536];
int rowidsnum = 0;

char*** cols;
int retcolslen = 0;
bool cont = 1;
auto gen = random_access(filename,cols, retcolumns,retcolslen, rowids, rowidsnum, cont);

while (1){
    cin >> filename >> rids >> retcols;
   
    retcolslen = extractattributes(retcols, retcolumns);
    rowidsnum = extractattributes(rids, rowids);
    //vector <vector <string>> cols;
    double duration = 0.0;
    std::clock_t start = std::clock();

     
    count_rows = 0;
    int rows = 0;
    while (gen){
        rows = gen();
        if (rows == -1) break;
        if (rows == -2) {cout << "The file is not a valid arcade file" << endl; break;}
        count_rows+=rows;
        
        //cout << *(cols[0][0]) << endl;
        //if (cols.size() > 0) count_rows += cols[0].size();
        print_columns(cols, rows, retcolslen);
    }
   
        
        //print_columns(gen());
    /*equi_filter(filename,col_num, val, retcols);
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration << endl;
    start = std::clock();
    equi_filter(filename,col_num, val, retcols);*/
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << endl;
     cout << "Returned " << count_rows << " rows in " << duration << " seconds" << endl;
    //;
}

return 1;
}



int main2(){
char* filename = new char[100];
int col_num;
int count_rows;
char* val = new char[200];
char* retcols = new char[65536*2];

int* retcolumns = new int[65536];


char*** cols;
int retcolslen = 0;
bool cont = 1;
auto gen = equi_filter(filename,cols, col_num, val, retcolumns,retcolslen, cont);

while (1){
    cin >> filename >> col_num >> val >> retcols;
    retcolslen = extractattributes(retcols, retcolumns);
    //vector <vector <string>> cols;
    double duration = 0.0;
    std::clock_t start = std::clock();

    
    count_rows = 0;
    int rows = 0;
    while (gen){
        rows = gen();
        if (rows == -1) break;
        if (rows == -2) {cout << "The file is not a valid arcade file" << endl; break;}
        count_rows+=rows;
        
        //cout << *(cols[0][0]) << endl;
        //if (cols.size() > 0) count_rows += cols[0].size();
        print_columns(cols, rows, retcolslen);
    }
   
        
        //print_columns(gen());
    /*equi_filter(filename,col_num, val, retcols);
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration << endl;
    start = std::clock();
    equi_filter(filename,col_num, val, retcols);*/
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << endl;
     cout << "Returned " << count_rows << " rows in " << duration << " seconds" << endl;
    //;
}

return 1;
}