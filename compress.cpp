#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <msgpack.hpp>
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
#include <mutex>          // std::mutex
#include "threadpool.h"

std::mutex mtx;           // mutex for critical section

#define split 8
  
using namespace std;

struct D {
    int dictsize;
    int indicessize;
    int numofvals;
    int bytes;
    int lendiff;
    int diff;
    int minmaxsize;
    int previndices;
};

struct fileH {
     int numofvals;
     int numofcols;
     int numofblocks;
};

struct rec {
     int rowid;
     int val;
};

int BLOCKSIZE = 65535;
int COLNUM = 1;
int COUNT = 23677600;



vector <string> globaldict;
unordered_map<string, size_t> lookup;



/*static int callback3(void* data, int argc, char** argv, char** azColName)
{
    hash<string> ptr_hash;
    int filenum = ptr_hash(argv[0])%split;
    fprintf(f1[filenum],"%s,%s\n",argv[0],argv[1]);
    return 0;
}*/

int compress10(vector <char *> dataset,FILE *f1){

    //printf("%s\n",dataset[0]);
    set<string> s(dataset.begin(), dataset.end());
    
    set<string> set;
    
    
    for (auto const& i: dataset) {
        set.insert(i);
    }
    return 1;

}

vector <string> slice( vector <string>  const &v, int m, int n)
{
    return std::vector<string>(v.begin() + m, v.begin()+n+1);
}


int compress(vector <string> vec, FILE *f1){
    vector <string> vals = vec;
    //printf("%s\n",dataset[0].c_str());
    unordered_set<string> s;
    for (string i : vec)
        s.insert(i);
    
    
    vec.assign( s.begin(), s.end() );
    sort( vec.begin(), vec.end() );
    vector<string> diff;
    
    std::vector<string> glob = globaldict;
    std::sort(glob.begin(), glob.end());
    
    set_difference(vec.begin(), vec.end(), glob.begin(), glob.end(), std::inserter(diff, diff.begin()));
    
    globaldict.insert(globaldict.end(), diff.begin(), diff.end());
     
/*
    
    int value = 0;
    for(size_t index = 0; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;
    
    
    std::stringstream buffer;
    msgpack::pack(buffer, diff);
    string st = buffer.str();
    const char* ss = st.c_str();
    fwrite(ss, strlen(ss) ,1 , f1 );
    
    if (globaldict.size()<256){
        char offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        fwrite(&offsets,sizeof(char),vals.size(),f1);
    }
    else if (globaldict.size()<65536){
        unsigned short offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        fwrite(&offsets,sizeof(unsigned short),vals.size(),f1);
    }
    if (globaldict.size()>65536){
        unsigned int offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] =  lookup[i];
            k++;
        }
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
   
    
    return 1;
*/
}


int compression(vector<vector <string>> *table, char* token1, char* token2, int filenum, int (*len)[split],  FILE  *out){
    //cout << filenum << " " << (*len)[filenum] << endl;
    //(*table)[0][(*len)[filenum]] = (char*)malloc(3000);
    //(*table)[1][(*len)[filenum]] = (char*)malloc(3000);
    
    (*table)[0][(*len)[filenum]] = string(token1);
    (*table)[1][(*len)[filenum]] = string(token2);
    
    
     (*len)[filenum] += 1;
     if ((*len)[filenum] == 65535){
        (*len)[filenum] = 0;
       /* results.emplace_back(
               (*pool).enqueue([table,out] {*/
                //std::this_thread::sleep_for(std::chrono::milliseconds(100));
         
        
         for (int i=0; i<65535; i++){
             
             //mtx.lock();
             //*out << (*table)[0][i] << "," << (*table)[1][i];
            fprintf(out, "%s,%s", (*table)[0][i].c_str(), (*table)[1][i].c_str());
             //t1.join();
             //mtx.unlock();
         }
   /* return 1;
                            
                    })
                  );*/
      
     }
    return 1;
}



#include <sstream>
#include <string>
#include <fstream>


int main(int argc, char** argv)
{
    /*vector<string> dataset(23677600);
    std::ifstream infile("ips.csv");
    */
    std::ifstream infile("ips.csv");
    std::ifstream outfile("ips.diff");
    
    FILE *f1;
    int columnindexes[COLNUM+1];
    memset( columnindexes, 0, COLNUM+1*sizeof(int) );
    f1 = fopen("ips.diff","wb");

   /* int res = 0;
   std::string line;
   while (std::getline(infile, line))
   {
       dataset[res++] = line;
   }
    */
    
    vector<string> dataset;

   /* if (FILE *fp = fopen("ips.csv", "r")) {
           string buf;
           while (size_t len = fread(&buf, 1, sizeof(buf), fp))
               dataset.insert(dataset.end(), buf, buf + len);
           fclose(fp);
    }
    */
    string x;
    while (infile >> x) {
        dataset.push_back(x);
    }
    
    cout << dataset[0] << endl;

       int i = 0;
       for (i=0; i < COUNT/BLOCKSIZE; i++){
           long tell_init = ftell(f1);
           for (int k = 0; k < COLNUM+1; k++)
             fwrite(&columnindexes[k],sizeof(int),1,f1);
           long tell = ftell(f1);
           
           
           for (int j = 0; j < COLNUM; j++){
               long tell1 = ftell(f1);
               compress(slice(dataset,i*BLOCKSIZE,i*BLOCKSIZE+BLOCKSIZE-1),f1);
               columnindexes[j] = tell1-tell;
           }
           long tell_end = ftell(f1);
           columnindexes[COLNUM] = tell_end - tell;
           fseek(f1,tell_init,SEEK_SET);
           for (int k = 0; k < COLNUM+1; k++)
             fwrite(&columnindexes[k],sizeof(int),1,f1);
           fseek(f1,tell_end,SEEK_SET);
       }
           
       long tell = ftell(f1);
       for (int k = 0; k < COLNUM+1; k++)
           fwrite(&columnindexes[k],sizeof(int),1,f1);
       for (int j = 0; j < COLNUM; j++)
           compress(slice(dataset,i*BLOCKSIZE,COUNT-1),f1);
      
    
    return (0);
}
