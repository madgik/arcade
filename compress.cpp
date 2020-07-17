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
#include <cmath>
#include <sstream>
#include <string>
#include <fstream>


  
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


vector <int> sizediff;
vector <string> globaldict;
unordered_map<string, size_t> lookup;
vector <short> diffvals;



vector <string> slice( vector <string>  const &v, int m, int n)
{
    return std::vector<string>(v.begin() + m, v.begin()+n+1);
}


int compress(vector <string> vec, FILE *f1){


    struct D header;
    header.numofvals = vec.size();
    vector <string> minmax(2);
    
    vector <string> vals = vec;
    //printf("%s\n",dataset[0].c_str());
    unordered_set<string> s;
    for (string i : vec)
        s.insert(i);
    
    
    vec.assign( s.begin(), s.end() );
    sort( vec.begin(), vec.end() );
    vector<string> diff;
    minmax[0] = vec[0];
    minmax[1] = vec[vec.size()-1];
    std::vector<string> glob = globaldict;
    std::sort(glob.begin(), glob.end());
    
    set_difference(vec.begin(), vec.end(), glob.begin(), glob.end(), std::inserter(diff, diff.begin()));
    
    
    
    int diffdict = 1;
    int ll = vec.size();
    if (globaldict.size()+diff.size()>200000 or globaldict.size() == 0)
        diffdict = 0;
    else if (globaldict.size() > 0 and diff.size()*1.0/ll > 0.99)
        diffdict = 0;
    else if ((globaldict.size()>=256 and ll < 256) or (globaldict.size() >= 65536 and ll < 65536)  or  (globaldict.size() < 256 and (globaldict.size() + diff.size()) > 255) or ( globaldict.size() < 65536 and (globaldict.size() + diff.size()) > 65535)){
        std::stringstream buffer;
        msgpack::pack(buffer, diff);
        string st = buffer.str();
        int diffdictdump = st.size();
            
        int diffcount = sizediff.size(); 
        int diffavg = 0;
        if (diffcount>0)
            diffavg = accumulate(sizediff.begin(), sizediff.end(), 0)/diffcount;
        else{
            std::stringstream buffer;
            msgpack::pack(buffer, diff);
            string st = buffer.str();
            diffavg = diffdictdump;
        }
           
        int lenvals3 = header.numofvals;
        int locs = 0;
        if (ll < 256) locs = 1;
        else if (ll < 65536) locs = 2;
        else locs = 4;
        int diffs = 0;
        if (globaldict.size()+diff.size() < 256) diffs = 1;
        else if (globaldict.size()+diff.size() < 65536) diffs= 2;
        else diffs = 4;
        
        std::stringstream bufferloc;
        msgpack::pack(bufferloc, vec);
        string stloc = bufferloc.str();
        int sizelocal = stloc.size() + lenvals3*locs;
        int sizeofdiff = diffdictdump + lenvals3*diffs;
        
        if (sizelocal < sizeofdiff)
            diffdict = 0;
        else if (diffcount*(diffs - locs) + diffcount*(diffdictdump-diffavg) - (sizelocal - sizeofdiff) > 0)
            diffdict = 0;
    
    }
    
    
if (diffdict == 1){
   
    globaldict.insert(globaldict.end(), diff.begin(), diff.end());

    
    int value = 0;
    for(size_t index = 0; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;

    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    const char* ssmm = stmm.c_str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, diff);
    string st = buffer.str();
    const char* ss = st.c_str();
    header.dictsize = st.size();
    sizediff.push_back(header.dictsize);
   
    header.lendiff = diff.size();
    
    diffvals.push_back(sizediff.size()-1);
    diffvals.push_back(header.lendiff);
    short* a = &diffvals[0];
    header.previndices = diffvals.size();
    header.diff = 0; // i am in diff dict
    if (globaldict.size()<256){
        char offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        header.bytes = 2;
        header.indicessize = vals.size()*sizeof(char);
        fwrite(&header, sizeof(header), 1, f1);
        fwrite(a, diffvals.size()*sizeof(short), 1, f1);
        fwrite(ssmm, header.minmaxsize ,1 , f1 );
        fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(char),vals.size(),f1);
    }
    else if (globaldict.size()<65536){
        unsigned short offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        header.bytes = 1;
        header.indicessize = vals.size()*sizeof(unsigned short);
         fwrite(&header, sizeof(header), 1, f1);
         fwrite(a, diffvals.size()*sizeof(short), 1, f1);
         fwrite(ssmm, header.minmaxsize ,1 , f1 );
         fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned short),vals.size(),f1);
    }
    if (globaldict.size()>65536){
        unsigned int offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] =  lookup[i];
            k++;
        }
        header.bytes = 0;
        header.indicessize = vals.size()*sizeof(unsigned int);
        fwrite(&header, sizeof(header), 1, f1);
        fwrite(a, diffvals.size()*sizeof(short), 1, f1);
        fwrite(ssmm, header.minmaxsize ,1 , f1 );
        fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
   
}

else if (diffdict == 0){

    globaldict.clear();
    sizediff.clear();
    diffvals.clear();
    globaldict.insert(globaldict.end(), vec.begin(), vec.end());

    
    int value = 0;
    for(size_t index = 0; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;

    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    const char* ssmm = stmm.c_str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, vec);
    string st = buffer.str();
    const char* ss = st.c_str();
    header.dictsize = st.size();
   
    header.lendiff = vec.size();
    
    header.diff = 1; 
    
    diffvals.push_back(0);
    diffvals.push_back(header.lendiff);
    header.previndices = diffvals.size();
    short* a = &diffvals[0];
    
    
    if (globaldict.size()<256){
        char offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        header.bytes = 2;
        header.indicessize = vals.size()*sizeof(char);
        fwrite(&header, sizeof(header), 1, f1);
        fwrite(a, diffvals.size()*sizeof(short), 1, f1);
        fwrite(ssmm, header.minmaxsize ,1 , f1 );
        fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(char),vals.size(),f1);
    }
    else if (globaldict.size()<65536){
        unsigned short offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] = lookup[i];
            k++;
        }
        header.bytes = 1;
        header.indicessize = vals.size()*sizeof(unsigned short);
         fwrite(&header, sizeof(header), 1, f1);
         fwrite(a, diffvals.size()*sizeof(short), 1, f1);
         fwrite(ssmm, header.minmaxsize ,1 , f1 );
         fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned short),vals.size(),f1);
    }
    if (globaldict.size()>65536){
        unsigned int offsets[vals.size()];
        int k = 0;
        for (string i: vals){
            offsets[k] =  lookup[i];
            k++;
        }
        header.bytes = 0;
        header.indicessize = vals.size()*sizeof(unsigned int);
        fwrite(&header, sizeof(header), 1, f1);
        fwrite(a, diffvals.size()*sizeof(short), 1, f1);
        fwrite(ssmm, header.minmaxsize ,1 , f1 );
        fwrite(ss, header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
    

}
    return 1;

}





int main(int argc, char** argv)
{
    /*vector<string> dataset(23677600);
    std::ifstream infile("ips.csv");
    */
    std::ifstream infile("ips.csv");
    
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
    
    fwrite("DIFF",4,1,f1);
    struct fileH fileheader1;
    fileheader1.numofcols = COLNUM;
    fileheader1.numofvals = dataset.size();
    fileheader1.numofblocks = ceil(fileheader1.numofvals*1.0/BLOCKSIZE);
    fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
    long blocksizes[fileheader1.numofblocks+1];
    long ft = ftell(f1);
    fwrite(blocksizes, (fileheader1.numofblocks+1)*sizeof(long), 1, f1);

       int i = 0;
       for (i=0; i < fileheader1.numofvals/BLOCKSIZE; i++){
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
           
           blocksizes[i] = tell_init;
       }
           
       long tell = ftell(f1);
       for (int k = 0; k < COLNUM+1; k++)
           fwrite(&columnindexes[k],sizeof(int),1,f1);
       for (int j = 0; j < COLNUM; j++)
           compress(slice(dataset,i*BLOCKSIZE,fileheader1.numofvals-1),f1);
      
    fseek(f1,ft,SEEK_SET);
    blocksizes[fileheader1.numofblocks] = BLOCKSIZE;
    fwrite(blocksizes, (fileheader1.numofblocks+1)*sizeof(long), 1, f1);
    return (0);
}
