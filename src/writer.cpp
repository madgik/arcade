#include <numeric>     
#include <fstream>
#include "bloom/bloom_filter.hpp"
#include "hps/hps.h"
#include "arcade.h"

using namespace std;
using namespace Arcade;

vector<std::string> extractColumn(vector<std::string> dataset, uint64_t colIndex) {
  char gDelimiter = ',';
  vector<std::string> column(dataset.size());
  for (int i=0; i < dataset.size(); i++){
      	uint64_t col = 0;
  		size_t start = 0;
  		size_t end = dataset[i].find(gDelimiter);
  		while (col < colIndex && end != std::string::npos) {
    		start = end + 1;
    		end = dataset[i].find(gDelimiter, start);
    		if (end == std::string::npos)
    		    end = dataset[i].size()-1;
    		++col;
  		}
  		column[i] = (col == colIndex ? dataset[i].substr(start, end - start) : "");
  		
  }
  
  return column;
}


template<class InputIt1, class OutputIt>
OutputIt calc_diff(InputIt1 first1, InputIt1 last1,
                        unordered_map <string, bool> &glob,
                        OutputIt d_first)
{
    while (first1 != last1){ 
        if (glob.find(*first1) == glob.end()){
            *d_first++ = *first1++;
        }
        else
            *first1++;
    }
    return d_first;
}


/*TODO refactor optimise this function, this can be much faster */
int compress_batch(vector <string> vals, FILE *f1, bloom_filter *filter, bool &isdictionary, vector <int> &sizediff, vector <string> &globaldict, unordered_map <string, bool> &glob, unordered_map<string, size_t> &lookup, vector <short> &diffvals, int blocknum, int BLOCKSIZE, bool SNAPPY, unsigned long &global_dict_memory, int &permanent_decision, double &duration5){
    int CACHE_SIZE = 8192000*2;
    struct D header;
    header.numofvals = vals.size();
    vector <string> minmax(4);
    vector <string> vec = vals;
    std::sort(vec.begin(),vec.end());
    vec.erase(std::unique(vec.begin(),vec.end()),vec.end() );
    int distinct_count = vec.size();
    minmax[0] = vec[0];
    minmax[1] = vec[distinct_count-1];
    
    if (vec.size()*1.0/vals.size()>0.99){
            isdictionary = false;
            header.dictsize = 0;
            header.previndices = 0;
            globaldict.clear();
    		glob.clear();
    		sizediff.clear();
    		diffvals.clear();
    		lookup.clear();
            string stmm = hps::to_string(minmax);
    		header.minmaxsize = 0;//stmm.size();
            string st = hps::to_string(vals);
            header.indicessize = st.size();
    		header.lendiff = 0;
        	header.bytes = 0; //TODO perhaps edit this and read to support minmax to parquet pages
        	fwrite(&header, sizeof(header), 1, f1);
        	//fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        	fwrite(&st[0], header.indicessize ,1 , f1 );
    
            return 1;		
    }
    
int diffdict = 1;
int globdsize = globaldict.size();
vector<string> diff;

if (permanent_decision == 1){
    
    calc_diff(vec.begin(), vec.end(), glob, std::inserter(diff, diff.begin()));
    if ((diff.size()*1.0)/distinct_count>0.9 and globdsize>0){
        permanent_decision = 1;
        }
}

if (diff.size() > 0){
minmax[2] = diff[0];
minmax[3] = diff[diff.size()-1];
}
else{
minmax[2] = " ";
minmax[3] = " ";
}

string st = "";
string stloc = "";


std::clock_t start;
start = std::clock();
    
if (permanent_decision == 1){
    if (global_dict_memory > CACHE_SIZE or globdsize == 0){
        diffdict = 0;
    }
    else if (globdsize > 0 and diff.size()*1.0/distinct_count > 0.99)  diffdict = 0;
    
    else if ((globdsize>=256 and distinct_count < 256) or (globdsize >= 65536 and distinct_count < 65536)  or  (globdsize < 256 and (globdsize + diff.size()) > 255) or ( globaldict.size() < 65536 and (globdsize + diff.size()) > 65535)){
        st = hps::to_string(diff);
        int diffdictdump = st.size();
        int diffcount = sizediff.size(); 
        int diffavg = 0;
        if (diffcount>0)
            diffavg = accumulate(sizediff.begin(), sizediff.end(), 0)/diffcount;
        else
            diffavg = diffdictdump;
        
        int lenvals3 = header.numofvals;
        int locs = 0;
        if (distinct_count < 256) locs = 1;
        else if (distinct_count < 65536) locs = 2;
        else locs = 4;
        int diffs = 0;
        if (globdsize + diff.size() < 256) diffs = 1;
        else if (globdsize+diff.size() < 65536) diffs= 2;
        else diffs = 4;
        
        stloc = hps::to_string(vec);
        int sizelocal = stloc.size() + lenvals3*locs;
        int sizeofdiff = diffdictdump + lenvals3*diffs;
        
        /*if (globdsize>0){
        if (sizelocal < sizeofdiff){
            diffdict = 0;
            cout  << "cost function1" << endl;}
        else if (diffcount*(diffs - locs) + diffcount*(diffdictdump-diffavg) - (sizelocal - sizeofdiff) > 0)
            diffdict = 0;
            cout  << "cost function2" << endl;
        }
        else {*/
        int pblocks = ((CACHE_SIZE-global_dict_memory)/(diffdictdump));
        if (pblocks*(diffs*BLOCKSIZE) + sizeofdiff - pblocks*(locs*BLOCKSIZE + diffavg) - sizelocal > 0){
        //if (pblocks*(diffs*BLOCKSIZE) + sizeofdiff - (pblocks/diffcount)*((diffcount+pblocks%diffcount)*(locs*BLOCKSIZE + diffavg) + sizelocal) > 0){
           // cout  << "cost function" << endl;
            diffdict = 0;
            //}
        }
    }
}
else diffdict = 0; //to demostrate the ram cpu compression trade-offs. 
diffdict = 1;
duration5 += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;

if (diffdict == 1){
    
    globaldict.insert(globaldict.end(), diff.begin(), diff.end());
    for (string i : diff){
        glob[i] = 0;
    }
    int value = 0;
    for(size_t index = globdsize; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;
    string stmm = hps::to_string(minmax);
    header.minmaxsize = stmm.size();   
    if (st==""){
        st = hps::to_string(diff);
        }
    snappy::string comprst;  
    if (SNAPPY){
            snappy::Compress(&st[0], st.size(), &comprst);
            header.dictsize = comprst.size();
        }
    else
        header.dictsize = st.size();
    global_dict_memory +=  st.size();
    sizediff.push_back(st.size());
   
    header.lendiff = diff.size();
    
    diffvals.push_back(blocknum);
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        if (SNAPPY){
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
            }
        else
            fwrite(&st[0], header.dictsize ,1 , f1 );
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
         fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
          if (SNAPPY)
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
        else
            fwrite(&st[0], header.dictsize ,1 , f1 );
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
         if (SNAPPY)
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
        else
            fwrite(&st[0], header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
}

else if (diffdict == 0){
    if (permanent_decision == 1){
        sizediff.clear();
        diffvals.clear();
        global_dict_memory = 0;
        globaldict = vec;
        glob.clear();
        for (string i : vec){
        glob[i] = 0;
    }
    }
    
    if (permanent_decision == 0){
    	bloom_parameters parameters;
    	parameters.projected_element_count = 55000;
    	parameters.false_positive_probability = 0.1;
    	parameters.random_seed = 0xA5A5A5A5;
    	parameters.compute_optimal_parameters();
    	bloom_filter filternew(parameters);
    	int count = 0;
    	for (string val: vec){
        	filternew.insert(val);
        	if ((*filter).contains(val))
            	count++;
    	}
    	if ((count*1.0)/vec.size()>0.2)
        	permanent_decision = 1;
    	*filter = filternew;
    
    }
    
    int value = 0;
    for(size_t index = 0; index < vec.size(); ++index)
        lookup[vec[index]] = index;

    string stmm = hps::to_string(minmax);
    header.minmaxsize = stmm.size();

    
    if (stloc==""){
        stloc = hps::to_string(vec);
      }
    
    snappy::string comprst;
    if (SNAPPY){
            snappy::Compress(&stloc[0], stloc.size(), &comprst);
            header.dictsize = comprst.size();
        }
    else
        header.dictsize = stloc.size();
    global_dict_memory +=  stloc.size();
    header.lendiff = distinct_count;
    
    header.diff = 1; 
    
    diffvals.push_back(blocknum);
    diffvals.push_back(header.lendiff);
    header.previndices = diffvals.size();
    short* a = &diffvals[0];
    
    
    if (vec.size()<256){
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        if (SNAPPY)
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
        else
            fwrite(&stloc[0], header.dictsize ,1 , f1 );
            
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
         fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
         if (SNAPPY)
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
        else
            fwrite(&stloc[0], header.dictsize ,1 , f1 );
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        if (SNAPPY)
            fwrite(&comprst[0], header.dictsize ,1 , f1 );
        else
            fwrite(&stloc[0], header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
    

}
    return 1;

}
	


int ArcadeWriter::compress(char* infile, char* outfile, int startp, int numofvals, int* retcols, int colnum){
    int permanent_decision = 1;
    double duration5 = 0.0;
    unsigned long global_dict_memory = 0;
    vector<int> mcolumns(retcols, retcols + colnum);
	int COLNUM = mcolumns.size();
	vector<vector <int>> sizediff(COLNUM);
	vector<vector <string>> globaldict(COLNUM);
	//vector<vector <string>> glob(COLNUM);
	vector<unordered_map <string, bool>> glob(COLNUM);
	vector<unordered_map<string, size_t>> lookup(COLNUM);
	vector<vector <short>> diffvals(COLNUM);
	bool isdictionary = true;
    bloom_parameters parameters;
    parameters.projected_element_count = 55000;
    parameters.false_positive_probability = 0.1;
    parameters.random_seed = 0xA5A5A5A5;
    parameters.compute_optimal_parameters();
    
   //Instantiate Bloom Filter
    bloom_filter filter(parameters);
    
    int blocknum = 0;
    std::string input;
    input = infile;
    std::clock_t start;
    double duration;
    if (strstr(outfile, "snappy"))
			SNAPPY = 1;
		else
			SNAPPY = 0;
			
    std::ifstream finput(input.c_str());
    
    FILE *f1;
    int columnindexes[COLNUM+1];
    memset( columnindexes, 0, COLNUM+1*sizeof(int) );
    f1 = fopen(outfile,"wb");

  /*init output file headers*/
  fwrite("DIFF",4,1,f1);
  long ft = ftell(f1);
  struct fileH fileheader1;
  fileheader1.numofcols = COLNUM;
  fileheader1.numofvals = numofvals;
  fileheader1.numofblocks = ceil(fileheader1.numofvals*1.0/BLOCKSIZE);
  //cout << fileheader1.numofblocks << endl;
  
  fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
  long blocksizes[fileheader1.numofblocks+1];
  
  fwrite(blocksizes, (fileheader1.numofblocks+1)*sizeof(long), 1, f1);
  
  bool eof = false;
  string line;
  vector<string> dataset; // buffer that holds a batch of rows in raw text
  
  int num_of_vals1 = 0;
  
  for (int i = 0; i<startp; i++){
        std::getline(finput, line);
  }
  
  while (!eof) {
    int numValues = 0;      // num of lines read in a batch

    dataset.clear();
    // read a batch of lines from the input file
    start = std::clock();
    
    for (int i = 0; i < BLOCKSIZE; ++i) {
      if (!std::getline(finput, line) or num_of_vals1 >= fileheader1.numofvals) {
        eof = true;
        break;
      }
      dataset.push_back(line.substr(0, line.size()));  
      ++numValues;
      num_of_vals1++;      
    }
    duration += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    /*compress batch*/
    long tell_init = ftell(f1);
    for (int k = 0; k < COLNUM+1; k++)
        fwrite(&columnindexes[k],sizeof(int),1,f1);
    long tell = ftell(f1);
           
    for (int j = 0; j < COLNUM; j++){
               long tell1 = ftell(f1);
               //compress(dataset, f1, sizediff[j], globaldict[j], glob[j], lookup[j], diffvals[j]);
               compress_batch(extractColumn(dataset,mcolumns[j]), f1, &filter, isdictionary, sizediff[j], globaldict[j], glob[j], lookup[j], diffvals[j], blocknum, BLOCKSIZE, SNAPPY, global_dict_memory, permanent_decision,duration5); 
               //compress(slice(dataset,0,dataset.size()-1),f1);
               columnindexes[j] = tell1-tell;
    }
    long tell_end = ftell(f1);
    columnindexes[COLNUM] = tell_end - tell;
    fseek(f1,tell_init,SEEK_SET);
    for (int k = 0; k < COLNUM+1; k++)
        fwrite(&columnindexes[k],sizeof(int),1,f1);
    
    
    fseek(f1,tell_end,SEEK_SET);       
    blocksizes[blocknum] = tell_init;    
    blocknum++;
}
         
    
    //std::cout<<"csv import time: "<< duration <<'\n';
    fseek(f1,ft,SEEK_SET);
    fileheader1.numofvals = num_of_vals1;
    fileheader1.numofblocks = ceil(fileheader1.numofvals*1.0/BLOCKSIZE);
    fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
    blocksizes[fileheader1.numofblocks] = BLOCKSIZE;
    fwrite(blocksizes, (fileheader1.numofblocks+1)*sizeof(long), 1, f1);
    fflush(f1);
    return 0;
}



//echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf > /dev/null
