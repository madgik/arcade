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
#include "bloom/bloom_filter.hpp"

using namespace std;
int BLOCKSIZE = 65535;
int CACHE_SIZE = 8192000*2;
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

unsigned long global_dict_memory = 0;

int permanent_decision = 1;

double duration5 = 0.0;

static char gDelimiter = ',';
// extract one column raw text from one line

vector <int> extractattributes(std::string s) {
  vector <int> columns;
  string parsed;
  stringstream input_stringstream(s);
  
  while (getline(input_stringstream,parsed,',')){
     cout << parsed << endl;
     columns.push_back(stoi(parsed));
}  
  return columns;
}

vector<std::string> extractColumn(vector<std::string> dataset, uint64_t colIndex) {
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

template<class InputIt1, class InputIt2, class OutputIt, class OutputIterator>
OutputIt merge5(InputIt1 first1, InputIt1 last1,
                        InputIt2 first2, InputIt2 last2,
                        OutputIt d_first, OutputIterator glob)
{
    while (first1 != last1) {
        if (first2 == last2){
         std::copy(first2,last2,glob);
         return std::copy(first1, last1, d_first);
         
        }   
        if (*first1 < *first2) {
            *glob++ = *first1;
            *d_first++ = *first1++;
            
        }
        else{
            if (! (*first2 < *first1)) {
                ++first1;
            }
            *glob++ = *first2++;
        }
    }
    return d_first;
}

template<class InputIt1, class InputIt2, class OutputIt>
OutputIt merge7(InputIt1 first1, InputIt1 last1,
                        InputIt2 first2, InputIt2 last2,
                        OutputIt d_first)
{
    while (first1 != last1) {
        if (first2 == last2){
         return std::copy(first1, last1, d_first);
         
        }   
        if (*first1 < *first2) {
            *d_first++ = *first1++;
            
        }
        else{
            if (! (*first2 < *first1)) {
                ++first1;
            }
            *first2++;
        }
    }
    return d_first;
}



template <class InputIterator1, class InputIterator2, class OutputIterator>
  OutputIterator merge6 (InputIterator1 first1, InputIterator1 last1,
                        InputIterator2 first2, InputIterator2 last2,
                        OutputIterator result)
{
  while (true) {
    if (first1==last1) return std::copy(first2,last2,result);
    if (first2==last2) return std::copy(first1,last1,result);
    *result++ = (*first2<*first1)? *first2++ : *first1++;
  }
}



vector<std::string> merge3(vector <string> &glob, vector <string> &vec){
    vector <string> diff;
    //vector <string> glob2(glob.size()+vec.size());
    //merge5(vec.begin(), vec.end(), glob.begin(), glob.end(), std::inserter(diff, diff.begin()), glob2.begin());
    std::clock_t start;
    start = std::clock();
    merge7(vec.begin(), vec.end(), glob.begin(), glob.end(), std::inserter(diff, diff.begin()));
    //glob.assign(glob2.begin(), glob2.begin()+diff.size()+glob.size());
    
    vector <string> temp(glob.size()+diff.size());
    merge6(glob.begin(), glob.end(), diff.begin(), diff.end(), temp.begin());
    glob.assign(temp.begin(), temp.end());
    /*cout << "lala" << endl;
    cout << diff.size() << endl;
    cout << glob.size() << endl;*/
    duration5 += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration5 << endl;
    
    return diff;
}



vector<std::string> merge2(vector <string> &glob, vector <string> &vec){
    
    
    int alen = glob.size();
    int blen = vec.size();
    vector <string> temp(alen+blen);
    cout << alen << " " << blen << endl;
    vector <string> diff;
    int i = 0, j = 0, k = 0, diffl = 0, templ = 0;

 while (i < alen and j < blen)
    {
        //cout << i << " " << glob.size() << endl;
        if (glob[i].compare(vec[j]) < 0){
            temp[templ] = glob[i];
            i++;
            templ++;
            }
        
        else if (glob[i].compare(vec[j]) == 0){
            temp[templ] = glob[i];
            i++;
            j++;
            templ++;
            }
        else if (glob[i].compare(vec[j]) > 0){
            temp[templ] = vec[j];
            diff.push_back(vec[j]);
            j++;
            templ++;
            }
        
        
    }

    cout << "diff:" << " "<< diff.size() << " "<< "temp: "<< temp.size() << endl;
    //if (temp.size()>0)
    //cout << temp[temp.size()-1] << endl;
    glob.assign( temp.begin(), temp.begin()+templ);
    
    return diff;
}

vector <string> slice( vector <string>  const &v, int m, int n)
{
    return std::vector<string>(v.begin() + m, v.begin()+n+1);
}


template<class InputIt1, class OutputIt>
OutputIt calc_diff(InputIt1 first1, InputIt1 last1,
                        unordered_map <string, bool> &glob,
                        OutputIt d_first)
{
    while (first1 != last1){ 
        if (glob.find(*first1) == glob.end())
            *d_first++ = *first1++;
        else
            *first1++;
    }
    return d_first;
}



int compress_batch(vector <string> vals, FILE *f1, bloom_filter *filter, bool &isdictionary, vector <int> &sizediff, vector <string> &globaldict, unordered_map <string, bool> &glob, unordered_map<string, size_t> &lookup, vector <short> &diffvals, int blocknum){
    struct D header;
    header.numofvals = vals.size();
    vector <string> minmax(4);
    //set<string> s(vals.begin(), vals.end());
    
     vector <string> vec = vals;
    
 //   set<string> s;
  //  for (string i : vals)
   //     s.insert(i);
    std::sort(vec.begin(),vec.end());
    vec.erase(std::unique(vec.begin(),vec.end()),vec.end() );
    
    //sort( vals.begin(), vals.end() );
    //std::vector<int>::iterator vec = unique( vals.begin(), vals.end() );;
   
    //vec.assign(s.begin(), s.end());
    //sort( vec.begin(), vec.end() );
    
    int distinct_count = vec.size();
    minmax[0] = vec[0];
    minmax[1] = vec[distinct_count-1];
    //std::vector<string> glob = globaldict;
    //std::sort(glob.begin(), glob.end());
    
    if (vec.size()*1.0/vals.size()>0.45){
            isdictionary = false;
            header.dictsize = 0;
            header.previndices = 0;
            globaldict.clear();
    		glob.clear();
    		sizediff.clear();
    		diffvals.clear();
    		lookup.clear();

    
    		std::stringstream buffermm;
    		msgpack::pack(buffermm, minmax);
    		string stmm = buffermm.str();
    		header.minmaxsize = 0;//stmm.size();

    
    		std::stringstream buffer;
    		msgpack::pack(buffer, vals);
    		string st = buffer.str();
            header.indicessize = st.size();
    		header.lendiff = 0;

        	header.bytes = 0;
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
        cout << "small delta" << endl;
        permanent_decision = 1;
        }
}

if (diff.size() > 0){
minmax[2] = diff[0];
minmax[3] = diff[diff.size()-1];
}
else{
minmax[2] = "";
minmax[3] = "";
}


if (permanent_decision == 1){
    if (global_dict_memory > CACHE_SIZE or globdsize == 0){
        cout << global_dict_memory << " " << CACHE_SIZE << " " << globdsize << endl;
        diffdict = 0;
        cout << "cache" << endl;
        }
    else if (globdsize > 0 and diff.size()*1.0/distinct_count > 0.99){
    diffdict = 0;
    cout << "small diff" << endl;
    }
        
    else if ((globdsize>=256 and distinct_count < 256) or (globdsize >= 65536 and distinct_count < 65536)  or  (globdsize < 256 and (globdsize + diff.size()) > 255) or ( globaldict.size() < 65536 and (globdsize + diff.size()) > 65535)){
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
        if (distinct_count < 256) locs = 1;
        else if (distinct_count < 65536) locs = 2;
        else locs = 4;
        int diffs = 0;
        if (globdsize + diff.size() < 256) diffs = 1;
        else if (globdsize+diff.size() < 65536) diffs= 2;
        else diffs = 4;
        
        std::stringstream bufferloc;
        msgpack::pack(bufferloc, vec);
        string stloc = bufferloc.str();
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
            cout  << "cost function" << endl;
            diffdict = 0;
            //}
        }
        

    }
}

//to demostrate the ram cpu compression trade-offs. 
else diffdict = 0;
//diffdict = 1;
if (diffdict == 1){
    
    globaldict.insert(globaldict.end(), diff.begin(), diff.end());
//    std::clock_t start;
 //   start = std::clock();
 //vector <string> temp(glob.size()+diff.size());
    //merge6(glob.begin(), glob.end(), diff.begin(), diff.end(), temp.begin());
    for (string i : diff){
        glob[i] = 0;
    }
    //glob.assign(temp.begin(), temp.end());
    ////vector <string> temp(globaldict.size());
    //merge(glob.begin(), glob.end(),diff.begin(),diff.end(), temp.begin());
    //glob.assign( temp.begin(), temp.end() );
 //   duration5 += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
 //   cout << duration5 << endl;

    int value = 0;
    std::clock_t start;
    for(size_t index = globdsize; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;
    start = std::clock();
    duration5 += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration5 << endl;
    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, diff);
    string st = buffer.str();
    header.dictsize = st.size();
    global_dict_memory +=  header.dictsize;
    sizediff.push_back(header.dictsize);
   
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
    	//cout << (count*1.0)/vec.size() << endl;
    	if ((count*1.0)/vec.size()>0.2)
        	permanent_decision = 1;
    	*filter = filternew;
    
    }
    
    int value = 0;
    for(size_t index = 0; index < vec.size(); ++index)
        lookup[vec[index]] = index;

    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, vec);
    string st = buffer.str();
    header.dictsize = st.size();
    global_dict_memory +=  header.dictsize;
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
        fwrite(&st[0], header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
    

}
    return 1;

}
	
	
int compress_batch2(vector <string> vals, FILE *f1, bloom_filter *filter, bool &isdictionary, vector <int> &sizediff, vector <string> &globaldict, vector <string> &glob, unordered_map<string, size_t> &lookup, vector <short> &diffvals){
    struct D header;
    header.numofvals = vals.size();
    vector <string> minmax(2);
    unordered_set<string> s;
    for (string i : vals)
        s.insert(i);
    
    vector <string> vec;
    vec.assign(s.begin(), s.end());
    sort( vec.begin(), vec.end() );
    vector<string> diff;
    int ll = vec.size();
    
    minmax[0] = vec[0];
    minmax[1] = vec[ll-1];
    //std::vector<string> glob = globaldict;
    //std::sort(glob.begin(), glob.end());
    if (vec.size()*1.0/vals.size()>0.99){
            isdictionary = false;
            header.dictsize = 0;
            header.previndices = 0;
            globaldict.clear();
    		glob.clear();
    		sizediff.clear();
    		diffvals.clear();
    		lookup.clear();

    
    		std::stringstream buffermm;
    		msgpack::pack(buffermm, minmax);
    		string stmm = buffermm.str();
    		header.minmaxsize = 0;//stmm.size();

    
    		std::stringstream buffer;
    		msgpack::pack(buffer, vals);
    		string st = buffer.str();
            header.indicessize = st.size();
    		header.lendiff = 0;

        	header.bytes = 0;
        	fwrite(&header, sizeof(header), 1, f1);
        	//fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
        	fwrite(&st[0], header.indicessize ,1 , f1 );
    
    		
    }
    set_difference(vec.begin(), vec.end(), glob.begin(), glob.end(), std::inserter(diff, diff.begin()));
    
    int diffdict = 1;
    int globdsize = globaldict.size();
    if (globdsize+diff.size()>200000 or globdsize == 0)
        diffdict = 0;
    else if (globdsize > 0 and diff.size()*1.0/ll > 0.99)
        diffdict = 0;
    else if ((globdsize>=256 and ll < 256) or (globdsize >= 65536 and ll < 65536)  or  (globdsize < 256 and (globdsize + diff.size()) > 255) or ( globaldict.size() < 65536 and (globdsize + diff.size()) > 65535)){
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
        if (globdsize + diff.size() < 256) diffs = 1;
        else if (globdsize+diff.size() < 65536) diffs= 2;
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
    std::clock_t start;
    start = std::clock();
    vector <string> temp(globaldict.size());
    merge(glob.begin(), glob.end(),diff.begin(),diff.end(), temp.begin());
    glob = temp;
    duration5 += ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << duration5 << endl;
    

    int value = 0;
    for(size_t index = globdsize; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;

    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, diff);
    string st = buffer.str();
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
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
        fwrite(&st[0], header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
}

else if (diffdict == 0){
    globaldict.clear();
    sizediff.clear();
    diffvals.clear();
    globaldict.insert(globaldict.end(), vec.begin(), vec.end());
    glob = vec;
    
    int value = 0;
    for(size_t index = 0; index < globaldict.size(); ++index)
        lookup[globaldict[index]] = index;

    
    std::stringstream buffermm;
    msgpack::pack(buffermm, minmax);
    string stmm = buffermm.str();
    header.minmaxsize = stmm.size();

    
    std::stringstream buffer;
    msgpack::pack(buffer, vec);
    string st = buffer.str();
    header.dictsize = st.size();
   
    header.lendiff = ll;
    
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
        fwrite(&stmm[0], header.minmaxsize ,1 , f1 );
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
        fwrite(&st[0], header.dictsize ,1 , f1 );
        fwrite(&offsets,sizeof(unsigned int),vals.size(),f1);
    }
    

}
    return 1;

}




int compress(char* infile, char* outfile, int numofvals, char* attributes){
    vector <int> mcolumns = extractattributes(attributes);
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
    start = std::clock();
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
  cout << fileheader1.numofblocks << endl;
  fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
  long blocksizes[fileheader1.numofblocks];
  
  fwrite(blocksizes, (fileheader1.numofblocks)*sizeof(long), 1, f1);
  
  bool eof = false;
  string line;
  vector<string> dataset; // buffer that holds a batch of rows in raw text
  
  int num_of_vals1 = 0;
  
  while (!eof) {
    int numValues = 0;      // num of lines read in a batch

    dataset.clear();
    // read a batch of lines from the input file
    for (int i = 0; i < BLOCKSIZE; ++i) {
      if (!std::getline(finput, line) or num_of_vals1 >= fileheader1.numofvals) {
        eof = true;
        break;
      }
      dataset.push_back(line.substr(0, line.size()));  
      ++numValues;
      num_of_vals1++;      
    }
    /*compress batch*/
    long tell_init = ftell(f1);
    for (int k = 0; k < COLNUM+1; k++)
        fwrite(&columnindexes[k],sizeof(int),1,f1);
    long tell = ftell(f1);
           
    for (int j = 0; j < COLNUM; j++){
               long tell1 = ftell(f1);
               //compress(dataset, f1, sizediff[j], globaldict[j], glob[j], lookup[j], diffvals[j]);
               compress_batch(extractColumn(dataset,mcolumns[j]), f1, &filter, isdictionary, sizediff[j], globaldict[j], glob[j], lookup[j], diffvals[j], blocknum); 
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
         
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;

    std::cout<<"csv import time: "<< duration <<'\n';
    
    fseek(f1,ft,SEEK_SET);
    fileheader1.numofvals = num_of_vals1;
    fileheader1.numofblocks = ceil(fileheader1.numofvals*1.0/BLOCKSIZE);
    fwrite(&fileheader1, sizeof(fileheader1), 1, f1);
    blocksizes[fileheader1.numofblocks-1] = BLOCKSIZE;
    fwrite(blocksizes, (fileheader1.numofblocks)*sizeof(long), 1, f1);
    return (0);
}

int main(int argc, char** argv)
{
    if (argc != 5){
        cout << "wrong input" << endl;
    }
    /*TODO check input*/
    return compress(argv[1], argv[2], atoi(argv[3]), argv[4]);
}
