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

struct Dglob {
    int minmaxsize;
    int indicessize;
    int bytes;
    int numofvals;
};

struct fileHglob {
     int numofvals;
     int numofcols;
     int glob_size;
};



//filter
/*struct record {
    int rowid;
    string val;
};
*/

//unsigned int t2[20000000];

//struct record col[119944493];
//string column[119]; //decompress
//int col[119944493];

struct rec {
        string val;
        int rowid;
    };
    

template <typename T>
vector<size_t> sort_indexes(const vector<T> &v) {

  // initialize original index locations
  vector<size_t> idx(v.size());
  iota(idx.begin(), idx.end(), 0);

  // sort indexes based on comparing values in v
  sort(idx.begin(), idx.end(),
       [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});

  return idx;
}


map<int, int> mergejoin(vector <rec> a, vector<rec> b){
    
    int alen = a.size();
    int blen = b.size();
    map<int, int> c;
    
    int i = 0, j = 0, k = 0;
 
    while (i < alen and j < blen)
    {
        if (a[i].val < b[j].val){
            i++;
            k++;
            }
        else if (a[i].val > b[j].val){
            k++;
            j++;
            }
        else {
        	c.insert(std::make_pair(a[i].rowid, b[j].rowid+1));
        	k++;
        	j++;
        
        }
    }

        
 
    return c;
}



map<int, int> mergejoinglobal(vector <string> a, vector<string> b){
    
    int alen = a.size();
    int blen = b.size();
    map<int, int> c;
    
    int i = 0, j = 0, k = 0;
 
    while (i < alen and j < blen)
    {
        if (a[i] < b[j]){
            i++;
            k++;
            }
        else if (a[i] > b[j]){
            k++;
            j++;
            }
        else {
        	c.insert(std::make_pair(i, j+1));
        	k++;
        	j++;
        
        }
    }

        
 
    return c;
}



map<int, int> mergejoinparquet(vector <rec> a, vector<rec> b){
    
    int alen = a.size();
    int blen = b.size();
    map<int, int> c;
    
    int i = 0, j = 0, k = 0;
 
    while (i < alen and j < blen)
    {
        if (a[i].val < b[j].val){
            i++;
            k++;
            }
        else if (a[i].val > b[j].val){
            k++;
            j++;
            }
        else {
        	c.insert(std::make_pair(a[i].rowid, b[j].rowid+1));
        	k++;
        	i++;
        
        }
    }

        
 
    return c;
}

map<int, int> mergejoindict(vector <string> a, vector<string> b){
    
    int alen = a.size();
    int blen = b.size();
    map<int, int> c;
    
    int i = 0, j = 0, k = 0;
 
    while (i < alen and j < blen)
    {
        if (a[i] < b[j]){
            i++;
            k++;
            }
        else if (a[i] > b[j]){
            k++;
            j++;
            }
        else {
        	c.insert(std::make_pair(i, j+1));
        	k++;
        	j++;
        
        }
    }

        
 
    return c;
}

int binarySearch1(vector <rec> a, string b){
    for (int i = 0; i < a.size(); i++){
        if (a[i].val == b)
            return a[i].rowid;
    
    }
    return -1;
}


int binarySearch(vector <rec> arr, string x) 
{ 
  int r = arr.size(), l = 0;
  while (l <= r) 
  { 
    int m = l + (r-l)/2; 
  
    if (arr[m].val == x)  
        return arr[m].rowid;   

    if (arr[m].val < x)  
        l = m + 1;  

    else 
         r = m - 1;  
  } 
  
  // if we reach here, then element was not present 
  return -1;  
} 
  
  
bool compareByval(rec &a, rec &b)
{
    return a.val < b.val;
}

vector<rec> merge(vector <rec> a, vector<string> b, int init){
    
    
    
    int alen = a.size();
    int blen = b.size();
    int tlen = alen + blen;
    vector<rec> c(tlen);
    
    int i = 0, j = 0, k = 0;
 
    while (i < alen and j < blen)
    {
        if (a[i].val < b[j]){
            c[k].val = a[i].val;
            c[k].rowid = a[i].rowid;
            i++;
            k++;
            }
        else{
            c[k].val = b[j];
            c[k].rowid = init + alen + j;
            k++;
            j++;
            }
    }
    
    while (i < alen){
        c[k].val = a[i].val;
        c[k].rowid = a[i].rowid;
        k++;
        i++;
        }
 
    while (j < blen){
        c[k].val = b[j];
        c[k].rowid = init + alen + j;
        k++;
        j++;
        }
        
 
    return c;



}


int join_global(int argc, char * argv[]){
    int totalcount1=0;   
    int totalcount2=0;  
    FILE *f1, *f2;
    f1 = fopen(argv[1],"rb");
    f2 = fopen(argv[2],"rb");
    
    vector<string> parquetvalues1;
    vector<string> parquetvalues2;
    vector <string> global_dict1;
    vector <string> global_dict2;
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*load file 2 in memory*/
    fseek(f2, 0, SEEK_END);
    long fsize2 = ftell(f2);
    fseek(f2, 0, SEEK_SET);  
    char *fptr2 = (char*)malloc(fsize2 + 1);
    fread(fptr2, 1, fsize2, f2);
    fclose(f2);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    memcpy( marker, &fptr2[0], 4);
    
    struct fileHglob fileheader1;
	struct fileHglob fileheader2;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    memcpy(&fileheader2,&fptr2[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    vector <vector <int>> index2;
    
    unsigned int* t1 = new unsigned int[fileheader1.numofvals];
    unsigned int* t2 = new unsigned int[fileheader2.numofvals];

    
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	unsigned long initstep2 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	struct Dglob header2;
	
	
	int count = 0;
	vector<rec> c1;
	vector<rec> c2;
    int jointype = 0;
    int case1 = -1;
    int case2 = -1;
	int join1 = atoi(argv[3]);
	int join2 = atoi(argv[4]);

    
	char *buffer1 = (char*) malloc(fileheader1.glob_size);
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;

    char *buffer2 = (char*) malloc(fileheader2.glob_size);

    memcpy(buffer2,&fptr2[initstep2],fileheader2.glob_size);
    
    msgpack::unpacked result2;
    unpack(result2, buffer2,fileheader2.glob_size);
     
    vector<string> values2;
    result2.get().convert(values2);
    initstep2 += fileheader2.glob_size;
    rec rr;
    
    
  	for (int i=0; i < values1.size(); i++){
  			  rr.val = values1[i];
  			  rr.rowid = i;
  			  c1.push_back(rr);
    }
    
  	sort(c1.begin(), c1.end(), compareByval);
    
    for (int i=0; i < values2.size(); i++){
  			  rr.val = values2[i];
  			  rr.rowid = i;
  			  c2.push_back(rr);
    }
    
  	sort(c1.begin(), c1.end(), compareByval);
    
    sort(c2.begin(), c2.end(), compareByval);
    
    
    map<int, int> mapoffsets;
  		
  	mapoffsets = mergejoin(c1,c2);
  	
  	vector <int> a;
  	vector<vector <int>> vect(values1.size(), a); 
  	index1.insert( index1.end(), vect.begin(), vect.end() );
  	
  	vector <int> b;
  	vector<vector <int>> vect2(values2.size(), b); 
  	index2.insert( index2.end(), vect2.begin(), vect2.end() );
 
    while (1){
    if (totalcount1 < fileheader1.numofvals){
            memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
            totalcount1 += header1.numofvals;
            int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
            if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	index1[offf].push_back(i);
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{

    		if (header1.bytes==1){ // two byte offsets
    		    
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			
				for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }

     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }
     			initstep1 += next;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }
     			initstep1 += next;
     		}

     	}
		
    }
    
    if (totalcount2 < fileheader2.numofvals){
            memcpy(&header2,&fptr2[initstep2], sizeof(struct Dglob));
            totalcount2 += header2.numofvals;
            int next = sizeof(struct Dglob) + header2.minmaxsize + header2.indicessize;
            if (header2.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr2[initstep2+sizeof(struct Dglob)+header2.minmaxsize], header2.indicessize);
                for (int i=0; i < header2.numofvals; i++){
                    
     		    	index2[offf].push_back(i);
     		    }
     			initstep2 += next;
            }
        else{
    		if (header2.bytes==1){ // two byte offsets
    		    
     			unsigned short offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct Dglob)+header2.minmaxsize], header2.indicessize);
				for (int i=0; i < header2.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index2[offsets2[i]].push_back(i);
     			        
     			        }
     			initstep2 += next;
     		}
     		if (header2.bytes==0){ // four byte offsets
     			unsigned int offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct Dglob)+header2.minmaxsize], header2.indicessize);
				for (int i=0; i < header2.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index2[offsets2[i]].push_back(i);
     			        
     			        }
     			initstep2 += next;
     		}
     		if (header2.bytes==2){ // one byte offsets
     			unsigned char offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct Dglob)+header2.minmaxsize], header2.indicessize);
     			for (int i=0; i < header2.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index2[offsets2[i]].push_back(i);
     			        
     			        }
     			initstep2 += next;
     		}	
     	}
		
    }

    
    if (totalcount1 == fileheader1.numofvals and totalcount2 == fileheader2.numofvals) break;
    }
  	

  	
    long resultscount = 0;
  	for (int i=0; i < index1.size(); i++){
  		int j = mapoffsets[i] - 1;
  		if (j >= 0)
  			 for (int l=0; l < index1[i].size(); l++)
 				 resultscount += index2[j].size();
 		}
  	    cout << resultscount << endl;
}


int join_diff(int argc, char * argv[] ){
    
    int totalcount1=0;   
    int totalcount2=0;  
    FILE *f1, *f2;
    f1 = fopen(argv[1],"rb");
    f2 = fopen(argv[2],"rb");
    
    vector<string> parquetvalues1;
    vector<string> parquetvalues2;
    
    
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*load file 2 in memory*/
    fseek(f2, 0, SEEK_END);
    long fsize2 = ftell(f2);
    fseek(f2, 0, SEEK_SET);  
    char *fptr2 = (char*)malloc(fsize2 + 1);
    fread(fptr2, 1, fsize2, f2);
    fclose(f2);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    memcpy( marker, &fptr2[0], 4);
    
    struct fileH fileheader1;
	struct fileH fileheader2;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    memcpy(&fileheader2,&fptr2[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    vector <vector <int>> index2;
    
    unsigned int* t1 = new unsigned int[fileheader1.numofvals];
    unsigned int* t2 = new unsigned int[fileheader2.numofvals];
    //for(int i = 0; i < fileheader1.numofcols; ++i)
      //  t1[i] = new unsigned int[fileheader1.numofvals];
    
	unsigned long initstep1 = 4 + sizeof(struct fileH) + fileheader1.numofblocks*8;
	unsigned long initstep2 = 4 + sizeof(struct fileH) + fileheader2.numofblocks*8;
	struct D header1;
	struct D header2;
	
	
	int count = 0;
	vector<rec> c1;
	vector<rec> c2;
    int jointype = 0;
    int case1 = -1;
    int case2 = -1;
    /*
    0 row * row
    1 row * dict
    2 row * diff
    3 dict * row
    4 dict * dict
    5 dict * diff
    6 diff * row
    7 diff * dict
    8 diff * diff
    */
	int join1 = atoi(argv[3]);
	int join2 = atoi(argv[4]);
	
 	while (1){
    	/*read block header*/
    	
   		
   		
   		if (totalcount1 < fileheader1.numofvals){
   		   
   		
   		    int current, next;
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
   		    
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));

   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
   		        case1 = 1;
    		    char buffer1[header1.indicessize];
    			memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2],header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			rec rr;
    			
    			if (c1.size() == 0){
  				  for (int i=0; i < values1.size(); i++){
  			        rr.val = values1[i];
  			        rr.rowid = i;
  			        c1.push_back(rr);
  			    }
  			    sort(c1.begin(), c1.end(), compareByval);
  				}
  				else if(values1.size()>0){
  				    sort(values1.begin(), values1.end());
  			   		c1 = merge(c1, values1 , 0);
  			   		}
    					
   				totalcount1 += header1.numofvals;
   				initstep1 += next - current;
    		}
    		
    		else {
    		
   			char buffer1[header1.dictsize];
       		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
   			totalcount1 += header1.numofvals;
   			rec rr;
  			if (c1.size() == 0){
  			    for (int i=0; i < values1.size(); i++){
  			        rr.val = values1[i];
  			        rr.rowid = i;
  			        c1.push_back(rr);
  			    }
  			}
  			else if(values1.size()>0)
  			    c1 = merge(c1, values1, 0);
  			case1 = 0;
  			vector <int> a;
  			vector<vector <int>> vect(values1.size(), a); 
  			index1.insert( index1.end(), vect.begin(), vect.end() );
  			/*read offsets of file 1*/
  			
  			
    		if (header1.bytes==1){ // two byte offsets
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }
     			initstep1 += next- current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }
     			initstep1 += next - current;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index1[offsets1[i]].push_back(i);
     			        
     			        }
     			initstep1 += next - current;
     		}	
		}
		
   		
   		}
        
    	if (totalcount2 < fileheader2.numofvals){
    	  
    	    int current, next;

   		    memcpy(&current,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    memcpy(&next,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            initstep2 += current + sizeof(int)*(fileheader2.numofcols+1);
            //cout << totalcount2 << endl;
           
    	
    		memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    		//cout << header2.dictsize << " " << header2.indicessize << " " << header2.numofvals << " " << header2.bytes <<" "<<header2.lendiff<< endl;
    		if (header2.dictsize == 0){
    		    if (totalcount2 == 0)
   		            parquetvalues2.reserve(fileheader2.numofvals);
    		    char buffer2[header2.indicessize];
    			memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)+header2.previndices*2],header2.indicessize);
    			msgpack::unpacked result2;
    			unpack(result2, buffer2, header2.indicessize);
    			vector<string> values2;
    			result2.get().convert(values2);
    			parquetvalues2.insert( parquetvalues2.end(), values2.begin(), values2.end() );
   				totalcount2 += header2.numofvals;
   				initstep2 += next - current;
   				case2 = 1;
    		
    		}
    		
    		else {
    		    
    			char *buffer2 = (char*) malloc(header2.dictsize);
    			memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)+header2.previndices*2+header2.minmaxsize],header2.dictsize);
    			msgpack::unpacked result2;
    			unpack(result2, buffer2, header2.dictsize);
    			vector<string> values2;
    			result2.get().convert(values2);
    			totalcount2 += header2.numofvals;
    		    case2 = 0;
    		    rec rr;
    		    
    			if (c2.size() == 0){
  			    	for (int i=0; i < values2.size(); i++){
  			        	rr.val = values2[i];
  			        	rr.rowid = i;
  			        	c2.push_back(rr);
  			    	}
  			    
  			    
  				}
  				else if(values2.size()>0)
  				    c2 = merge(c2, values2, 0);
  			vector <int> a;
  			vector<vector <int>> vect(values2.size(), a); 
  			index2.insert( index2.end(), vect.begin(), vect.end() );
  			
  			/*read offsets of file 2*/
    		if (header2.bytes==1){ // two byte offsets
    		    
     			unsigned short offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.previndices*2+header2.minmaxsize+header2.dictsize], header2.indicessize);
     			if (atoi(argv[5]) != 1)
     			    for (int i=0; i < header2.numofvals; i++)
     			        t2[ totalcount2+i - header2.numofvals] = offsets2[i];
     			        
     			else
     			    for (int i=0; i < header2.numofvals; i++){
     			        //tree.Insert(offsets2[i],totalcount2-header2.numofvals+i);
     			        index2[offsets2[i]].push_back(i);
     			        
     			        }
     			        
     			initstep2 += next - current;	
     		}
     		 
     		if (header2.bytes==0){ // four byte offsets
     			unsigned int offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.previndices*2+header2.minmaxsize+header2.dictsize], header2.indicessize);
     			if (atoi(argv[5]) != 1)
     			    for (int i=0; i < header2.numofvals; i++)
     			        t2[totalcount2+i - header2.numofvals] = offsets2[i];
     			else
     			    for (int i=0; i < header2.numofvals; i++)
     			        index2[offsets2[i]].push_back(i);
     			
     			initstep2 += next - current;	
     		}
     		if (header2.bytes==2){ // one byte offsets
     			unsigned char offsets2 [header2.numofvals];
     			memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.previndices*2+header2.minmaxsize+header2.dictsize], header2.indicessize);
     		if (atoi(argv[5]) != 1)
     			    for (int i=0; i < header2.numofvals; i++)
     			        t2[totalcount2+i - header2.numofvals] = offsets2[i];
     			else
     			    for (int i=0; i < header2.numofvals; i++){
     			        index2[offsets2[i]].push_back(i);
     			        }
     			        
     			initstep2 += next - current;
     		}
     		
        
        }
        }
    	if (totalcount1 == fileheader1.numofvals and totalcount2 == fileheader2.numofvals) break;
	}
	
    
	
	for(int i=0; i<c1.size(); ++i)
	    if (c1[i].val == "10.1103/physrevc.74.015801")
            std::cout << c1[i].val << ' ' <<c1[i].rowid<<endl;
            
    for(int i=0; i<c2.size(); ++i)
	    if (c2[i].val == "10.1103/physrevc.74.015801")
            std::cout << c2[i].val << ' ' <<c2[i].rowid<<endl;
	
	if (case1 == 0 and case2 == 0){ // diff join diff
  		map<int, int> mapoffsets;
  		
  		mapoffsets = mergejoin(c1,c2); /* offsets.rowid1 contains offset1 and rowid2 offset2 from file 1 and 2 */
  		long resultscount = 0;
  		for (int i=0; i < index1.size(); i++){
  			int j = mapoffsets[i] - 1;
  			if (j >= 0)
  			    for (int l=0; l < index1[i].size(); l++)
 				    resultscount += index2[j].size();
 		}
 		
  	    cout << resultscount << endl;
    }
    
    if (case1 == 1 and case2 == 0){ // parquet join diff
  		map<int, int> mapoffsets;
  		//cout << c2[0].rowid << " " << c2[1].rowid << "l" << c2[0].val << "l" << c2[1].val << "l" << endl;
  		
  		mapoffsets = mergejoinparquet(c1,c2); /* offsets.rowid1 contains offset1 and rowid2 offset2 from file 1 and 2 */
  		long resultscount = 0;
  		for (int i=0; i < c1.size(); i++){
  			int j = mapoffsets[i] - 1;
  			if (j >= 0)
 				 resultscount += index2[j].size();
 		}

  	    cout << resultscount << endl;
    }
    
    
    if (case1 == 2 and case2 == 0){ // parquet join diff
  		
  		
  		long resultscount = 0;
  		for (int i=0; i<totalcount1; i++){
  		    int j = binarySearch(c2, parquetvalues1[i]);
  		    if (j>=0)
 			    resultscount += index2[j].size();
  		}
  	
  	
  	
  		cout << resultscount << endl;
    }
    

    delete[] t1;
    delete[] t2;
    free(fptr1);
	fptr1 = NULL;
	free(fptr2);
	fptr2 = NULL;
    return 0; 

	
	
    



}

int join_parquet(int argc, char * argv[] ){
 
    int totalcount1=0;   
    int totalcount2=0;  
    FILE *f1, *f2;
    f1 = fopen(argv[1],"rb");
    f2 = fopen(argv[2],"rb");
    
    
    
    /*load file 1 in memory*/
    
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*load file 2 in memory*/
    fseek(f2, 0, SEEK_END);
    long fsize2 = ftell(f2);
    fseek(f2, 0, SEEK_SET);  
    char *fptr2 = (char*)malloc(fsize2 + 1);
    fread(fptr2, 1, fsize2, f2);
    fclose(f2);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    memcpy( marker, &fptr2[0], 4);


    int join1 = atoi(argv[3]);
	int join2 = atoi(argv[4]);
    struct fileH fileheader1;
	struct fileH fileheader2;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    memcpy(&fileheader2,&fptr2[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    vector <vector <int>> index2;
    
    unsigned int* t1 = new unsigned int[fileheader1.numofvals];
    unsigned int* t2 = new unsigned int[fileheader2.numofvals];
    //for(int i = 0; i < fileheader1.numofcols; ++i)
      //  t1[i] = new unsigned int[fileheader1.numofvals];
    
	unsigned long initstep1 = 4 + sizeof(struct fileH);
	unsigned long initstep2 = 4 + sizeof(struct fileH);

	struct D header1;
	struct D header2;
	
	
	int count = 0;
	vector<rec> c1;
	vector<rec> c2;

	int numofvals1=0, numofvals2=0, numofvals1fin=0,numofvals2fin=0, check1=0, check2=0;
	long resultscount = 0;
	
 	while (1){
    	/*read block header*/
   		
   		if (totalcount1 < fileheader1.numofvals){
   		    
   		    int current, next;
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));
            
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)]+header1.minmaxsize,header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
   			totalcount1 += header1.numofvals;
  			    
  			/*read offsets of file 1*/
    		if (header1.bytes==1){ // two byte offsets
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4 + sizeof(struct fileH) ;
  			    totalcount2 = 0;
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
   		    	
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					std::map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;	
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			       
     						initstep2 = initstep2+next2;		
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     					
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
        		} else break;
   		}
     			
     		
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4;
  			totalcount2 = 0;
   		
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    				
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header1.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header1.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;		
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+ header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     		
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
     		
        		} else break;
        	
   		
   		
   		}
     		
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4;
  			totalcount2 = 0;
   		
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    				
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;	
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     		
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
     		
        		} else break;
        	
   		
   		
   		}
     		}
  	
  			
   		} else break;
   		
   		
   		

    	
        
    	
	}
	
  	
  	cout << resultscount << endl;

    
    return 0; 


}



int join_adaptive(int argc, char * argv[] ){
 
    int totalcount1=0;   
    int totalcount2=0;  
    FILE *f1, *f2;
    f1 = fopen(argv[1],"rb");
    f2 = fopen(argv[2],"rb");
    
    
    
    /*load file 1 in memory*/
    
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*load file 2 in memory*/
    fseek(f2, 0, SEEK_END);
    long fsize2 = ftell(f2);
    fseek(f2, 0, SEEK_SET);  
    char *fptr2 = (char*)malloc(fsize2 + 1);
    fread(fptr2, 1, fsize2, f2);
    fclose(f2);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    memcpy( marker, &fptr2[0], 4);


    int join1 = atoi(argv[3]);
	int join2 = atoi(argv[4]);
    struct fileH fileheader1;
	struct fileH fileheader2;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    memcpy(&fileheader2,&fptr2[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    vector <vector <int>> index2;
    
    unsigned int* t1 = new unsigned int[fileheader1.numofvals];
    unsigned int* t2 = new unsigned int[fileheader2.numofvals];
    //for(int i = 0; i < fileheader1.numofcols; ++i)
      //  t1[i] = new unsigned int[fileheader1.numofvals];
    
	unsigned long initstep1 = 4 + sizeof(struct fileH);
	unsigned long initstep2 = 4 + sizeof(struct fileH);

	struct D header1;
	struct D header2;
	
	
	int count = 0;
	vector<rec> c1;
	vector<rec> c2;

	int numofvals1=0, numofvals2=0, numofvals1fin=0,numofvals2fin=0, check1=0, check2=0;
	long resultscount = 0;
	
 	while (1){
    	/*read block header*/
   		
   		if (totalcount1 < fileheader1.numofvals){
   		
   		    int current, next;
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));

   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)]+header1.minmaxsize,header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
   			totalcount1 += header1.numofvals;
  			    
  			/*read offsets of file 1*/
    		if (header1.bytes==1){ // two byte offsets
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4 + sizeof(struct fileH) ;
  			    totalcount2 = 0;
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
   		    	
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					std::map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;	
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			       
     						initstep2 = initstep2+next2;		
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     					
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
        		} else break;
   		}
     			
     		
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4;
  			totalcount2 = 0;
   		
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    				
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header1.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header1.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;		
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+ header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     		
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
     		
        		} else break;
        	
   		
   		
   		}
     		
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.minmaxsize+header1.dictsize], header1.indicessize);
     			initstep1 = initstep1+next;
     			
     			initstep2 = 4;
  			totalcount2 = 0;
   		
   		    while (1){
   		    	if (totalcount2 < fileheader2.numofvals){
    	    		int current2, next2;
   		    		memcpy(&current2,&fptr2[initstep2+sizeof(int)*join2],sizeof(int));
   		    		memcpy(&next2,&fptr2[initstep2+sizeof(int)*(fileheader2.numofcols)],sizeof(int));
            		initstep2 += current2 + sizeof(int)*(fileheader2.numofcols+1);
    				memcpy(&header2,&fptr2[initstep2], sizeof(struct D));
    				
    					char buffer2[header2.dictsize];
    					memcpy(buffer2,&fptr2[initstep2+sizeof(struct D)]+header2.minmaxsize,header2.dictsize);
    					msgpack::unpacked result2;
    					unpack(result2, buffer2, header2.dictsize);
    					vector<string> values2;
    					result2.get().convert(values2);
    					totalcount2 += header2.numofvals;
    		
    					map<int, int> mapoffsets;
    					mapoffsets = mergejoindict(values1,values2); 
  			 
  						vector <int> a;
  						vector<vector <int>> index2(values2.size(), a); 
  						/*read offsets of file 2*/
    					if (header2.bytes==1){ // two byte offsets
     						unsigned short offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;	
     					}
     					if (header2.bytes==0){ // four byte offsets
     						unsigned int offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     			
     						initstep2 = initstep2+next2;
     					}
     					if (header2.bytes==2){ // one byte offsets
     						unsigned char offsets2 [header2.numofvals];
     						memcpy(offsets2, &fptr2[initstep2+sizeof(struct D)+header2.minmaxsize+header2.dictsize], header2.indicessize);
     						for (int i=0; i < header2.numofvals; i++)
     			        		index2[offsets2[i]].push_back(i);
     						initstep2 = initstep2+next2;
     					}
     		
     		
  						for (int i=0; i<header1.numofvals; i++){
  						    int j = mapoffsets[offsets1[i]] - 1;
  						    if (j >= 0)
 								resultscount += index2[j].size();
 						}
     		
        		} else break;
        	
   		
   		
   		}
     		}
  	
  			
   		} else break;
   		
   		
   		

    	
        
    	
	}
	
  	
  	cout << resultscount << endl;

    
    return 0; 


}



int main(int argc, char * argv[] ){
  
  if (strstr(argv[1], "diff"))
     return join_diff(argc,argv);
  if (strstr(argv[1], "glob"))
     return join_global(argc,argv);
     
  return join_parquet(argc,argv);

}
