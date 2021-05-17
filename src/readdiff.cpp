#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <msgpack.hpp>
#include "cache.h"
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

int SNAPPY = 0;

size_t result;
  
int compare(int offset, string op, vector <float> predicate, vector <int> predminmax){
    if (op==">")
    for (int i=0; i < predicate.size(); i++)
        if (offset > predicate[i] and offset <= predminmax[i])
            return 1;
            
    if (op == "<")
         for (int i=0; i < predicate.size(); i++)
        if ( offset < predicate[i] and offset >= predminmax[i])
            return 1;
return 0;

}




vector <int> extractattributes(std::string s) {
  vector <int> columns;
  string parsed;
  stringstream input_stringstream(s);
  
  while (getline(input_stringstream,parsed,',')){
     columns.push_back(stoi(parsed));
}  
  return columns;
}




int read_diff_materialize(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    vector <string> parquetvalues1;
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);  
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileH fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;

    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH) + fileheader1.numofblocks*8;
	struct D header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
    	/*read block header*/
   		    int current, next;
   		    
   		    int i1,i2,i3;
   		    
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));
   //		    cout << header1.dictsize << " "<< header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<endl;
            
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    			memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			count += values1.size();
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				totalcount1 += header1.numofvals;
   				initstep1 += next-current;

    		}
    		else {
            
            vector<string> values1;
            if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
    		  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		  msgpack::unpacked result1;
    		  unpack(result1, buffer1, header1.dictsize);
    		  result1.get().convert(values1);
    		}
    		
    		
   			totalcount1 += header1.numofvals;

   			if (header1.diff == 1){
   			    global_dict.clear();
   			}

   			
   			global_dict.insert(global_dict.end(), values1.begin(), values1.end());

   			int lendict = global_dict.size();
   			if (header1.indicessize == 4){
                int offf;
                
                memcpy(&offf, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
                if (offf < lendict)
                for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offf];
     		    	count++;
     		    }
     		    else
     		    for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offf-lendict];
     		    	count++;
     		    }
     			initstep1 += next - current;
            
            }
            else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
     			
				if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
				if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     		
     			unsigned char offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
     			if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    
     		    	count++;
     		    }
     		    else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = global_dict[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next-current ;
     		}
     		}
     		
     		
		}
	}

    cout << count << " " << column[10] << endl;
    fclose(f1);
    return 0; 
}

int read_diff(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    vector<string> parquetvalues1;
    /*load file 1 in memory*/
    fseek(f1, 0, SEEK_END);
    long fsize1 = ftell(f1);
    fseek(f1, 0, SEEK_SET);
    char *fptr1 = (char*)malloc(fsize1 + 1);
    result =  fread(fptr1, 1, fsize1, f1);
    fclose(f1);
    
    /*read marker of file*/
    char marker[5];
    memcpy( marker, &fptr1[0], 4);
    
    struct fileH fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;

    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH) + fileheader1.numofblocks*8;
	struct D header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
    	/*read block header*/
   		    int current, next;
   		    
   		    int i1,i2,i3;
   		    
   		    memcpy(&current,&fptr1[initstep1+sizeof(int)*join1],sizeof(int));
   		    memcpy(&next,&fptr1[initstep1+sizeof(int)*(fileheader1.numofcols)],sizeof(int));
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
   		    memcpy(&header1,&fptr1[initstep1], sizeof(struct D));
   		    
   		    if (header1.dictsize == 0){
   		        
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    			memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.indicessize);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				totalcount1 += header1.numofvals;
   				initstep1 += next-current;
    		}
    		else {
    	   vector<string> values1;
    	   if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		result1.get().convert(values1);
    		}
   			totalcount1 += header1.numofvals;
   			if (header1.diff == 1){
   			    global_dict.clear();
   			}
   			
   			int lendict = global_dict.size();
   			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
                if (offf < lendict)
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = global_dict[offf];
     		    	count++;
     		    }
     		    else
     		    for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf-lendict];
     		    	count++;
     		    }
     			initstep1 += next - current;
            }
            else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
				//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];*/
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
				//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    */
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
    		       memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize+header1.minmaxsize], header1.indicessize);
     			//if (values1.size()>0)
     			for (int i=0; i < header1.numofvals; i++){
				    /*if (offsets1[i] < lendict)
     		    	    column[count] = global_dict[offsets1[i]];
     		    	else
     		    	    column[count] = values1[offsets1[i]-lendict];
     		    	    */
     		    	count++;
     		    }
     		  /*  else
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }*/
     			initstep1 += next-current ;
     		}
     		}
     		
     		global_dict.insert(global_dict.end(), values1.begin(), values1.end());
     		
		}
	}

    cout << count << endl;
    fclose(f1);
    return 0; 
}


int random_access_diff(int argc, char * argv[] ){
       
        FILE *f1;
        f1 = fopen(argv[1],"rb");
        int row = atoi(argv[3]);

        
        int fcount = 0;
        int offset = -1;
        int global_len = 0;
        /*read marker of file*/
        char marker[5];
        result =  fread( marker, 4, 1, f1);
        
        struct fileH fileheader1;
        result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
        vector <vector <int>> index1;

        int* col = new int[fileheader1.numofvals];
        long data[fileheader1.numofblocks];
        
        int rowid = 0;
        unsigned long initstep1 = 0;
        int blocknum = 0;
        result =  fread(data,fileheader1.numofblocks*8,1,f1);
        if (fileheader1.numofblocks > 1){
        	
        	blocknum = row/data[fileheader1.numofblocks -1];
        	rowid = row%data[fileheader1.numofblocks -1];
        	initstep1 = data[blocknum];
        }
        else {
            rowid =  row;
            initstep1 = ftell(f1);
        }
        
        
        //cout << data[blocknum] << endl;
        
        struct D header1;
        
        
        int count = 0;
        int join1 = atoi(argv[2]);
        
        int current;
        fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
        result =  fread(&current,sizeof(int),1,f1);
        initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
        fseek(f1,initstep1, SEEK_SET);
        result =  fread(&header1, sizeof(struct D),1,f1);
        unsigned short int previndex[header1.previndices];
        result =  fread(previndex, header1.previndices * 2, 1, f1);
        
        if (header1.dictsize == 0){
                    vector<string> values1;
                    char buffer1[header1.indicessize];
                    result =  fread(buffer1,header1.indicessize,1,f1);
                    msgpack::unpacked result1;
                    unpack(result1, buffer1, header1.indicessize);
                    result1.get().convert(values1);
                    cout << values1[rowid] << endl;
                    return 1;
        }
        else if (header1.diff == 0){
        
            int off;
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
              if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned short offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize+rowid*2,SEEK_SET);
                  result =  fread(&offsets1,2,1,f1);
                  off = offsets1;
                  }
              }
              
              if (header1.bytes==0){ // four byte offsets
                  if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned int offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid*4,SEEK_SET);
                  result =  fread(&offsets1,4,1,f1);
                  off = offsets1;
                  }
              }
              if (header1.bytes==2){ // one byte offsets
              if (SNAPPY){
              fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                  unsigned char offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid,SEEK_SET);
                  result =  fread(&offsets1,1,1,f1);
                  off = offsets1;
                  }
              }
                
        
    int temp = 0;
    int prevtemp = 0;
    int rightblock = 0;
    int found = 0;
    int position_in_block;
    int reelative_block_num = 0;

    for (int co = 1; co<=header1.previndices; co+=2){
        prevtemp = temp;
        temp += previndex[co];

        if (temp < off)
            continue;
        else {
            found = 1;
            position_in_block = off - prevtemp;
            rightblock = previndex[co-1];
            break;
        }
    }
      
        if (header1.previndices == 0){ //i am in the first block
                found = 1;
                position_in_block = off;
                
            }
        if (found == 0){
            position_in_block = off - temp;
            rightblock = blocknum;
        }
        
        if (blocknum != rightblock){
            unsigned long initstep2 = data[rightblock];
            struct D header2;
            int current2;
            fseek(f1, initstep2 + sizeof(int)*join1, SEEK_SET);
            result =  fread(&current2,sizeof(int),1,f1);
            initstep2 += current2 + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep2, SEEK_SET);
            result =  fread(&header2, sizeof(struct D),1,f1);
            
            vector<string> values1;
            fseek(f1,initstep2+sizeof(struct D)+header2.minmaxsize+ header2.previndices*2,SEEK_SET);
            if (SNAPPY){
   			             char buffer1[header2.dictsize];
    		             result =  fread(buffer1,header2.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header2.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                char buffer1[header2.dictsize];
            
                result =  fread(buffer1,header2.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header2.dictsize);
                result1.get().convert(values1);
            }
            cout << values1[position_in_block] << endl;
        }
        else {
            
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
            if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                 char buffer1[header1.dictsize];
                 result =  fread(buffer1,header1.dictsize,1,f1);
                 msgpack::unpacked result1;
                 unpack(result1, buffer1, header1.dictsize);
                 result1.get().convert(values1);
            }
            cout << values1[position_in_block] << endl;
        }
   // cout <<  position_in_block << " "<< rightblock << endl;
        
        }
    
        else if (header1.diff == 1){
        
            int off;
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned short offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize+rowid*2,SEEK_SET);
                result =  fread(&offsets1,2,1,f1);
                off = offsets1;
                }
            }
            
            if (header1.bytes==0){ // four byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned int offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid*4,SEEK_SET);
                result =  fread(&offsets1,4,1,f1);
                off = offsets1;
                }
            }
            if (header1.bytes==2){ // one byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       off = offsets1[rowid];
    		    }
    		    else{
                unsigned char offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid,SEEK_SET);
                result =  fread(&offsets1,1,1,f1);
                off = offsets1;
                }
            }
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
             if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		}
            else{
                char buffer1[header1.dictsize];
                result =  fread(buffer1,header1.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header1.dictsize);
                result1.get().convert(values1);
            }
            cout << values1[off] << endl;
            
            
        }
        fclose(f1);
        return 0;
    }



auto get_column_value(FILE *f1, int blocknum, long int blockstart, struct fileH fileheader1, struct D header1, vector <int> columns, int* rowids, int rowidsnum, int join1, vector <string> &vec, long* data, unordered_map <int, vector <string>> &dict_cache){
/*

rowids are the relative rowids of the specific block
TODO gets list with row ids and returns the values from the other columns
Several trade-offs and optimisation opportunities


-- A cache with blocks/offsets, so that we do not read the disk multiple times for each rowid
-- for each row id there are 2 cases:
- the other column is not dictionary encoded -> get the other column and retrieve the value
- the other column is dictionary encoded -> get the offset and given the min/max values calculate the dictionary in which the value exists. 
This dictionary may be already in the cache, so fseek is avoided. The cache is big enough to hold ALL the dictionaries of the sequence.


*/

  
  vector <vector <string>> cols(columns.size(), std::vector<string>(rowidsnum));
  int colnum = -1;
  int initstep;
  for (int i: columns){
    colnum++;
    if (i == join1){
        cols[colnum] = vec;
    }
    else{
    
    initstep = blockstart;
    int current;
  	fseek(f1, initstep + sizeof(int)*i, SEEK_SET);
   	result = fread(&current,sizeof(int),1,f1);
   	fseek(f1, initstep + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
    initstep += current + sizeof(int)*(fileheader1.numofcols+1);
    fseek(f1, initstep, SEEK_SET);
   	result =  fread(&header1, sizeof(struct D),1,f1);
   	
   	    unsigned short int previndex[header1.previndices];
        result =  fread(previndex, header1.previndices * 2, 1, f1);
    
    if (header1.dictsize == 0){ // case no dictionary
      
      char buffer1[header1.indicessize];
      result =  fread(buffer1,header1.indicessize,1,f1);
      msgpack::unpacked result1;
      unpack(result1, buffer1, header1.indicessize);
      vector<string> values1;
      result1.get().convert(values1);
      
      int c = 0;
      for (int j = 0; j < rowidsnum; j++){
         cols[colnum][c] = values1[rowids[j]];
         c++;
       
      }
    
    }
    
    
    else if (header1.diff == 0){
     //case differential dictionary
     int c = 0;

      
        int initstep1 = initstep;

        int off[rowidsnum];
        
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
              if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                  unsigned short offsets1[header1.numofvals];
                  //fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize+rowids[j]*2,SEEK_SET);
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                  result =  fread(&offsets1,2,header1.numofvals,f1);
                  for (int j = 0; j < rowidsnum; j++)
                      off[j] = offsets1[rowids[j]];
                      
                  }
              }
              
              if (header1.bytes==0){ // four byte offsets
                  if (SNAPPY){
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                  unsigned int offsets1[header1.numofvals];
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                  result =  fread(&offsets1,4,header1.numofvals,f1);
                  for (int j = 0; j < rowidsnum; j++)
                      off[j] = offsets1[rowids[j]];
                  }
              }
              if (header1.bytes==2){ // one byte offsets
              if (SNAPPY){
              fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                  unsigned char offsets1[header1.numofvals];
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                  result =  fread(&offsets1,1,header1.numofvals,f1);
                  for (int j = 0; j < rowidsnum; j++)
                      off[j] = offsets1[rowids[j]];
                  }
              }
               
    for (int j = 0; j < rowidsnum; j++){
    int temp = 0;
    int prevtemp = 0;
    int rightblock = 0;
    int found = 0;
    int position_in_block;
    int reelative_block_num = 0;
    
    for (int co = 1; co<=header1.previndices; co+=2){
        prevtemp = temp;
        temp += previndex[co];

        if (temp < off[j])
            continue;
        else {
            found = 1;
            position_in_block = off[j] - prevtemp;
            rightblock = previndex[co-1];
            break;
        }
    }
    
      
       
        if (found == 0){
            position_in_block = off[j] - temp;
            rightblock = blocknum;
        }
        
        if (blocknum != rightblock){
            if (dict_cache.find(rightblock) == dict_cache.end()){
            unsigned long initstep2 = data[rightblock];
             
            struct D header2;
            int current2;
            
            fseek(f1, initstep2 + sizeof(int)*i, SEEK_SET);
            result =  fread(&current2,sizeof(int),1,f1);
            initstep2 += current2 + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep2, SEEK_SET);
            result =  fread(&header2, sizeof(struct D),1,f1);
            vector<string> values1;
            fseek(f1,initstep2+sizeof(struct D)+header2.minmaxsize+ header2.previndices*2,SEEK_SET);
            
            if (SNAPPY){
   			             char buffer1[header2.dictsize];
    		             result =  fread(buffer1,header2.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header2.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		             dict_cache[rightblock] = values1;
    		}
            else{
                char buffer1[header2.dictsize];
                result =  fread(buffer1,header2.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header2.dictsize);
                result1.get().convert(values1);
                dict_cache[rightblock] = values1;
                
            }
            cols[colnum][c] = values1[position_in_block];
            //cout << values1[position_in_block] << endl;
            c++;
           }
           else {
           cols[colnum][c] = dict_cache[rightblock][position_in_block];
            //cout << values1[position_in_block] << endl;
            c++;
           
           
           }
            
        }
        else {
            if (dict_cache.find(rightblock) == dict_cache.end()){
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
            if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		             dict_cache[rightblock] = values1;
    		}
            else{
                 char buffer1[header1.dictsize];
                 result =  fread(buffer1,header1.dictsize,1,f1);
                 msgpack::unpacked result1;
                 unpack(result1, buffer1, header1.dictsize);
                 result1.get().convert(values1);
                 dict_cache[rightblock] = values1;
            }

            cols[colnum][c] = values1[position_in_block];
            c++;
            }
            else {
                
                cols[colnum][c] = dict_cache[rightblock][position_in_block];
                c++;
            
            }
        }
        }
    }
        else if (header1.diff == 1){ //local dictionary
        int c = 0;
            int initstep1 = initstep;
            int off[rowidsnum];
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned short offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		       	   off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                unsigned short offsets1[header1.numofvals];
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                result =  fread(&offsets1,2,header1.numofvals,f1);
                for (int j = 0; j < rowidsnum; j++)
                    off[j] = offsets1[rowids[j]];
                
                }
            }
            
            
            if (header1.bytes==0){ // four byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned int offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		       	   off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                unsigned int offsets1[header1.numofvals];
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                result =  fread(&offsets1,4,header1.numofvals,f1);
                for (int j = 0; j < rowidsnum; j++)
                    off[j] = offsets1[rowids[j]];
                
                }
            }
            if (header1.bytes==2){ // one byte offsets
            if (SNAPPY){
            fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                   unsigned char offsets1[header1.numofvals];
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		       for (int j = 0; j < rowidsnum; j++)
    		       	   off[j] = offsets1[rowids[j]];
    		    }
    		    else{
                unsigned char offsets1[header1.numofvals];
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
                result =  fread(&offsets1,1,header1.numofvals,f1);
                for (int j = 0; j < rowidsnum; j++)
                    off[j] = offsets1[rowids[j]];
                }
            }
            
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            vector<string> values1;
             if (SNAPPY){
   			             char buffer1[header1.dictsize];
    		             result =  fread(buffer1,header1.dictsize,1,f1);
    		             string output;
    		             snappy::Uncompress(buffer1, header1.dictsize, &output);
    		             msgpack::unpacked result1;
    		             unpack(result1, output.data(), output.size());
    		             result1.get().convert(values1);
    		             dict_cache[0] = values1;
    		}
            else{
                char buffer1[header1.dictsize];
                result =  fread(buffer1,header1.dictsize,1,f1);
                msgpack::unpacked result1;
                unpack(result1, buffer1, header1.dictsize);
                result1.get().convert(values1);
                dict_cache[0] = values1;
                
            }
            for (int j = 0; j < rowidsnum; j++){
                cols[colnum][c] = values1[off[j]];
                c++;
            //cout << values1[off] << endl;
            
            }

    
    }
    }

}

return cols;
}

int equi_filter(int argc, char* filename,char* col_num,char* val,char* boolmin,char* boolmindiff,char* retcols){
    
    int ommits = 0;
    int ommits2 = 0;
    unordered_map <int, vector <string>> dict_cache;
    vector <vector <string>> cols;
    vector <int> retcolumns = extractattributes(retcols);
    int colnum = retcolumns.size();
    
    int boolminmax = atoi(boolmin); 
    int boolminmax_diff =  atoi(boolmindiff);
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(filename,"rb");
    
    vector<string> parquetvalues1;
    string value = val;
    int fcount = 0;
    int offset = -1;
    int global_len = 0;
    /*read marker of file*/
    char marker[5];
    result =  fread( marker, 4, 1, f1);
    
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;

    int* col = new int[fileheader1.numofvals];

    long data[fileheader1.numofblocks];
    result =  fread(data,fileheader1.numofblocks*8,1,f1);

	unsigned long initstep1 = 4 + sizeof(struct fileH)+fileheader1.numofblocks*8;
	struct D header1;
	
	
	int count = 0;
	int join1 = atoi(col_num);
	int blocknum = -1;
	
	
 	while (totalcount1 < fileheader1.numofvals){
 	     	
    		int found_index = 0;
 	        blocknum++;
   		    int current, next;
   		    int blockstart = initstep1;
   		    fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
   		    result = fread(&current,sizeof(int),1,f1);
   		    fseek(f1, initstep1 + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
   		    result =  fread(&next,sizeof(int),1,f1);
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep1, SEEK_SET);
   		    result =  fread(&header1, sizeof(struct D),1,f1);
   		    totalcount1 += header1.numofvals;
   		    int rowids[header1.numofvals]; //TODO dynamic memory allocation
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    		    //fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			msgpack::unpacked result1;
    			
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			count += values1.size();
    			for(string i : values1) 
                    if (i == value){
                      found_index++;
                      fcount++;
                      }
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
    			std::vector<string> myvec(found_index, value);
     		    cols = get_column_value(f1, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, myvec, data, dict_cache);
     		    for (int i=0; i<cols[0].size(); i++){
     		     for (int j=0; j<cols.size(); j++){
     		      if (j == cols.size()-1)
        			cout << cols[j][i];
        		  else {
        		  cout << cols[j][i] ;
        		  cout << "\033[1;31m|\033[0m";
        		  }
    			}
    		  cout << endl;	
     		}
   				//totalcount1 += header1.numofvals;
   				initstep1 += next-current;
   				
    		}
    		else {
    		int check = 0;
    		if (header1.diff == 1){  global_len = 0; dict_cache.clear();}
    		if(offset  == -1 or header1.diff == 1){
    		 int temp = global_len;
    		 global_len += header1.lendiff;
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 vector<string> values1;
   			 if (header1.lendiff > 0){
   			   if (header1.diff == 0){
    	     if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			   char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1,  buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    }
    		    //cout << values1.front() << " la "<<values1.back() << " la "<<minmax1[0] << " la -" <<minmax1[1]<<endl;
    		    if (boolminmax_diff)
    			if (value.compare(values1.front())<0 or value.compare(values1.back())>0){
   		      		count += header1.numofvals;
   		      		//cout << "kaka" << endl;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		ommits++;
   		      		offset = -1;
   		      	continue;
   		    	} 
   		    	}
   		    else {
   		        check = 1;
   		    	if (boolminmax and (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0)){
   		    	    ommits++;
   		      		count += header1.numofvals;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		offset = -1;
   		      	continue;
   		    	}
   		    	else {
   		    	if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   		    	char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    }
   		    	}
   		    	}
   		    }

    		 for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k + temp;
   			     break;
   			     }
   			     offset = -1;
   			 }
   		    
             if (offset == -1){
                //if (header1.lendiff == 0)
                    ommits2++;
                count += header1.numofvals;
                initstep1 += next-current;
                continue;
             }
   			}
   			if (!check){
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;

    		 minmax.get().convert(minmax1);
    		 if (boolminmax)
    		 if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
    		     ommits++;
    		     count += header1.numofvals;
   		      	 initstep1 += next-current;
    		     continue;
    		 }
   			}
   			if (header1.indicessize == 4){
   			    
                int offf;
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (offf == offset){
                  for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	rowids[found_index] = i; 
     		        found_index++;
     		    	fcount++;
     		    	count++;
     		    }
     		    // TODO in this case, the full block evaluates
     		    std::vector<string> myvec(found_index, value);

   

     		    cols = get_column_value(f1, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, myvec, data, dict_cache);
     		    //cols = get_column_value(f1, blocknum, blockstart, fileheader1, header1, retcolumns, col, fcount, join);
     		    
     		    }
     		    else {count += header1.numofvals;}
     			initstep1 += next-current;
            
        }
        else{
            
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
     			
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);

				for (int i=0; i < header1.numofvals; i++){
     		    
     		        if (offsets1[i] == offset){
     		        	//col[fcount].rowid = count;
     		            //col[fcount].val = value;
     		            rowids[found_index] = i; 
     		            found_index++;
     		            col[fcount] = count;
     		            fcount++;
     		        }
     		        count++;
     		    
     		    
     		    
    			}
     			initstep1 += next-current;
     		}

     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);

				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        rowids[found_index] = i; 
     		        found_index++;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		    
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);

     			for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        rowids[found_index] = i; 
     		        found_index++;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next-current ;
     		}
     		std::vector<string> myvec(found_index, value);
     		cols = get_column_value(f1, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, myvec, data, dict_cache);
     		
     		
     		
     		 for (int i=0; i<cols[0].size(); i++){
     		     for (int j=0; j<cols.size(); j++){
     		      if (j == cols.size()-1)
        			cout << cols[j][i];
        		  else {
        		  cout << cols[j][i] ;
        		  cout << "\033[1;31m|\033[0m";
        		  }
    			}
    		  cout << endl;	
     		}
     		}
		}
	}
    /*for (int i=0; i<fcount; i++){
        cout << col[i] << endl;
    }*/
    
    
    
    cout <<  fcount << " "<< fcount*1.0/count*1.0 << " " << ommits << " "<< ommits2 <<  endl;
    fclose(f1);
    return 0;
}


int read_diff_filt(int argc, char * argv[] ){
    int ommits = 0;
    int ommits2 = 0;
    int boolminmax = atoi(argv[4]); 
    int boolminmax_diff =  atoi(argv[5]);
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    string value = argv[3];
    vector<string> parquetvalues1;

    int fcount = 0;
    int offset = -1;
    int global_len = 0;
    /*read marker of file*/
    char marker[5];
    result =  fread( marker, 4, 1, f1);
    
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;

    int* col = new int[fileheader1.numofvals];

    long data[fileheader1.numofblocks];
    result =  fread(data,fileheader1.numofblocks*8,1,f1);

	unsigned long initstep1 = 4 + sizeof(struct fileH)+fileheader1.numofblocks*8;
	struct D header1;
	
	
	int count = 0;
	int join1 = atoi(argv[2]);
	
	
 	while (totalcount1 < fileheader1.numofvals){
 	   
   		    int current, next;
   		    int blockstart = initstep1;
   		    fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
   		    result = fread(&current,sizeof(int),1,f1);
   		    fseek(f1, initstep1 + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
   		    result =  fread(&next,sizeof(int),1,f1);
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep1, SEEK_SET);
   		    result =  fread(&header1, sizeof(struct D),1,f1);
   		    totalcount1 += header1.numofvals;
   		    
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    		    //fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			msgpack::unpacked result1;
    			
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			count += values1.size();
    			for(string i : values1) 
                    if (i == value)
                      fcount++;
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				//totalcount1 += header1.numofvals;
   				initstep1 += next-current;
    		}
    		else {
    		int check = 0;
    		if (header1.diff == 1){  global_len = 0;}
    		if(offset  == -1 or header1.diff == 1){
    		 int temp = global_len;
    		 global_len += header1.lendiff;
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 vector<string> values1;
   			 if (header1.lendiff > 0){
   			   if (header1.diff == 0){
    	     if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			   char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1,  buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    }
    		    //cout << values1.front() << " la "<<values1.back() << " la "<<minmax1[0] << " la -" <<minmax1[1]<<endl;
    		    
    		    if (boolminmax_diff)
    			if (value.compare(values1.front())<0 or value.compare(values1.back())>0){
   		      		count += header1.numofvals;
   		      		//cout << "kaka" << endl;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		ommits++;
   		      		offset = -1;
   		      	continue;
   		    	} 
   		    	}
   		    else {
   		        check = 1;
   		    	if (boolminmax and (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0)){
   		    	    ommits++;
   		      		count += header1.numofvals;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		offset = -1;
   		      	continue;
   		    	}
   		    	else {
   		    	if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   		    	char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    }
   		    	}
   		    	}
   		    }

    		 for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k + temp;
   			     break;
   			     }
   			     offset = -1;
   			 }
   		    
             if (offset == -1){
                //if (header1.lendiff == 0)
                    ommits2++;
                count += header1.numofvals;
                initstep1 += next-current;
                continue;
             }
   			}
   			if (!check){
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;

    		 minmax.get().convert(minmax1);
    		 if (boolminmax)
    		 if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
    		     ommits++;
    		     count += header1.numofvals;
   		      	 initstep1 += next-current;
    		     continue;
    		 }
   			}
   			if (header1.indicessize == 4){
   			    
                int offf;
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (offf == offset)
                for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	fcount++;
     		    	count++;
     		    }
     		    else {count += header1.numofvals;}
     			initstep1 += next-current;
            
        }
        else{
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
     			
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next-current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
				for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next-current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
     			for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next-current ;
     		}
     		}
		}
	}

    cout <<  fcount << " "<< fcount*1.0/count*1.0 << " " << ommits << " "<< ommits2 <<  endl;
    fclose(f1);
    return 0;
}

int read_diff_range(int argc, char * argv[] ){
    int boolminmax = atoi(argv[5]); 
    int boolminmax_diff = atoi(argv[6]);
    int totalcount1=0;   
    vector <int> predicate;
    vector <int> predminmax;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    string value = argv[3];
    string op = argv[4];
    vector<string> parquetvalues1;

    int fcount = 0;
    float offset = -1, offset2 = -1;
    int global_len = 0;
    /*read marker of file*/
    char marker[5];
    result =  fread( marker, 4, 1, f1);
    
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH) + fileheader1.numofblocks*8;
	struct D header1;
	
	int count = 0;
	int join1 = atoi(argv[2]);
	
 	while (totalcount1 < fileheader1.numofvals){
 	    
    	/*read block header*/
   		    int current, next;
   		    fseek(f1, initstep1 + sizeof(int)*join1, SEEK_SET);
   		    result =  fread(&current,sizeof(int),1,f1);
   		    fseek(f1, initstep1 + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
   		    result =  fread(&next,sizeof(int),1,f1);
            initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
            fseek(f1,initstep1, SEEK_SET);
   		    result =  fread(&header1, sizeof(struct D),1,f1);
   		    totalcount1 += header1.numofvals;
   		    
   		 
   		    if (header1.dictsize == 0){
   		        if (totalcount1 == 0)
   		            parquetvalues1.reserve(fileheader1.numofvals);
    		    char buffer1[header1.indicessize];
    		    //fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			for (string i : values1){
    			  
    			  if ((op == ">" and value.compare(i)<0) or (op == "<" and value.compare(i)>0)){
    			    col[fcount] = count;
     		        fcount++;
     		      }
     		      count++;
    			}
   				initstep1 += next-current;
    		}
    		else {
    		if (header1.diff == 1){
    		 global_len = 0;
    		 predicate.clear();
    		 predminmax.clear();
    		}
       	    if(1 == 1){
    		 int temp = global_len;
    		 global_len += header1.lendiff;
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 vector<string> values1;
   			
   			 if (header1.lendiff > 0){
   			   if (header1.diff == 0){
   			   
   			if (SNAPPY){
   			  char buffer1[header1.dictsize];
    		  result =  fread(buffer1,header1.dictsize,1,f1);
    		  string output;
    		  snappy::Uncompress(buffer1, header1.dictsize, &output);
    		  msgpack::unpacked result1;
    		  unpack(result1, output.data(), output.size());
    		  result1.get().convert(values1);
    		}
    		else {
   			   
   			   char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    }
   		    	}
   		    else {
   		        
   		    	if ( boolminmax and ((op == ">" and value.compare(minmax1[1])>0) or (value.compare(minmax1[0])<0 and op == "<"))){
   		      		count += header1.numofvals;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next - current;
   		      		//cout << "lala1" << endl;
   		      	continue;
   		    	}
   		    	else {
   		    	 if (SNAPPY){
   			       char buffer1[header1.dictsize];
    		       result =  fread(buffer1,header1.dictsize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.dictsize, &output);
    		       msgpack::unpacked result1;
    		       unpack(result1, output.data(), output.size());
    		       result1.get().convert(values1);
    		}
    		else {
   		    	char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
   		    	}
   		    	}
   		    }
   		    }
   		    
   		  /* if (values1.size()<10){
   		   cout <<  global_len << endl;
   		   for (int k = 0; k < values1.size(); k++){
   		   cout << values1[k] << endl;
   		   }
   		   }*/
   		    
    		   
    		   
    	int pred=-2;
    	int preminmax=-1;  

    	if (header1.lendiff>0){
    	for (int k = 0; k < values1.size(); k++){   
    	     if (k>0 and value > values1[k-1] and value < values1[k]){
   			     offset = k-0.5;
   			     
   			     if (op == ">"){
   			         preminmax = global_len-1;
   			         pred = k-1+temp;
   			     }
   			     if (op == "<"){
   			         preminmax = global_len-values1.size();
   			         pred = k+temp;
   			         
   			         }
   
   			 }
    		 else if ( value == values1[k]){
   			     offset = k;
   			     pred = k+temp;
   			     if (op == ">")
   			         preminmax = values1.size()+temp-1;
   			     if (op == "<")
   			         preminmax = temp;
   			     //cout << offset << endl;
   			 }
   			
   			
   			 else if (op == ">" and value < values1[0] ){
   			     offset = -0.5;
   			     pred = global_len-values1.size()-1;
   			     preminmax = global_len-1;
   			 }
   			 
   			 else if ( op == "<" and value > values1[values1.size()-1]){
   			     offset = values1.size()-0.5;
   			     pred = global_len;
   			     preminmax = global_len-values1.size();
   			 }
   			 else if ( values1.size()==1){
   			       if (op == "<" and value > values1[0]){
   			           pred = global_len;
   			           preminmax = global_len-1;
   			       }
   			        if (op == ">" and (value < values1[0])){
   			           pred = global_len-1;
   			           preminmax = global_len-1;
   			       }   
   			   }
   			 
   			 if (pred !=-2){
   			     if (op == ">"){
   			       if (predminmax.size()>0 and predminmax[predminmax.size()-1] == floor(pred)){
   			         predminmax[predminmax.size()-1] = preminmax;
   			         break;
   			       
   			       }
   			       else {
   			         predicate.push_back(pred);
   			         predminmax.push_back(preminmax);
   			         break;
   			         
   			         }    
   			     }
   			     else if (op == "<"){
   			     if(predicate.size()>0 and predicate[predicate.size()-1] == preminmax){
   			         predicate[predicate.size()-1]  = pred;
   			         break;
   			     }
   			     else {
   			         predicate.push_back(pred);
   			         predminmax.push_back(preminmax);
   			         break;
   			     }
   			     
   			     }
   			 
   			 }
   		     
   			    offset = -1;
   			   
   			 }
   			 
   			 }
   		     
   		     
             if (offset == -1){
               
                //todo find exactly the next/previous corresponding offset and set to offset
                
                if (header1.diff == 1){
                    count += header1.numofvals;
                    initstep1 += next - current;
                    continue;
                    
                }
             }
   			}
   			
        if (header1.diff == 1){
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 if (boolminmax and ((op == ">" and value.compare(minmax1[1])>0) or (value.compare(minmax1[0])<0 and op == "<"))){
    		     count += header1.numofvals;
   		      	 initstep1 += next - current;

    		     continue;
    		 }
   			
   			 if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (op == ">" and offf > offset)
				for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else if (op == "<" and offf < offset)
     		   for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else count += header1.numofvals;
     		   
     			initstep1 += next - current;
            
        }
        else{
       
       
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next - current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next - current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
     			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next ;
     		}
     		}
     		}
     		
     	
     	else if (header1.diff == 0){
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D) + header1.previndices*2, SEEK_SET);
    		 
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    		 msgpack::unpacked minmax;
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;
    		 minmax.get().convert(minmax1);
    		 
    		 if (boolminmax and ((op == ">" and value.compare(minmax1[1])>0) or (value.compare(minmax1[0])<0 and op == "<"))){
    		     //cout << "lalaki" << endl;
    		     count += header1.numofvals;
   		      	 initstep1 += next - current;
    		     continue;
    		 }
   			
   			
   			 if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (op == ">")
				    for (int i=0; i < header1.numofvals; i++){
                        for (int j=0; j < predicate.size(); j++){
                            //if (offsets1[i] > predminmax[j]+1) continue;
        					if (unsigned(offf - predicate[j]) <  predminmax[j] - predicate[j]){
            					col[fcount] = count;
     		                    fcount++;
     		                    break;
     		                } if (offf < predminmax[j]+1) break;
     		                
     		           }
     		           count++;
     		   }
            
    				if (op == "<")
    				  for (int i=0; i < header1.numofvals; i++){
         				for (int j=0; j < predicate.size(); j++){
         				   // if (offsets1[i] > predicate[j]) continue;
        					if (unsigned(offf - predminmax[j]) <  predicate[j] - predminmax[j]){
        					    col[fcount] = count;
     		                    fcount++;
     		                    break;
        					} if (offf < predicate[j] ) break;
        				}
        				count++;
        					}
     			initstep1 += next - current;
            }
            else{
   			
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned short)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			if (op == ">")
				    for (int i=0; i < header1.numofvals; i++){
                        for (int j=0; j < predicate.size(); j++){
                            //if (offsets1[i] > predminmax[j]+1) continue;
        					if (unsigned(offsets1[i]-1 - predicate[j]) <  predminmax[j] - predicate[j]){
            					col[fcount] = count;
     		                    fcount++;
     		                    break;
     		                } if (offsets1[i] < predminmax[j]+1) break;
     		                
     		           }
     		           count++;
     		   }
            
    				if (op == "<")
    				  for (int i=0; i < header1.numofvals; i++){
         				for (int j=0; j < predicate.size(); j++){
         				   // if (offsets1[i] > predicate[j]) continue;
        					if (unsigned(offsets1[i] - predminmax[j]) <  predicate[j] - predminmax[j]){
        					    col[fcount] = count;
     		                    fcount++;
     		                    break;
        					} if (offsets1[i] < predicate[j] ) break;
        				}
        				count++;
        					}
            					
    
     		    
     		
     			initstep1 += next - current;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned int)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
				if (op == ">")
				    for (int i=0; i < header1.numofvals; i++){
                        for (int j=0; j < predicate.size(); j++){
                            //if (offsets1[i] > predminmax[j]+1) continue;
        					//if (offsets1[i] > predicate[j] and offsets1[i] < predminmax[j]+1){
        					if (unsigned(offsets1[i]-1 - predicate[j]) <  predminmax[j] - predicate[j]){
            					col[fcount] = count;
     		                    fcount++;
     		                    break;
     		                } if (offsets1[i] < predminmax[j]+1) break;
     		                
     		           }
     		           count++;
     		   }
            
    				if (op == "<")
    				  for (int i=0; i < header1.numofvals; i++){
         				for (int j=0; j < predicate.size(); j++){
         				   // if (offsets1[i] > predicate[j]) continue;
        					//if ( offsets1[i] < predicate[j] and offsets1[i] >= predminmax[j]){
        					if (unsigned(offsets1[i] - predminmax[j]) <  predicate[j] - predminmax[j]){
        					    col[fcount] = count;
     		                    fcount++;
     		                    break;
        					} if (offsets1[i] < predicate[j] ) break;
        				}
        				count++;
        					}
            					
    
     			initstep1 += next - current ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
     			if (SNAPPY){
   			       char buffer1[header1.indicessize];
   			       result =  fread(buffer1,header1.indicessize,1,f1);
    		       string output;
    		       snappy::Uncompress(buffer1, header1.indicessize, &output);
    		       
    		       copy(&output[0], &output[0]+(sizeof(unsigned char)*header1.numofvals), reinterpret_cast<char*>(offsets1));
    		    }
    		    else 
    			result =  fread(offsets1,header1.indicessize,1,f1);
     			if (op == ">")
				    for (int i=0; i < header1.numofvals; i++){
                        for (int j=0; j < predicate.size(); j++){
                            //if (offsets1[i] > predminmax[j]+1) continue;
        					if (unsigned(offsets1[i]-1 - predicate[j]) <  predminmax[j] - predicate[j]){
            					col[fcount] = count;
     		                    fcount++;
     		                    break;
     		                } if (offsets1[i] < predminmax[j]+1) break;
     		                
     		           }
     		           count++;
     		   }
            
    				if (op == "<")
    				  for (int i=0; i < header1.numofvals; i++){
         				for (int j=0; j < predicate.size(); j++){
         				   // if (offsets1[i] > predicate[j]) continue;
        					if (unsigned(offsets1[i] - predminmax[j]) <  predicate[j] - predminmax[j]){
        					    col[fcount] = count;
     		                    fcount++;
     		                    break;
        					} if (offsets1[i] < predicate[j] ) break;
        				}
        				count++;
        					}
            					
    
     			initstep1 += next - current ;
     		}
     		}
     		
     	}
		}
	}

    cout << fcount << "  " << count << " " << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 0;
}



int main(int argc, char * argv[] ){
  //std::cout.flush();
  
  if (strstr(argv[1], "snappy"))
      SNAPPY = 1;
  if (argc == 3){
      if (strstr(argv[1], "diff"))
         return read_diff_materialize(argc,argv);
      
      }
  if (argc == 4){
      if (strstr(argv[1], "diff"))
         return random_access_diff(argc,argv);
  }
  
  if (argc == 6){
      
      if (strstr(argv[1], "diff"))
         return read_diff_filt(argc,argv);
     
      }
  
  if (argc == 7){
      
      if (strstr(argv[1], "diff")){
         char lala[7] = "Talwar";
         return equi_filter(argc, argv[1],argv[2],argv[3],argv[4],argv[5],argv[6]);
         return equi_filter(argc, argv[1],argv[2],lala,argv[4],argv[5],argv[6]);
         }
     
      }
 if (argc == 8){
      if (strstr(argv[1], "diff"))
         return read_diff_range(argc,argv);
      
        
      }
      
      
}
