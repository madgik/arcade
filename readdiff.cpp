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

//////
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>

#include "reader_writer.h"


#include "orc/ColumnPrinter.hh"

#include "orc/Exceptions.hh"

#include <getopt.h>
#include <string>
#include <memory>
#include <iostream>
#include <string>

#include <gtest/gtest.h>

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
#include <parquet/column_reader.h>
#include <parquet/column_writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/platform.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/test_util.h>
#include <parquet/types.h>




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

struct Dglob {
    int minmaxsize;
    int indicessize;
    int bytes;
    int numofvals;
};

struct Dindirect {
    int minmaxsize;
    int indicessize;
    int bytes;
    int numofvals;
    int mapsize;
    int mapbytes;
    int mapcount;
};

struct fileH {
     int numofvals;
     int numofcols;
     int numofblocks;
};

struct fileHglob {
     int numofvals;
     int numofcols;
     int glob_size;
};

struct fileH_mostlyorderedglob {
     int numofvals;
     int numofcols;
     int global_position;
     int glob_size;
     int sortedsize;
     int totalsize;
};

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



int read_mostly_orderedglob(int argc, char * argv[] ){
    
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    
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
    
    struct fileH_mostlyorderedglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileH_mostlyorderedglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << " "<< fileheader1.global_position << " "<< fileheader1.glob_size <<" "<< fileheader1.sortedsize<< " "<< fileheader1.totalsize << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH_mostlyorderedglob);
	struct Dglob header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[fileheader1.global_position],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
            if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
            else{
        
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		}
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    
    cout << count << endl;
    fclose(f1);
    return 1;
}

int read_mostly_orderedglob_filt(int argc, char * argv[] ){

	int boolminmax = atoi(argv[4]); 
	int ommits = 0;
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    fseek(f1, 0, SEEK_SET);  
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
    
	string value = argv[3];
	
	int count = 0;
	int fcount = 0;

    
    /*read marker of file*/
    
    struct fileH_mostlyorderedglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH_mostlyorderedglob), 1, f1);
    vector <vector <int>> index1; 
    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH_mostlyorderedglob);
	struct Dglob header1;
	
    int* col = new int[fileheader1.numofvals];
	int join1 = atoi(argv[2]);
	
	fseek(f1,fileheader1.global_position,SEEK_SET);
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    int offset = -1;
    for (int k = 0; k < values1.size(); k++){
   			   if (value != "" and value == values1[k]){
   			     offset = k;
   			     break;
   			     }
   			     offset = -1;
   			 }
     		 
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
   	 
    
    fseek(f1,initstep1,SEEK_SET);
    
    
    while (totalcount1 < fileheader1.numofvals){
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
            ommits++;
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            continue;
            }
        
        if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (offf == offset)
                for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	fcount++;
     		    	count++;
     		    }
     		    else count += header1.numofvals;
     			initstep1 += next;
            
        }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next ;
     		}
     		}
       
    
    }
    
    
    cout << fcount*1.0/count*1.0 << " "<< ommits << endl;
    fclose(f1);
    return 1;

}

int read_mostly_orderedglob_range(int argc, char * argv[] ){
    int boolminmax = atoi(argv[5]); 
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    fseek(f1, 0, SEEK_SET);  
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
    
	string value = argv[3];
	
	int count = 0;
	int fcount = 0;
    string op = argv[4];
    
    /*read marker of file*/
    
    struct fileH_mostlyorderedglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH_mostlyorderedglob), 1, f1);
    vector <vector <int>> index1; 
    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileH_mostlyorderedglob);
	struct Dglob header1;
	
    int* col = new int[fileheader1.numofvals];
	int join1 = atoi(argv[2]);
	
	fseek(f1,fileheader1.global_position,SEEK_SET);
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    float offset = -1;
    int unsorted = -1;
    cout << fileheader1.numofvals << " " << fileheader1.numofcols << " "<< fileheader1.global_position << " "<< fileheader1.glob_size <<" "<< fileheader1.sortedsize<< " "<< fileheader1.totalsize << endl;
    
    int counter = 0;
    for (int k = 0; k < fileheader1.sortedsize; k++){
          if (values1[k]!=""){
             counter += 1;
             if (value <= values1[k]){
               if (value == values1[k]){
                 offset = k;
                 }
               else{
               if (counter == 1)
                 offset = -2;
               else
   			     offset = float(k-0.5);
   			     }
   			   break;
   			 }
   			 }
   			
   	}
   	if(offset == -1) offset = fileheader1.sortedsize+0.5;
   	
   	
   	
   	for (int k = fileheader1.sortedsize; k < fileheader1.totalsize; k++){
            if (op == ">" and value < values1[k] ){
   			     unsorted = 0;
   			     break;
   			 }
   			 
   			 else if ( op == "<" and value > values1[k]){
   			     unsorted = 0;
   			     break;
   			 }
   	}
   	

   	if (offset == -1 and unsorted == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
    fseek(f1,initstep1,SEEK_SET);
    
    while (totalcount1 < fileheader1.numofvals){
        
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
        
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if ((op == ">" and value.compare(minmax1[1])>0) or (op=="<" and value.compare(minmax1[0])<0)){
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            continue;
            }
       
       if (header1.indicessize == 4){
                
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
    			

    			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
				  
     		      if (offf < fileheader1.sortedsize and offf > offset){
     		        
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offf >= fileheader1.sortedsize and values1[offf]>value){
     		        col[fcount] = count;
     		        fcount++;
     		     }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offf < fileheader1.sortedsize and offf < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offf >= fileheader1.sortedsize and values1[offf]<value){
     		        col[fcount] = count;
     		        fcount++;
     		     
     		     }
     		    count++;
     		}
     			initstep1 += next ;
            
       }
       else{
       
       if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
                
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] < fileheader1.sortedsize and offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]>value){
     		        col[fcount] = count;
     		        fcount++;
     		     }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < fileheader1.sortedsize and offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]<value){
     		        col[fcount] = count;
     		        fcount++;
     		     
     		     }
     		    count++;
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob) + header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] < fileheader1.sortedsize and offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]>value){
     		        col[fcount] = count;
     		        fcount++;
     		     }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < fileheader1.sortedsize and offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]<value){
     		        col[fcount] = count;
     		        fcount++;
     		     
     		     }
     		    count++;
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     		    
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
     			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (offsets1[i] < fileheader1.sortedsize and offsets1[i] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]>value){
     		        col[fcount] = count;
     		        fcount++;
     		     }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (offsets1[i] < fileheader1.sortedsize and offsets1[i] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		     else if (offsets1[i] >= fileheader1.sortedsize and values1[offsets1[i]]<value){
     		        col[fcount] = count;
     		        fcount++;
     		     
     		     }
     		    count++;
     		}
     			initstep1 += next ;
     			
		}
		
		}
    
    }
    /*FILE *f2 = fopen("res.txt","wb");
    for (int i=0;i<fcount;i++)
      fprintf(f2,"%d\n",col[i]);*/
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 1;
}

int read_glob_mater(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
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
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << count << endl;
    fclose(f1);
    return 1;
}


int read_glob(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
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
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dglob));
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dglob)+header1.minmaxsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << count << endl;
    fclose(f1);
    return 1;
}

int read_indirect_mater(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
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
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
                    if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
             else{
                if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
				    if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==0){
     		    unsigned int map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offf]];
     		    	else
     		    	    column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	if (header1.mapsize > 0)
     		    	    column[count] = values1[map[offsets1[i]]];
     		    	else
     		    	    column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
     			
     			}

        /* end of read mapping */
        
        
        
         
        

        totalcount1 += header1.numofvals;
    
    }
    
    cout << count << " "<< column[10000000] << endl;
    fclose(f1);
    return 1;
}


int read_indirect(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
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
    
    struct fileHglob fileheader1;
    memcpy(&fileheader1,&fptr1[4], sizeof(struct fileHglob));
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;

	int join1 = atoi(argv[2]);
	
	char buffer1[fileheader1.glob_size];
    memcpy(buffer1,&fptr1[initstep1],fileheader1.glob_size);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        unsigned int map [header1.mapcount];
        if (header1.mapbytes==1)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     		if (header1.mapbytes==0)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     		if (header1.mapbytes==2)
     			memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);

        /* end of read mapping */
        
        
        
         if (header1.indicessize == 4){
                int offf;
                memcpy(&offf, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
                for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offf];
     		    	count++;
     		    }
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	//column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     	}
        

        totalcount1 += header1.numofvals;
    
    }
    
    cout << count << endl;
    fclose(f1);
    return 1;
}

int read_indirect_filt(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string value = argv[3];
    
    /*read marker of file*/
    char marker[5];
    result = fread( marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result = fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	int* col = new int[fileheader1.numofvals];
	char buffer1[fileheader1.glob_size];
	result = fread(buffer1,fileheader1.glob_size, 1, f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    
    int offset = -1;
    for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k;
   			     break;
   			     }
   			     offset = -1;
   			 }
   		 
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        result = fread(&header1, sizeof(struct Dindirect), 1, f1);
        //memcpy(&header1,&fptr1[initstep1], sizeof(struct Dindirect));
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
                if (header1.mapsize > 0){
                    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                
                if (header1.mapsize > 0){
                    if (map[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     		}
     		if (header1.mapbytes==0){
     		   unsigned int map [header1.mapcount];
     		   if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
     		    if (header1.mapsize > 0){
                    if (map[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
     		    if (header1.mapsize > 0){
                    if (map[offf] == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
                }
     		    else {
     		        if (offf == offset)
                      for (int i=0; i < header1.numofvals; i++){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    count++;
     		        }
     		        else {count += header1.numofvals;}
     		    }
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize>0) {
				  for (int i=0; i < header1.numofvals; i++){
     		    	if (map[offsets1[i]] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     		    else{
     		      for (int i=0; i < header1.numofvals; i++){
     		    	if (offsets1[i] == offset){
     		    	    col[fcount] = count;
     		    	    fcount++;
     		    	    
     		    	 }
     		    	count++;
     		      }
     		    }
     			initstep1 += next ;
     		}
     	}
     			}

        /* end of read mapping */
        
        
        
         
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << fcount << endl;
    fclose(f1);
    return 1;
}

// unordered indirect
int read_indirect_range(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string value = argv[3];
    string op = argv[4];
    
    
    /*read marker of file*/
    char marker[5];
    result = fread( marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result = fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	int* col = new int[fileheader1.numofvals];
	char buffer1[fileheader1.glob_size];
    result = fread(buffer1,fileheader1.glob_size, 1, f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
   	 
    
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        result = fread(&header1, sizeof(struct Dindirect), 1, f1);
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
                if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
                if (op == ">" and values1[offf] > value)
				for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else if (op == "<" and values1[offf] < value)
     		   for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else count += header1.numofvals;
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     		}
     		if (header1.mapbytes==0){
     		   unsigned int map [header1.mapcount];
               if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
                if (op == ">" and values1[offf] > value)
				for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else if (op == "<" and values1[offf] < value)
     		   for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else count += header1.numofvals;
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     		}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
                if (op == ">" and values1[offf] > value)
				for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else if (op == "<" and values1[offf] < value)
     		   for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else count += header1.numofvals;
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[map[offsets1[i]]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[map[offsets1[i]]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
     		if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     		}
        
        
        
         
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 1;
}


int read_indirect_range_ord(int argc, char * argv[] ){
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string value = argv[3];
    string op = argv[4];
    char marker[5];
    result = fread( marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result = fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    //cout << fileheader1.numofvals << " " << fileheader1.numofcols << endl;
    
    //string* column = new string[fileheader1.numofvals];
	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dindirect header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	int* col = new int[fileheader1.numofvals];
	char buffer1[fileheader1.glob_size];
    result = fread(buffer1,fileheader1.glob_size, 1, f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    
    
    float offset = -1;
    
    for (int k = 0; k < values1.size(); k++){
             if (k>0 and value > values1[k-1] and value < values1[k]){
   			     offset = k-0.5;
   			     break;
   			 }
    		 else if ( value == values1[k]){
   			     offset = k;
   			     break;
   			 }
   			 else if (op == ">" and value < values1[0] ){
   			     offset = -0.5;
   			     break;
   			 }
   			 
   			 else if ( op == "<" and value > values1[values1.size()-1]){
   			     offset = values1.size()-0.5;
   			     break;
   			 }
   	}
   			
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
   	 
    
    initstep1 += fileheader1.glob_size;
    
    while (totalcount1 < fileheader1.numofvals){
        result = fread(&header1, sizeof(struct Dindirect), 1, f1);
        //cout << header1.indicessize <<" "<< header1.numofvals << " "<< header1.minmaxsize << " "<< header1.bytes <<" "<< header1.mapsize << " "<< header1.mapbytes <<endl;
        int next = sizeof(struct Dindirect) + header1.minmaxsize + header1.indicessize + header1.mapsize;
        
        /* read mapping - possible bug here, with casting to unsigned int */
        if (header1.mapbytes==1){
                unsigned short int map [header1.mapcount];
     			if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
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
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     		}
     		if (header1.mapbytes==0){
     		   unsigned int map [header1.mapcount];
               if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
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
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     			}
     		if (header1.mapbytes==2){
     		    unsigned char map [header1.mapcount];
     			if (header1.mapsize > 0){
     			    fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize,SEEK_SET);
                    result = fread(map,  header1.mapsize, 1, f1);
     			    //memcpy(map, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize], header1.mapsize);
     			   }
     			if (header1.indicessize == 4){
                int offf;
                result = fread(&offf,  header1.indicessize, 1, f1);
                if (header1.mapsize > 0)
                    offf = map[offf];
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
     		   
     		    
     			initstep1 += next;
            
            }
        else{
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
				if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1, initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize,SEEK_SET);
                result = fread(offsets1,  header1.indicessize, 1, f1);
     			//memcpy(offsets1, &fptr1[initstep1+sizeof(struct Dindirect)+header1.minmaxsize+ header1.mapsize], header1.indicessize);
     			if (header1.mapsize > 0){
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (map[offsets1[i]] > offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (map[offsets1[i]] < offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     		}
     		else{
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
     		
     		
     		
     		}
     			initstep1 += next ;
     		}
     	}
     			}
        
        
        
         
        

        totalcount1 += header1.numofvals;
    
    }
    
    
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 1;
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
    fclose(f1);
    
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
    		
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
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

    cout << count << column[10000000]<< endl;
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
    		
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)+ header1.previndices*2 + header1.minmaxsize],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
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

int read_glob_filt(int argc, char * argv[] ){
    int boolminmax = atoi(argv[4]);
    int ommits = 0; 
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    
    /*load file 1 in memory*/
  
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
   
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];
    
    

	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	string value = argv[3];
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    int offset = -1;
    for (int k = 0; k < values1.size(); k++){
   			   if (value == values1[k]){
   			     offset = k;
   			     break;
   			     }
   			     offset = -1;
   			 }
   		 
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
    
    while (totalcount1 < fileheader1.numofvals){
        
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
        
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            ommits++;
            continue;
            }
        
        if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (offf == offset)
                for (int i=0; i < header1.numofvals; i++){
     		    	col[fcount] = count;
     		    	fcount++;
     		    	count++;
     		    }
     		    else count += header1.numofvals;
     			initstep1 += next;
            
        }
        else{
       
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next;
     		}
     		else if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next ;
     		}
     		else if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next ;
     		}

       }
    
    }
    
   
    cout << fcount*1.0/count*1.0 << " " << ommits << endl;
    fclose(f1);
    return 1;
}

int random_access_glob(int argc, char * argv[] ){
    return 1;
}

int random_access_mostly_orderedglob(int argc, char * argv[] ){
    return 1;
}
int random_access_dict(int argc, char * argv[] ){
    return 1;
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
        result =  fread(data,fileheader1.numofblocks*8,1,f1);
        int blocknum = row/data[fileheader1.numofblocks -1];
        int rowid = row%data[fileheader1.numofblocks -1];

        unsigned long initstep1 = data[blocknum];
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
    /*cout << header1.previndices << endl;
    for (int i = 0; i<header1.previndices; i++)
        cout << previndex[i] << endl;*/
        
        if (header1.dictsize == 0){
                    char buffer1[header1.indicessize];
                    fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
                    result =  fread(buffer1,header1.indicessize,1,f1);
                    msgpack::unpacked result1;
                    unpack(result1, buffer1, header1.indicessize);
                    vector<string> values1;
                    result1.get().convert(values1);
                    cout << values1[rowid] << endl;
                    return 1;
        }
        else if (header1.diff == 0){
            int off;
              if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
                  unsigned short offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize+rowid*2,SEEK_SET);
                  result =  fread(&offsets1,2,1,f1);
                  off = offsets1;
              }
              
              if (header1.bytes==0){ // four byte offsets
                  unsigned int offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid*4,SEEK_SET);
                  result =  fread(&offsets1,4,1,f1);
                  off = offsets1;
              }
              if (header1.bytes==2){ // one byte offsets
                  unsigned char offsets1;
                  fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid,SEEK_SET);
                  result =  fread(&offsets1,1,1,f1);
                  off = offsets1;
              }
                
        
    int temp = 0;
    int prevtemp = 0;
    int rightblock = 0;
    int found = 0;
    int position_in_block;

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
            char buffer1[header2.dictsize];
            fseek(f1,initstep2+sizeof(struct D)+header2.minmaxsize+ header2.previndices*2,SEEK_SET);
            result =  fread(buffer1,header2.dictsize,1,f1);
            msgpack::unpacked result1;
            unpack(result1, buffer1, header2.dictsize);
            vector<string> values1;
            result1.get().convert(values1);
            cout << values1[position_in_block] << endl;
        }
        else {
            char buffer1[header1.dictsize];
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            result =  fread(buffer1,header1.dictsize,1,f1);
            msgpack::unpacked result1;
            unpack(result1, buffer1, header1.dictsize);
            vector<string> values1;
            result1.get().convert(values1);
            cout << values1[position_in_block] << endl;
        }
   // cout <<  position_in_block << " "<< rightblock << endl;
        
        }
    
        else if (header1.diff == 1){
            int off;
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
                unsigned short offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize+rowid*2,SEEK_SET);
                result =  fread(&offsets1,2,1,f1);
                off = offsets1;
            }
            
            if (header1.bytes==0){ // four byte offsets
                unsigned int offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid*4,SEEK_SET);
                result =  fread(&offsets1,4,1,f1);
                off = offsets1;
            }
            if (header1.bytes==2){ // one byte offsets
                unsigned char offsets1;
                fseek(f1,initstep1+sizeof(struct D)+header1.dictsize +header1.previndices*2+ header1.minmaxsize+rowid,SEEK_SET);
                result =  fread(&offsets1,1,1,f1);
                off = offsets1;
            }
            char buffer1[header1.dictsize];
            fseek(f1,initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2,SEEK_SET);
            result =  fread(buffer1,header1.dictsize,1,f1);
            msgpack::unpacked result1;
            unpack(result1, buffer1, header1.dictsize);
            vector<string> values1;
            result1.get().convert(values1);
            cout << values1[off] << endl;
            
            
        }
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
    		    fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
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
   			   char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1,  buffer1, header1.dictsize);
    		    result1.get().convert(values1);
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
   		    	char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
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

   

int read_dict(int argc, char * argv[] ){
    
    int totalcount1=0;   
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    
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
    
    string* column = new string[fileheader1.numofvals];

    
	unsigned long initstep1 = 4 + sizeof(struct fileH);
	struct D header1;
	
	int count = 0;

    
	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
    	/*read block header*/
   		    int current, next;
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
   				initstep1 += next;
    		}
    		else {
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
   			totalcount1 += header1.numofvals;
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    	column[count] = values1[offsets1[i]];
     		    	count++;
     		    }
     			initstep1 += next ;
     		}
		}
	}

   
    cout << count << endl;
    fclose(f1);
    return 0; 


}


int read_glob_range_unordered(int argc, char * argv[] ){
    int boolminmax = atoi(argv[5]); 
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string op = argv[4];
    
    /*load file 1 in memory*/
  
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];

	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	string value = argv[3];
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
   
    while (totalcount1 < fileheader1.numofvals){
        
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
        
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if ((op == ">" and value.compare(minmax1[1])>0) or (op=="<" and value.compare(minmax1[0])<0)){
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            continue;
            }
        
        if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (op == ">" and values1[offf] > value)
				for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else if (op == "<" and values1[offf] < value)
     		   for (int i=0; i < header1.numofvals; i++){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        count++;
     		   }
     		   else count += header1.numofvals;
     		   
     			initstep1 += next;
            
        }
        else{
       
       if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
				if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob) + header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
    			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		}
     			initstep1 += next ;
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
    			result =  fread(offsets1,header1.indicessize,1,f1);
     			if (op == ">")
				for (int i=0; i < header1.numofvals; i++){
     		      if (values1[offsets1[i]] > value){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		        count++;
     		   }
     		   if (op == "<")
     		   for (int i=0; i < header1.numofvals; i++){
     		     if (values1[offsets1[i]] < value){
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
    
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 1;
}


int read_glob_range(int argc, char * argv[] ){
	int boolminmax = atoi(argv[5]); 
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    vector <string> global_dict;
    string op = argv[4];
    
    /*load file 1 in memory*/
  
    fseek(f1, 0, SEEK_SET);  
    
    /*read marker of file*/
    char marker[5];
    result =  fread(marker, 4, 1, f1);
    
    struct fileHglob fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileHglob), 1, f1);
    vector <vector <int>> index1;
    
    int* col = new int[fileheader1.numofvals];

	unsigned long initstep1 = 4 + sizeof(struct fileHglob);
	struct Dglob header1;
	
	int count = 0;
	int fcount = 0;

	int join1 = atoi(argv[2]);
	string value = argv[3];
	char buffer1[fileheader1.glob_size];
    result =  fread(buffer1,fileheader1.glob_size,1,f1);
    msgpack::unpacked result1;
    unpack(result1, buffer1,fileheader1.glob_size);
    vector<string> values1;
    result1.get().convert(values1);
    initstep1 += fileheader1.glob_size;
    
    float offset = -1;
    
    for (int k = 0; k < values1.size(); k++){
             if (k>0 and value > values1[k-1] and value < values1[k]){
   			     offset = k-0.5;
   			     break;
   			 }
    		 else if ( value == values1[k]){
   			     offset = k;
   			     break;
   			 }
   			 else if (op == ">" and value < values1[0] ){
   			     offset = -0.5;
   			     break;
   			 }
   			 
   			 else if ( op == "<" and value > values1[values1.size()-1]){
   			     offset = values1.size()-0.5;
   			     break;
   			 }
   	}
   			
   	if (offset == -1){
   	 cout << "0" << endl;
   	 return 0;
   	 }
    
   
    while (totalcount1 < fileheader1.numofvals){
        
        result =  fread(&header1, sizeof(struct Dglob),1,f1);
        totalcount1 += header1.numofvals;
        char minmaxbuf[header1.minmaxsize];
        result =  fread(minmaxbuf,header1.minmaxsize,1,f1);
        
    	msgpack::unpacked minmax;
    	unpack(minmax, minmaxbuf, header1.minmaxsize);
    	vector<string> minmax1;
    	minmax.get().convert(minmax1);
        
        //cout << minmax1[0] << " " << minmax1[1] << endl;
        
        int next = sizeof(struct Dglob) + header1.minmaxsize + header1.indicessize;
        if (boolminmax)
        if ((op == ">" and value.compare(minmax1[1])>0) or (op=="<" and value.compare(minmax1[0])<0)){
            initstep1 += next;
            fseek(f1,initstep1,SEEK_SET);
            count += header1.numofvals;
            continue;
            }
       
       if (header1.indicessize == 4){
                int offf;
                fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     		   
     			initstep1 += next;
            
        }
        else{
       
       
       if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
     			initstep1 += next;
     		}
     		if (header1.bytes==0){ // four byte offsets
     			unsigned int offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob) + header1.minmaxsize,SEEK_SET);
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
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char offsets1 [header1.numofvals];
     			fseek(f1,initstep1+sizeof(struct Dglob)+ header1.minmaxsize,SEEK_SET);
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
 /*FILE *f2 = fopen("res2.txt","wb");
    for (int i=0;i<fcount;i++)
      fprintf(f2,"%d\n",col[i]);
    //cout << fcount << endl;
    return 1;*/
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 1;
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
    		    fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    			result =  fread(buffer1,header1.indicessize,1,f1);
    			msgpack::unpacked result1;
    			unpack(result1, buffer1, header1.indicessize);
    			vector<string> values1;
    			result1.get().convert(values1);
    			parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
   				totalcount1 += header1.numofvals;
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
   			   char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
    		    
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
   		    	
   		    	char buffer1[header1.dictsize];
   			    result =  fread(buffer1,header1.dictsize,1,f1);
    		    msgpack::unpacked result1;
    		    unpack(result1, buffer1, header1.dictsize);
    		    result1.get().convert(values1);
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

    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 0;
}







int read_dict_filt(int argc, char * argv[] ){
    
    int totalcount1=0;
    int boolminmax = atoi(argv[4]); 
    FILE *f1;
    f1 = fopen(argv[1],"rb");
    string value = argv[3];
    vector<string> parquetvalues1;
    int fcount = 0;
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
    
    int* col = new int[fileheader1.numofvals];

    
	unsigned long initstep1 = 4 + sizeof(struct fileH);
	struct D header1;
	
	int count = 0;

    
	int join1 = atoi(argv[2]);
 	while (totalcount1 < fileheader1.numofvals){
 	       int offset = -1;
    	/*read block header*/
   		    int current, next;
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
   				initstep1 += next - current;
    		}
    		else {
   			char buffer1[header1.dictsize];
    		memcpy(buffer1,&fptr1[initstep1+sizeof(struct D)],header1.dictsize);
    		msgpack::unpacked result1;
    		unpack(result1, buffer1, header1.dictsize);
    		vector<string> values1;
    		result1.get().convert(values1);
   			totalcount1 += header1.numofvals;
   			
   			if (value.compare(values1.front())<0 or value.compare(values1.back())>0){
   		      initstep1 += next - current;
   		      continue;
   		    }
   		    
    		for (int k = 0; k < values1.size(); k++){
   			 if (value == values1[k]){
   			     offset = k;
   			     break;
   			     }
   			
   			}
   		     
            if (offset == -1){
                initstep1 += next - current;
                continue;
             }
             
   			
   			
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     			unsigned short offsets1 [header1.numofvals];
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    if (offsets1[i] == offset){
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
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
				for (int i=0; i < header1.numofvals; i++){
     		    if (offsets1[i] == offset){
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
     			memcpy(offsets1, &fptr1[initstep1+sizeof(struct D)+header1.dictsize], header1.indicessize);
     			for (int i=0; i < header1.numofvals; i++){
     		    if (offsets1[i] == offset){
     		        //col[fcount].rowid = count;
     		        //col[fcount].val = value;
     		        col[fcount] = count;
     		        fcount++;
     		        }
     		    count++;
     		   }
     			initstep1 += next - current ;
     		}
		}
	}

   
    cout << fcount*1.0/count*1.0 << endl;
    fclose(f1);
    return 0; 


}


int read_orc(int argc, char * argv[]){
    uint64_t batchSize = 65536;
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(argv[1]), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<orc::ColumnVectorBatch> batch =
    rowReader->createRowBatch(batchSize);

    unsigned long rows = 0;
    unsigned long batches = 0;
    while (rowReader->next(*batch)) {
      batches += 1;
      rows += batch->numElements;
    }
    cout << "Rows: " << rows << std::endl;
    cout << "Batches: " << batches << std::endl;    

}

int random_access_orc(int argc, char * argv[]){
    int row = atoi(argv[3])+1;
    uint64_t batchSize = 1;
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(argv[1]), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<orc::ColumnVectorBatch> batch =
    rowReader->createRowBatch(batchSize);

    unsigned long rows = 0;
    unsigned long batches = 0;
    std::string line;
    std::unique_ptr<orc::ColumnPrinter> printer =
      createColumnPrinter(line, &rowReader->getSelectedType());
    rowReader->seekToRow(row);
    
    
    rowReader->next(*batch);
      printer->reset(*batch);
      batches += 1;
      line.clear();
      printer->printRow(0);
      cout << line << endl;

    
}

using namespace orc;
int read_orc_filt(int argc, char * argv[]){
    uint64_t batchSize = 65536;
    char* value = argv[3];
    orc::ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(argv[1]), readerOpts);
    std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader();
    std::unique_ptr<orc::ColumnVectorBatch> batch =
    rowReader->createRowBatch(batchSize);

    
    unsigned long rows = 0;
    std::string line;
    
    StructVectorBatch *root =dynamic_cast<StructVectorBatch *>(batch.get());
    StringVectorBatch *x = dynamic_cast<StringVectorBatch *>(root->fields[0]);
    while (rowReader->next(*batch)) {
    for (int i=0; i < batch->numElements; i++){
      if (!memcmp(value, x->data[i], x->length[i]-1))
                      rows++;
    }

    }
    cout << rows << endl;
}


int read_parquet(int argc, char * argv[]){

  std::string filename;

  // Read command-line options
  int batch_size = 65535;
  std::vector<int> columns;
  int num_columns = 1;
  columns.push_back(atoi(argv[2]));
  char *param, *value;

  filename = argv[1];

  try {
   
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);

    int64_t total_rows = parquet::ScanFileContents(columns, batch_size, reader.get());

    std::cout << total_rows << " rows scanned" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

  return 0;
}

int random_access_parquet(int argc, char * argv[]){
    int randomrow = atoi(argv[3]);
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::OpenFile(argv[1], false);
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    int num_row_groups = file_metadata->num_row_groups();
    cout << num_row_groups << endl;
    int num_columns = file_metadata->num_columns();
    cout << num_columns << endl;
    int num_rows = file_metadata->num_rows();
    
    int div = ceil(num_rows/num_row_groups);
    int rowgroupfound = randomrow/div;
    int offset = randomrow%div;
    cout << num_rows << endl;
    int64_t rows_read = 0;
    int64_t values_read = 0;
    
    //unsigned char** column1 = (unsigned char**) malloc(num_rows * sizeof(unsigned char*));
    int row_id = 0;
    std::vector<int> col_row_counts(num_columns, 0);
    int col_id = atoi(argv[2]);
    int rows = 0;
    
    
          std::shared_ptr<parquet::RowGroupReader> row_group_reader =
          parquet_reader->RowGroup(rowgroupfound);
          auto metadata = row_group_reader->metadata();
          int rowgroupnum = metadata->num_rows();
          std::shared_ptr<parquet::ColumnReader> column_reader;
          //for (int col_id = 0; col_id < num_columns; col_id++){
          parquet::ByteArray* value = (parquet::ByteArray* )malloc(rowgroupnum*sizeof(parquet::ByteArray));
          column_reader = row_group_reader->Column(col_id);
      	  parquet::ByteArrayReader* ba_reader =
          static_cast<parquet::ByteArrayReader*>(column_reader.get());
          int rowid = 0;
          
          while (ba_reader->HasNext()) {
              
              rows_read = ba_reader->ReadBatch(rowgroupnum, nullptr, nullptr, value, &values_read);
              int oldrowid = rowid;
              rowid += values_read;
              if (offset < rowid){
                cout << value[offset - oldrowid].ptr << endl;
                break;
                }
                  
                
          }
          free(value);
          
          

    
    cout << rows << endl;
    //cout << column1[0] << endl;
    return 1;
}

int read_parquet_filt(int argc, char * argv[]){
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::OpenFile(argv[1], false);
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    int num_row_groups = file_metadata->num_row_groups();
    cout << num_row_groups << endl;
    int num_columns = file_metadata->num_columns();
    cout << num_columns << endl;
    int num_rows = file_metadata->num_rows();
    cout << num_rows << endl;
    int64_t rows_read = 0;
    int64_t values_read = 0;
    
    //unsigned char** column1 = (unsigned char**) malloc(num_rows * sizeof(unsigned char*));
    int row_id = 0;
    std::vector<int> col_row_counts(num_columns, 0);
    int col_id = atoi(argv[2]);
    char * expectedvalue = argv[3];
    int length = strlen(expectedvalue);
    int rows = 0;
    
    for (int r = 0; r < num_row_groups; ++r) {
          std::shared_ptr<parquet::RowGroupReader> row_group_reader =
          parquet_reader->RowGroup(r);
          
          /*get minmax statistics
          auto metadata = row_group_reader->metadata();
          auto column_schema = metadata->schema()->Column(col_id);
          auto column_chunk = metadata->ColumnChunk(0);
          auto stats = column_chunk->statistics();
          
          //cout << stats->EncodeMax() << " "<< stats->EncodeMin() << endl;
          /*end of statistics*/
          auto metadata = row_group_reader->metadata();
          int rowgroupnum = metadata->num_rows();
          std::shared_ptr<parquet::ColumnReader> column_reader;
          //for (int col_id = 0; col_id < num_columns; col_id++){
          parquet::ByteArray* value = (parquet::ByteArray* )malloc(rowgroupnum*sizeof(parquet::ByteArray));
          column_reader = row_group_reader->Column(col_id);
      	  parquet::ByteArrayReader* ba_reader =
          static_cast<parquet::ByteArrayReader*>(column_reader.get());
          while (ba_reader->HasNext()) {
              rows_read = ba_reader->ReadBatch(rowgroupnum, nullptr, nullptr, value, &values_read);
              for (int i=0; i<values_read;i++)
                  if (!memcmp(expectedvalue, value[i].ptr, value[i].len))
                      rows++;
                  
             
          }
          free(value);
          
          
    //}
    }
    cout << rows << endl;
    //cout << column1[0] << endl;
    return 1;
}

int main(int argc, char * argv[] ){
  if (argc == 3){
      if (strstr(argv[1], "diff"))
         return read_diff_materialize(argc,argv);
      if (strstr(argv[1], "mostlyordered"))
         return read_mostly_orderedglob(argc,argv);
      if (strstr(argv[1], "glob"))
         return read_glob_mater(argc,argv);
      if (strstr(argv[1], "parq"))
         return read_parquet(argc,argv);
      if (strstr(argv[1], "orc"))
         return read_orc(argc,argv);
      if (strstr(argv[1], "indirect"))
         return read_indirect_mater(argc,argv);
      
      return read_dict(argc,argv);
      }
  if (argc == 4){
      if (strstr(argv[1], "diff"))
         return random_access_diff(argc,argv);
      if (strstr(argv[1], "mostlyordered"))
         return random_access_mostly_orderedglob(argc,argv);
      if (strstr(argv[1], "glob"))
         return random_access_glob(argc,argv);
      if (strstr(argv[1], "orc"))
         return random_access_orc(argc,argv);
      if (strstr(argv[1], "parquet"))
         return random_access_parquet(argc,argv);
      
      return random_access_dict(argc,argv);
      
      
  }
  if (argc == 6){
      if (strstr(argv[1], "diff"))
         return read_diff_filt(argc,argv);
      if (strstr(argv[1], "mostlyordered"))
         return read_mostly_orderedglob_filt(argc,argv);
      if (strstr(argv[1], "glob"))
         return read_glob_filt(argc,argv);
      if (strstr(argv[1], "parq"))
         return read_parquet_filt(argc,argv);
      if (strstr(argv[1], "orc"))
         return read_orc_filt(argc,argv);
      if (strstr(argv[1], "indirect"))
         return read_indirect_filt(argc,argv);
     
      return read_dict_filt(argc,argv);
      }
 if (argc == 7){
      if (strstr(argv[1], "diff"))
         return read_diff_range(argc,argv);
      else if (strstr(argv[1], "mostlyordered"))
         return read_mostly_orderedglob_range(argc,argv);
      else if  (strstr(argv[1], "unordered"))
          return read_glob_range_unordered(argc,argv);
      else if  (strstr(argv[1], "glob"))
          return read_glob_range(argc,argv);
      else if  (strstr(argv[1], "unindirect"))
          return read_indirect_range(argc,argv);
      else if  (strstr(argv[1], "indirect"))
          return read_indirect_range_ord(argc,argv);
       
      read_diff_range(argc,argv);
        
      }
}
