//g++-10 -O3 -std=c++20 "-fcoroutines" -o test test.cpp  -lsnappy
// g++-10  -std=c++20  "-fcoroutines" -o test2 test.cpp  -lsnappy -O3 -freorder-blocks-algorithm=simple
int SNAPPY = 0;
size_t result;


int get_values_no_cache(FILE *f1, long int position, int dictsize, vector <string> &values, unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){
    
      fseek(f1, position, SEEK_SET);
	  if (SNAPPY){
   		char buffer1[dictsize];
    	size_t ot =  fread(buffer1,dictsize,1,f1);
    	string output;
    	snappy::Uncompress(buffer1, dictsize, &output);
    	values = hps::from_char_array<std::vector<string>>(buffer1);
      }
      else{
        char buffer1[dictsize];
        result =  fread(buffer1,dictsize,1,f1);
        values = hps::from_char_array<std::vector<string>>(buffer1);         
      }
    return 1;
}


int get_values(FILE *f1, vector <string>* &values, long int position, int dictsize, unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){
    
    if (values_cache.find(position) == values_cache.end()) {
      fseek(f1, position, SEEK_SET);
      char buffer1[dictsize];
	  if (SNAPPY){
    	size_t ot =  fread(&buffer1,dictsize,1,f1);
    	string output;
    	snappy::Uncompress(buffer1, dictsize, &output);
    	values_cache[position] = hps::from_char_array<std::vector<string>>(buffer1);
      }
      else{
        result =  fread(buffer1,dictsize,1,f1);
        values_cache[position] = hps::from_char_array<std::vector<string>>(buffer1);         
      }
    }
    values = &values_cache[position];
    return 1;
}


/*int get_value(FILE *f1, char*** &cols, int valposition, long int position, int dictsize, unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){
    
    if (values_cache.find(position) == values_cache.end()) {
      fseek(f1, position, SEEK_SET);
      char buffer1[dictsize];
	  if (SNAPPY){
    	size_t ot =  fread(&buffer1,dictsize,1,f1);
    	string output;
    	snappy::Uncompress(buffer1, dictsize, &output);
    	values_cache[position] = hps::from_char_array<std::vector<string>>(buffer1);
      }
      else{
        result =  fread(buffer1,dictsize,1,f1);
        values_cache[position] = hps::from_char_array<std::vector<string>>(buffer1);         
      }
    }
    values = &values_cache[position];
    return 1;
}*/




//fseek(f1,initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
int get_short_offsets(FILE *f1, long int position, int indicessize,int numofvals, unsigned short *(&offsets), unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){

    if (short_offsets_cache.find(position) == short_offsets_cache.end()) {
      fseek(f1, position, SEEK_SET);
      offsets = new unsigned short [numofvals];  

      if (SNAPPY){
   		char buffer1 [indicessize];
   		result =  fread(buffer1,indicessize,1,f1);
        string output;
    	snappy::Uncompress(buffer1, indicessize, &output);
    	copy(&output[0], &output[0]+(sizeof(unsigned short)*numofvals), reinterpret_cast<char*>(offsets));	      
      }
      else{
        result =  fread(offsets, 2, numofvals,f1);  

       }
      short_offsets_cache[position] = offsets;
      return 0;
    }
    else
        offsets = short_offsets_cache[position];
    return 1;
}

int get_int_offsets(FILE *f1,long int position, int indicessize,int numofvals, unsigned int *(&offsets), unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){

    
    if (int_offsets_cache.find(position) == int_offsets_cache.end()) {
      fseek(f1, position, SEEK_SET);
      offsets = new unsigned int [numofvals];  
      if (SNAPPY){
   		char buffer1[indicessize];
   		result =  fread(buffer1,indicessize,1,f1);
        string output;
    	snappy::Uncompress(buffer1, indicessize, &output);
    	copy(&output[0], &output[0]+(sizeof(unsigned int)*numofvals), reinterpret_cast<char*>(offsets));	      
      }
      else
        result =  fread(offsets,4,numofvals,f1);  
       
      int_offsets_cache[position] = offsets;
      return 0;
    }
    else{
        offsets = int_offsets_cache[position];
        }
    return 1;
}

int get_char_offsets(FILE *f1,long int position, int indicessize, int numofvals, unsigned char *(&offsets), unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){
   
    if (char_offsets_cache.find(position) == char_offsets_cache.end()) {
      fseek(f1, position, SEEK_SET);
      offsets = new unsigned char [numofvals];  
      if (SNAPPY){
   		char buffer1[indicessize];
   		result =  fread(buffer1,indicessize,1,f1);
        string output;
    	snappy::Uncompress(buffer1, indicessize, &output);
    	copy(&output[0], &output[0]+(sizeof(unsigned char)*numofvals), reinterpret_cast<char*>(offsets));	      
      }
      else
        result =  fread(offsets,1,numofvals,f1);  
       
      char_offsets_cache[position] = offsets;
      return 0;
    }
    else{
        offsets = char_offsets_cache[position];
        }
    return 1;
}
 /*for (int j = 0; j < rowidsnum; j++)
    off[j] = offsets1[rowids[j]];
*/

int get_column_value(FILE *f1, char*** &cols, int blocknum, long int blockstart, struct fileH fileheader1, struct D header1, vector <int> columns, int* rowids, int rowidsnum, int join1, long* data, vector <unordered_map <int, vector <string>*>> &dict_cache,
unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache){
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
  int colnum = -1;
  int initstep;
    int temp = 0;
    int prevtemp = 0;
    int rightblock = 0;
    int found = 0;
    int position_in_block;
    int relative_block_num = 0;    
  for (int i: columns){
    colnum++;
    if (i == join1){
        continue;
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
   	unsigned short int *previndex = new unsigned short int[header1.previndices];
    result =  fread(previndex, header1.previndices * 2, 1, f1);
    if (header1.dictsize == 0){ // case no dictionary
      vector <string> *values1;
      get_values(f1,values1,initstep+sizeof(struct D)+header1.previndices * 2,header1.indicessize,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
      ;
      int c = 0;
      for (int j = 0; j < rowidsnum; j++){
         cols[colnum][c] = &(*values1)[rowids[j]][0];
         c++;
      }
    }
    
    else if (header1.diff == 0){
     //case differential dictionary
     
     int c = 0;
        int initstep1 = initstep;

        int *off = new int[rowidsnum];
        long int seek_position = initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize;
        
        if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*
              unsigned short* offsets;  
              int res = get_short_offsets(f1,seek_position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
              }
              
        if (header1.bytes==0){ // four byte offsets
              unsigned int* offsets;   
              int res = get_int_offsets(f1,seek_position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
              }
              
        if (header1.bytes==2){ // one byte offsets
              unsigned char* offsets;    
              int res = get_char_offsets(f1, seek_position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
              }
      
 
    for (int j = 0; j < rowidsnum; j++){
    temp = 0;
    prevtemp = 0;
    rightblock = 0;
    found = 0;
    position_in_block;
    relative_block_num = 0; 
    
    
    
    for (int co = 1; co<header1.previndices; co+=2){
        prevtemp = temp;
        temp += previndex[co];

        if (temp <= off[j])
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
            if (dict_cache[i].find(rightblock) == dict_cache[i].end()){
            	unsigned long initstep2 = data[rightblock];
            	struct D header2;
            	int current2;
            	fseek(f1, initstep2 + sizeof(int)*i, SEEK_SET);
            	result =  fread(&current2,sizeof(int),1,f1);
            	initstep2 += current2 + sizeof(int)*(fileheader1.numofcols+1);
            	fseek(f1,initstep2, SEEK_SET);
            	result =  fread(&header2, sizeof(struct D),1,f1);   
            	get_values(f1,dict_cache[i][rightblock], initstep2+sizeof(struct D)+header2.minmaxsize+ header2.previndices*2,   header2.dictsize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
           }
           cols[colnum][c] = &(*dict_cache[i][rightblock])[position_in_block][0];
           c++;
        }
        else {
            if (dict_cache[i].find(rightblock) == dict_cache[i].end())
                get_values(f1,dict_cache[i][rightblock], initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2, header1.dictsize,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
            cols[colnum][c] = &(*dict_cache[i][rightblock])[position_in_block][0];
            c++;
        }
        
        }
        delete [] off;
    }
        else if (header1.diff == 1){ //local dictionary
        
        int c = 0;
            int initstep1 = initstep;
            int *off = new int[rowidsnum];
            long int position = initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize;
            if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
              unsigned short* offsets;  
              get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
              }
              
            
           if (header1.bytes==0){ // four byte offsets
            unsigned int* offsets;  
              get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
            }
            if (header1.bytes==2){ // one byte offsets
            unsigned char* offsets;  
              get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int j = 0; j < rowidsnum; j++)
    		           off[j] = offsets[rowids[j]];
            }
            
            
            
            get_values(f1,dict_cache[i][0],initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2, header1.dictsize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
            for (int j = 0; j < rowidsnum; j++){
                cols[colnum][c] = &(*dict_cache[i][0])[off[j]][0];
                c++;
            //cout << values1[off] << endl;
            
            }
            delete [] off;
    }
    delete [] previndex;
    }


}
return rowidsnum;
}

int filter_page(FILE *f1, char*** &cols, int &blocknum, unsigned long &initstep1, int join1, struct fileH fileheader1, int &totalcount1, string &value, vector <int> retcolumns, vector <unordered_map <int, vector <string>*>> &dict_cache, int &global_len, int &offset, long* (&data),
unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache  ){
	        
	        struct D header1;
	        int boolminmax = 1; 
            int boolminmax_diff = 1;
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
   		    //TODO if it is a count query, do not malloc rowids
   		    int* rowids = new int[header1.numofvals]; //TODO dynamic memory allocation
   		    if (header1.dictsize == 0){
   		        //if (totalcount1 == 0)
   		         //   parquetvalues1.reserve(fileheader1.numofvals);
   		         vector <string> *values1;
   		         get_values(f1,values1, initstep1 + sizeof(struct D), header1.indicessize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
   		        //fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
   		        //get_values(f1,initstep1 + sizeof(struct D), header1.indicessize, values1,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
    		    char buffer1[header1.indicessize];
    		    
    			for(string &i : *values1) 
                    if (i == value)
                      found_index++;
                      
    			//parquetvalues1.insert( parquetvalues1.end(), values1.begin(), values1.end() );
    			//string**  myvec = (string**)malloc(found_index*sizeof(string*));
     		    //for(int mal=0; mal < found_index; mal++) myvec[mal] = &value;
     		    int retcs = retcolumns.size();
     		   
                int coln = -1;
                for (int i: retcolumns){
                      coln++;
                      if (i == join1){
                        for(int mall=0; mall < found_index; mall++)
                          cols[coln][mall] = &value[0];
                      }
                }
                if (retcs>0)
     		    get_column_value(f1, cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, data, dict_cache,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);

   				//totalcount1 += header1.numofvals;
   				initstep1 += next-current;
   				delete [] rowids;
   				return found_index;
   				
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
    		 vector <string> minmax1 = hps::from_char_array<std::vector<string>>(minmaxbuf);
    		 vector<string> *values1;
   			 if (header1.lendiff > 0){
   			   if (header1.diff == 0){
   			   
   			    get_values(f1,values1, initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
    		    //cout << values1.front() << " la "<<values1.back() << " la "<<minmax1[0] << " la -" <<minmax1[1]<<endl;
    			if (value.compare((*values1).front())<0 or value.compare((*values1).back())>0){
   		      		//cout << "kaka" << endl;
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		offset = -1;
   		      		delete [] rowids;
   		      	return 0;
   		    	} 
   		    }
   		    else {
   		        check = 1;
   		    	if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
   		      		//global_len = global_len + header1.lendiff;
   		      		initstep1 += next-current;
   		      		offset = -1;
                delete [] rowids;
 				return 0;
   		    	}
   		    	else {
   		    	    get_values(f1,values1, initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
   		    	}
   		 }
   		 
   		}
   		     int valsize = (*values1).size();
    		 for (int k = 0; k < valsize; k++){
   			   if (value == (*values1)[k]){
   			     offset = k + temp;
   			     break;
   			     }
   			     offset = -1;
   			 }

   		    
             if (offset == -1){
                //if (header1.lendiff == 0)
                    
                initstep1 += next-current;
                delete [] rowids;
                return 0;
             }
   			}
   			
   			if (!check){
   			
   			 char minmaxbuf[header1.minmaxsize];
   			 fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2, SEEK_SET);
    		 result =  fread(minmaxbuf,header1.minmaxsize,1,f1);

    		 vector <string> minmax1 = hps::from_char_array<std::vector<string>>(minmaxbuf);


    		/* msgpack::unpacked minmax;
    		 
    		 unpack(minmax, minmaxbuf, header1.minmaxsize);
    		 vector<string> minmax1;

    		 minmax.get().convert(minmax1);*/
    		 if (boolminmax)
    		 if (value.compare(minmax1[0])<0 or value.compare(minmax1[1])>0){
    		     
   		      	 initstep1 += next-current;
   		      	 delete [] rowids;
    		     return 0;
    		 }
   			}
   			if (header1.indicessize == 4){
   			    
                int offf;
                fseek(f1,initstep1+sizeof(struct D)+header1.previndices*2+header1.dictsize + header1.minmaxsize,SEEK_SET);
    			result =  fread(&offf,header1.indicessize,1,f1);
                if (offf == offset){
                  for (int i=0; i < header1.numofvals; i++){
     		    	rowids[found_index] = i; 
     		        found_index++;
     		    }
     		    // TODO in this case, the full block evaluates
                //string**  myvec = (string**)malloc(found_index*sizeof(string*));
     		     //for(int mal=0; mal < found_index; mal++) myvec[mal] = &value;
     		    int retcs = retcolumns.size();
     		   
    			int coln = -1;
                for (int i: retcolumns){
                      coln++;
                      if (i == join1){
                        for(int mall=0; mall < found_index; mall++)
                          cols[coln][mall] = &value[0];
                      }
                }
                if (retcs>0)
     		    get_column_value(f1, cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1,  data, dict_cache,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
     		    //cols = get_column_value(f1, blocknum, blockstart, fileheader1, header1, retcolumns, col, fcount, join);
     		    initstep1 += next-current;
     		    delete [] rowids;
     		    return found_index;
     		    }
     			initstep1 += next-current;
        }
        else{
            long int position = initstep1+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize;
    		if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
     		  unsigned short* offsets;  
              get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
              for (int i=0; i < header1.numofvals; i++){
     		    
     		   if (offsets[i] == offset){
     		        	//col[fcount].rowid = count;
     		            //col[fcount].val = value;
     		       rowids[found_index] = i; 
     		       found_index++;
     		   }
    		}
     		}

     		if (header1.bytes==0){ // four byte offsets
     			unsigned int* offsets;  
                get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
                for (int i=0; i < header1.numofvals; i++){
     		    
     		   if (offsets[i] == offset){
     		        	//col[fcount].rowid = count;
     		            //col[fcount].val = value;
     		       rowids[found_index] = i; 
     		       found_index++;
     		   }
    		}
     		}
     		if (header1.bytes==2){ // one byte offsets
     			unsigned char* offsets;  
                get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
                for (int i=0; i < header1.numofvals; i++){
     		    
     		    if (offsets[i] == offset){
     		        	//col[fcount].rowid = count;
     		            //col[fcount].val = value;
     		       rowids[found_index] = i; 
     		       found_index++;
     		   }
    		}
     		}
     		initstep1 += next-current;
     		 //string**  myvec = (string**)malloc(found_index*sizeof(string*));
     		     //for(int mal=0; mal < found_index; mal++) myvec[mal] = &value;
     		    int retcs = retcolumns.size();
     		   
    			int coln = -1;
                for (int i: retcolumns){
                      coln++;
                      if (i == join1){
                        for(int mall=0; mall < found_index; mall++)
                          cols[coln][mall] = &value[0];
                      }
                }
            if (retcs>0)
     		get_column_value(f1,cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, data, dict_cache,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
     		delete [] rowids;
     		return found_index;
     		}
		}
    return 0;
}





Generator <int> equi_filter(char* filename, char*** &cols, int &col_num, char* &val, int* &retcols, int &colnum, bool &cont){

  unordered_map<long int, vector <string>> values_cache;
  unordered_map<long int, unsigned short* > short_offsets_cache;  
  unordered_map<long int, unsigned int* > int_offsets_cache;
  unordered_map<long int, unsigned char* > char_offsets_cache;
  
  while (cont == 1) {  
    //TODO if file changes free caches
    //colnum = sizeof(retcols) / sizeof(retcols[0]);
    vector<int> retcolumns(retcols, retcols + colnum);
    int max = -1;
    if (colnum>0)
        max = *max_element(retcolumns.begin(), retcolumns.end());
    vector <unordered_map <int, vector <string>*>> dict_cache(max+1);
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(filename,"rb");
    vector<string> parquetvalues1;
    string value = val;
    int offset = -1;
    int global_len = 0;
    /*read marker of file*/

    char marker[5];
    result =  fread(marker, 4, 1, f1);
    if (strncmp(marker,"DIFF",4) != 0){
        co_yield -2;
    }
    else{
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;

    long* data = new long[fileheader1.numofblocks+1];
	result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);

	unsigned long initstep1 = 4 + sizeof(struct fileH)+(fileheader1.numofblocks+1)*8;
	
	if (strstr(filename, "snappy"))
      SNAPPY = 1;
    else
      SNAPPY = 0;
      
	int join1 = col_num;
	int blocknum = -1;
    
    int rows = 0;
     cols = (char***)malloc(colnum*sizeof(char **));
     for(int mal=0; mal < colnum; mal++) cols[mal] = (char**)malloc(65535*sizeof(char*));
    
    
 	while (totalcount1 < fileheader1.numofvals){
 	     rows = filter_page(f1, cols, blocknum, initstep1, join1, fileheader1, totalcount1,  value, retcolumns, dict_cache, global_len, offset, data,  values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
 	     co_yield rows;
 	}
 	delete [] data;
 	for(int mal=0; mal < colnum; mal++) free(cols[mal]);
 	free(cols);
    fclose(f1);
    co_yield -1;
    }
    }
 //TODO free caches
}


Generator <int> random_access(char* filename, char*** &cols, int* &retcols, int &colnum,  int* &rowids, int &rowidsnum, bool &cont){


        unordered_map<long int, vector <string>> values_cache;
  		unordered_map<long int, unsigned short* > short_offsets_cache;  
  		unordered_map<long int, unsigned int* > int_offsets_cache;
  		unordered_map<long int, unsigned char* > char_offsets_cache;
  		
  		
  		while (cont==1){
  		
  		cols = (char***)malloc(colnum*sizeof(char **));
        for(int mal=0; mal < colnum; mal++) cols[mal] = (char**)malloc(65535*sizeof(char*));
  		 vector<int> columns(retcols, retcols + colnum);
         int max = -1;
         if (colnum>0)
              max = *max_element(columns.begin(), columns.end());
    
        vector <unordered_map <int, vector <string>*>> dict_cache(max+1);
        
        FILE *f1;
        f1 = fopen(filename,"rb");
        struct D header1;
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
        long* data = new long[fileheader1.numofblocks+1];
        
        int rowid = 0;
        unsigned long initstep1 = 0;
        int blocknum = 0;
        if (strstr(filename, "snappy"))
      		SNAPPY = 1;
    	else
      		SNAPPY = 0;
        
        result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
        
        int temp_blocknum = -1;
        int rows_per_block = 0;
        int start_of_block = 0;
    
        for (int i = 0; i < rowidsnum; i++){
            blocknum = (rowids[i])/data[fileheader1.numofblocks];
            if (blocknum != temp_blocknum and temp_blocknum != -1){
                rowids[i] = (rowids[i])%data[fileheader1.numofblocks];
                dict_cache.clear();
                co_yield get_column_value(f1,cols, temp_blocknum, initstep1, fileheader1, header1, columns, rowids+start_of_block, rows_per_block, -1, data, dict_cache,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);   
                initstep1 = data[blocknum];
                temp_blocknum = blocknum;
                rows_per_block = 0;
                start_of_block = i;   
            }
            temp_blocknum = blocknum;
            rows_per_block++;
        	rowids[i] = (rowids[i])%data[fileheader1.numofblocks];
        	initstep1 = data[blocknum];
        	if (i == rowidsnum-1){
        		dict_cache.clear();
        	    co_yield get_column_value(f1,cols, blocknum, initstep1, fileheader1, header1, columns, rowids+start_of_block, rows_per_block, -1, data, dict_cache,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);   

        	}
        }
       delete [] data;
 	   for(int mal=0; mal < colnum; mal++) free(cols[mal]);
 	    free(cols);
        fclose(f1);
        co_yield -1;
}
        
        //cout << data[blocknum] << endl;

}


int scan_page(FILE *f1, char*** &cols, int &blocknum, unsigned long &initstep1, struct fileH fileheader1, int &totalcount1, vector <int> retcolumns, vector <string>* &globaldict, int &global_len,
unordered_map<long int, vector <string>> &values_cache,
unordered_map<long int, unsigned short* > &short_offsets_cache,  
unordered_map<long int, unsigned int* > &int_offsets_cache,
unordered_map<long int, unsigned char* > &char_offsets_cache  ){
	    //partially works, needs handling of global dictionary, currently there is no append into global dict
		struct D header1;
		blocknum++;
		int current, next;
		int blockposition = initstep1;
		int columnslen = retcolumns.size();
		for (int colnum = 0; colnum < columnslen; colnum++){
			fseek(f1, blockposition + sizeof(int)*colnum, SEEK_SET);
			result = fread(&current,sizeof(int),1,f1);
			fseek(f1, blockposition + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
			result =  fread(&next,sizeof(int),1,f1);
			initstep1 += current + sizeof(int)*(fileheader1.numofcols+1);
			fseek(f1,blockposition + current + sizeof(int)*(fileheader1.numofcols+1), SEEK_SET);
			result =  fread(&header1, sizeof(struct D),1,f1);
			totalcount1 += header1.numofvals;
			cout << header1.numofvals << endl;
			if (header1.dictsize == 0){ //no dict column
				 vector <string> *values1;
				 get_values(f1,values1, blockposition + current + sizeof(int)*(fileheader1.numofcols+1) + sizeof(struct D), header1.indicessize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
				 int ind = 0;
				 for (string &i : *values1)
				   cols[colnum][ind++] = &i[0];    
			}
			else { // dict column
				int check = 0;
				if (header1.diff == 1) {global_len = 0; globaldict = NULL;}// case local dict
				
				int temp = global_len;
				global_len += header1.lendiff;
				vector<string> *values1;
				if (header1.lendiff > 0)
					get_values(f1,values1, blockposition + current + sizeof(int)*(fileheader1.numofcols+1)+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
				
				if (header1.indicessize == 4){
					for (int i=0; i < header1.numofvals; i++)
							cols[colnum][i] = &(*values1)[0][0]; // revisit 
				}
				else{
				    
					long int position = blockposition + current + sizeof(int)*(fileheader1.numofcols+1)+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize;
					if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
					  unsigned short* offsets;  
					  get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
					  for (int i=0; i < header1.numofvals; i++)
						   cols[colnum][i] = &(*values1)[offsets[i]][0]; 
					  
					}
					if (header1.bytes==0){ // four byte offsets
						unsigned int* offsets; 
						 
						get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
						for (int i=0; i < header1.numofvals; i++)
							  cols[colnum][i] =  &(*values1)[offsets[i]][0];
						
					
					}
					if (header1.bytes==2){ // one byte offsets
						unsigned char* offsets;  
						get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets,values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
						for (int i=0; i < header1.numofvals; i++)
							  cols[colnum][i] = &(*values1)[offsets[i]][0];
					}
					
				}
			} 

		}
		initstep1 = ftell(f1);
		cout  << header1.numofvals << endl;
		return header1.numofvals;
}


Generator <int> scan(char* filename, char*** &cols, int* &retcols, int &colnum, bool &cont){
  	unordered_map<long int, vector <string>> values_cache;
  	unordered_map<long int, unsigned short* > short_offsets_cache;  
  	unordered_map<long int, unsigned int* > int_offsets_cache;
  	unordered_map<long int, unsigned char* > char_offsets_cache;
    
    while (cont == 1) {  
    //TODO if file changes free caches
    //colnum = sizeof(retcols) / sizeof(retcols[0]);
    
    vector<int> retcolumns(retcols, retcols + colnum);
    int max = -1;
    if (colnum>0)
        max = *max_element(retcolumns.begin(), retcolumns.end());
    int totalcount1=0;
    FILE *f1;
    f1 = fopen(filename,"rb");
    vector<string> parquetvalues1;
    vector <string>* globaldict;
    int global_len = 0;
    /*read marker of file*/

    char marker[5];
    result =  fread(marker, 4, 1, f1);
    if (strncmp(marker,"DIFF",4) != 0){
        co_yield -2;
    }
    else{
    struct fileH fileheader1;
    result =  fread(&fileheader1,sizeof(struct fileH), 1, f1);
    vector <vector <int>> index1;

	unsigned long initstep1 = 4 + sizeof(struct fileH)+(fileheader1.numofblocks+1)*8;
	
	if (strstr(filename, "snappy"))
      SNAPPY = 1;
    else
      SNAPPY = 0;
      
	int blocknum = -1;
    
    int rows = 0;
     cols = (char***)malloc(colnum*sizeof(char **));
     for(int mal=0; mal < colnum; mal++) cols[mal] = (char**)malloc(65535*sizeof(char*));
    
    
 	while (totalcount1 < fileheader1.numofvals)
 	     co_yield scan_page(f1, cols, blocknum, initstep1, fileheader1, totalcount1, retcolumns, globaldict, global_len, values_cache,short_offsets_cache,int_offsets_cache,char_offsets_cache);
 	     
 	for(int mal=0; mal < colnum; mal++) free(cols[mal]);
 	free(cols);
    fclose(f1);
    co_yield -1;
    }
    }

}




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
	unsigned long initstep1 = 4 + sizeof(struct fileH) + (fileheader1.numofblocks+1)*8;
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
	unsigned long initstep1 = 4 + sizeof(struct fileH) + (fileheader1.numofblocks+1)*8;
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
        result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
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

    for (int co = 1; co<header1.previndices; co+=2){
        prevtemp = temp;
        temp += previndex[co];

        if (temp <= off)
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
    result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);

	unsigned long initstep1 = 4 + sizeof(struct fileH)+(fileheader1.numofblocks+1)*8;
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

int print_columns(char*** &cols, int rows, int coln){
             for (int i=0; i<rows; i++){
     		     for (int j=0; j<coln; j++){
     		      if (j == coln-1)
        			printf("%s", cols[j][i]);
        		  else {
        		  printf("%s", cols[j][i]); ;
        		  cout << "\033[1;31m|\033[0m";
        		  }
    			}
    			if (coln>0) cout << endl;	
     		}
     		
     	

return 1;
}

/*int main(int argc, char * argv[] ){
  
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
         char lala[21] = "10.1051/cocv:2002047";
         double duration;
         
         equi_filter(argv[1],argv[2],argv[3],argv[4],argv[5],argv[6]);
         int i = 0;
        while (i<100){
         std::clock_t start = std::clock();
         equi_filter(argv[1],argv[2],lala,argv[4],argv[5],argv[6]);
         duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
         cout << duration << endl;
         i++;
         }
         }
     
      }
 
      
}
*/