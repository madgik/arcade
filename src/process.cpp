#include "process.h"


int Processing::get_column_value(FILE *f1, char*** &cols, int blocknum, long int blockstart, struct fileH fileheader1, struct D header1, vector <int> columns, int* rowids, int rowidsnum, int join1, long* data, vector <unordered_map <int, vector <string>*>> &dict_cache,
Caches &mcaches){

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
	  mcaches.get_values(f1,values1,initstep+sizeof(struct D)+header1.previndices * 2,header1.indicessize);
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
			  int res = mcaches.get_short_offsets(f1,seek_position, header1.indicessize, header1.numofvals, offsets);
			  for (int j = 0; j < rowidsnum; j++)
					   off[j] = offsets[rowids[j]];
			  }
		  
		if (header1.bytes==0){ // four byte offsets
			  unsigned int* offsets;   
			  int res = mcaches.get_int_offsets(f1,seek_position, header1.indicessize, header1.numofvals, offsets);
			  for (int j = 0; j < rowidsnum; j++)
					   off[j] = offsets[rowids[j]];
			  }
		  
		if (header1.bytes==2){ // one byte offsets
			  unsigned char* offsets;    
			  int res = mcaches.get_char_offsets(f1, seek_position, header1.indicessize, header1.numofvals, offsets);
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
				mcaches.get_values(f1,dict_cache[i][rightblock], initstep2+sizeof(struct D)+header2.minmaxsize+ header2.previndices*2,   header2.dictsize);
		   }
		   cols[colnum][c] = &(*dict_cache[i][rightblock])[position_in_block][0];
		   c++;
		}
		else {
			if (dict_cache[i].find(rightblock) == dict_cache[i].end())
				mcaches.get_values(f1,dict_cache[i][rightblock], initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2, header1.dictsize);
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
			  mcaches.get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
			  for (int j = 0; j < rowidsnum; j++)
					   off[j] = offsets[rowids[j]];
			  }
		  
		
		   if (header1.bytes==0){ // four byte offsets
			unsigned int* offsets;  
			  mcaches.get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
			  for (int j = 0; j < rowidsnum; j++)
					   off[j] = offsets[rowids[j]];
			}
			if (header1.bytes==2){ // one byte offsets
			unsigned char* offsets;  
			  mcaches.get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
			  for (int j = 0; j < rowidsnum; j++)
					   off[j] = offsets[rowids[j]];
			}
		
		
		
			mcaches.get_values(f1,dict_cache[i][0],initstep1+sizeof(struct D)+header1.minmaxsize+ header1.previndices*2, header1.dictsize);
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

int Processing::filter_page(FILE *f1, char*** &cols, int &blocknum, unsigned long &initstep1, int join1, struct fileH fileheader1, int &totalcount1, string &value, vector <int> retcolumns, vector <unordered_map <int, vector <string>*>> &dict_cache, int &global_len, int &offset, long* (&data),
Caches &mcaches  ){
		
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
				 vector <string> *values1;
				 mcaches.get_values(f1,values1, initstep1 + sizeof(struct D), header1.indicessize);
				 char buffer1[header1.indicessize];
			
				for(string &i : *values1) 
					if (i == value)
					  found_index++;
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
				get_column_value(f1, cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, data, dict_cache, mcaches);

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
		   
				mcaches.get_values(f1,values1, initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize);
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
					mcaches.get_values(f1,values1, initstep1+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize);
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
				get_column_value(f1, cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1,  data, dict_cache, mcaches);
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
			  mcaches.get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
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
				mcaches.get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
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
				mcaches.get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
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
			get_column_value(f1,cols, blocknum, blockstart, fileheader1, header1, retcolumns, rowids, found_index, join1, data, dict_cache, mcaches);
			delete [] rowids;
			return found_index;
			}
		}
	return 0;
}

int Processing::scan_page(FILE *f1, char*** &cols, int &blocknum, unsigned long &initstep1, struct fileH fileheader1, int &blockn, int &totalcount1, vector <int> retcolumns, vector<vector <string>> &globaldict, int &global_len,
Caches &mcaches  ){
		struct D header1;
	
		blocknum++;
		int current, next;
		int blockposition = initstep1;
		int columnslen = retcolumns.size();
	
		for (int colnum = 0; colnum < columnslen; colnum++){
			fseek(f1, blockposition + sizeof(int)*retcolumns[colnum], SEEK_SET);
			result = fread(&current,sizeof(int),1,f1);
			fseek(f1, blockposition + sizeof(int)*(fileheader1.numofcols), SEEK_SET);
			result =  fread(&next,sizeof(int),1,f1);
			fseek(f1,blockposition + current + sizeof(int)*(fileheader1.numofcols+1), SEEK_SET);
			result =  fread(&header1, sizeof(struct D),1,f1);
			if (colnum == 0){
				initstep1 += sizeof(int)*(fileheader1.numofcols+1) + next;
				totalcount1 += header1.numofvals;
			} 
		
			if (header1.dictsize == 0){ //no dict column
				 vector <string> *values1;
				 mcaches.get_values(f1,values1, blockposition + current + sizeof(int)*(fileheader1.numofcols+1) + sizeof(struct D), header1.indicessize);
				 int ind = 0;
				 for (string &i : *values1)
				   cols[colnum][ind++] = &i[0];    
			}
			else { // dict column
				int check = 0;
				if (header1.diff == 1) {global_len = 0; globaldict[colnum].clear(); blockn = 0;}// case local dict
				int temp = global_len;
				global_len += header1.lendiff;
				vector<string> *values1;
				if (header1.lendiff > 0){
					mcaches.get_values(f1,values1, blockposition + current + sizeof(int)*(fileheader1.numofcols+1)+sizeof(struct D)+header1.previndices*2+header1.minmaxsize,header1.dictsize);
					globaldict[colnum].insert(globaldict[colnum].end(), (*values1).begin(), (*values1).end());
				}
				if (header1.indicessize == 4){
				    int unique_offset;
				    fseek(f1,blockposition + current + sizeof(int)*(fileheader1.numofcols+1)+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize,SEEK_SET);
				    result = fread(&unique_offset,sizeof(int),1,f1);
					for (int i=0; i < header1.numofvals; i++) // TODO to revisit perhaps this is buggy 
						cols[colnum][i] = &(globaldict[colnum][unique_offset])[0];
				}
				else{
					long int position = blockposition + current + sizeof(int)*(fileheader1.numofcols+1)+sizeof(struct D)+header1.dictsize+header1.previndices*2 + header1.minmaxsize;
					if (header1.bytes==1){ // two byte offsets /*read offsets of file 1*/
					  unsigned short* offsets;  
				  
					  mcaches.get_short_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
					  for (int i=0; i < header1.numofvals; i++)
							cols[colnum][i] = &(globaldict[colnum][offsets[i]])[0];
					}
					if (header1.bytes==0){ // four byte offsets
						unsigned int* offsets;  
						mcaches.get_int_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
						for (int i=0; i < header1.numofvals; i++)
							cols[colnum][i] = &(globaldict[colnum][offsets[i]])[0];
					}
					if (header1.bytes==2){ // one byte offsets
						unsigned char* offsets;  
						mcaches.get_char_offsets(f1, position, header1.indicessize, header1.numofvals, offsets);
						for (int i=0; i < header1.numofvals; i++)
							cols[colnum][i] = &(globaldict[colnum][offsets[i]])[0];
					}
				}
			blockn++;
			} 
		}
		return header1.numofvals;
}


