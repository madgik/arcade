#include "cache.h"


int Caches::get_values(FILE *f1, vector <string>* &values, long int position, int dictsize){

	if (this->values_cache.find(position) == this->values_cache.end()) {
	  fseek(f1, position, SEEK_SET);
	  char buffer1[dictsize];
	  if (SNAPPY){
		size_t ot = fread(&buffer1,dictsize,1,f1);
		string output;
		snappy::Uncompress(buffer1, dictsize, &output);
		this->values_cache[position] = hps::from_string<std::vector<string>>(output);
	  }
	  else{
		result = fread(buffer1,dictsize,1,f1);
		this->values_cache[position] = hps::from_char_array<std::vector<string>>(buffer1);         
	  }
	}
	values = &(this->values_cache)[position];
	return 1;
}


int Caches::get_short_offsets(FILE *f1, long int position, int indicessize,int numofvals, unsigned short *(&offsets)){

	if (this->short_offsets_cache.find(position) == this->short_offsets_cache.end()) {
	  fseek(f1, position, SEEK_SET);
	  offsets = new unsigned short [numofvals];  

	  if (SNAPPY == -1){
		char buffer1 [indicessize];
		result =  fread(buffer1,indicessize,1,f1);
		string output;
		snappy::Uncompress(buffer1, indicessize, &output);
		copy(&output[0], &output[0]+(sizeof(unsigned short)*numofvals), reinterpret_cast<char*>(offsets));	      
	  }
	  else{
		result =  fread(offsets, 2, numofvals,f1);  

	   }
	  this->short_offsets_cache[position] = offsets;
	  return 0;
	}
	else
		offsets = this->short_offsets_cache[position];
	return 1;
}

int Caches::get_int_offsets(FILE *f1,long int position, int indicessize,int numofvals, unsigned int *(&offsets)){


	if (this->int_offsets_cache.find(position) == this->int_offsets_cache.end()) {
	  fseek(f1, position, SEEK_SET);
	  offsets = new unsigned int [numofvals];  
	  if (SNAPPY == -1){
		char buffer1[indicessize];
		result =  fread(buffer1,indicessize,1,f1);
		string output;
		snappy::Uncompress(buffer1, indicessize, &output);
		copy(&output[0], &output[0]+(sizeof(unsigned int)*numofvals), reinterpret_cast<char*>(offsets));	      
	  }
	  else
		result =  fread(offsets,4,numofvals,f1);  
   
	  this->int_offsets_cache[position] = offsets;
	  return 0;
	}
	else{
		offsets = this->int_offsets_cache[position];
		}
	return 1;
}

int Caches::get_char_offsets(FILE *f1,long int position, int indicessize, int numofvals, unsigned char *(&offsets)){

	if (this->char_offsets_cache.find(position) == this->char_offsets_cache.end()) {
	  fseek(f1, position, SEEK_SET);
	  offsets = new unsigned char [numofvals];  
	  if (SNAPPY == -1){
		char buffer1[indicessize];
		result =  fread(buffer1,indicessize,1,f1);
		string output;
		snappy::Uncompress(buffer1, indicessize, &output);
		copy(&output[0], &output[0]+(sizeof(unsigned char)*numofvals), reinterpret_cast<char*>(offsets));	      
	  }
	  else
		result =  fread(offsets,1,numofvals,f1);  
   
	  this->char_offsets_cache[position] = offsets;
	  return 0;
	}
	else{
		offsets = this->char_offsets_cache[position];
		}
	return 1;
}
