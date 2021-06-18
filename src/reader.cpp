//g++-10 -O3 -std=c++20 "-fcoroutines" -o test test.cpp  -lsnappy
// g++-10  -std=c++20  "-fcoroutines" -o test2 test.cpp  -lsnappy -O3 -freorder-blocks-algorithm=simple
#include "arcade.h"
using namespace Arcade;

Generator <int> ArcadeReader::equi_filter(char* filename, char*** &cols, int col_num, char* val, int* retcols, int colnum){

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
		this->mcaches.SNAPPY = 1;
	else
		this->mcaches.SNAPPY = 0;
  
	int join1 = col_num;
	int blocknum = -1;

	int rows = 0;
	 cols = (char***)malloc(colnum*sizeof(char **));
	 for(int mal=0; mal < colnum; mal++) cols[mal] = (char**)malloc(65535*sizeof(char*));


	while (totalcount1 < fileheader1.numofvals){
		 rows = processing.filter_page(f1, cols, blocknum, initstep1, join1, fileheader1, totalcount1,  value, retcolumns, dict_cache, global_len, offset, data, this->mcaches);
		 co_yield rows;
	}
	delete [] data;
	for(int mal=0; mal < colnum; mal++) free(cols[mal]);
	free(cols);
	fclose(f1);
	}
	
 //TODO free caches
}


Generator <int> ArcadeReader::random_access(char* filename, char*** &cols, int* retcols, int colnum,  int* rowids, int rowidsnum){

	    
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
			this->mcaches.SNAPPY = 1;
		else
			this->mcaches.SNAPPY = 0;
	
		result =  fread(data,(fileheader1.numofblocks+1)*8,1,f1);
	
		int temp_blocknum = -1;
		int rows_per_block = 0;
		int start_of_block = 0;
        
		for (int i = 0; i < rowidsnum; i++){
		
			blocknum = (rowids[i])/data[fileheader1.numofblocks];
			if (blocknum != temp_blocknum and temp_blocknum != -1){
				rowids[i] = (rowids[i])%data[fileheader1.numofblocks];
				dict_cache.clear();
				co_yield processing.get_column_value(f1,cols, temp_blocknum, initstep1, fileheader1, header1, columns, rowids+start_of_block, rows_per_block, -1, data, dict_cache, this->mcaches);   
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
				co_yield processing.get_column_value(f1,cols, blocknum, initstep1, fileheader1, header1, columns, rowids+start_of_block, rows_per_block, -1, data, dict_cache, this->mcaches);   
			}
		}
	   delete [] data;
	   for(int mal=0; mal < colnum; mal++) free(cols[mal]);
		free(cols);
		fclose(f1);
		//cout << data[blocknum] << endl;

}


Generator <int> ArcadeReader::scan(char* filename, char*** &cols, int* retcols, int colnum){	
 
	//TODO if file changes free caches
	//colnum = sizeof(retcols) / sizeof(retcols[0]);
	vector<vector<string>> globaldict(colnum);
	vector<int> retcolumns(retcols, retcols + colnum);
	int max = -1;
	if (colnum>0)
		max = *max_element(retcolumns.begin(), retcolumns.end());
	int totalcount1=0;
	FILE *f1;
	int blocknum = -1;
	f1 = fopen(filename,"rb");
	vector<string> parquetvalues1;

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
		this->mcaches.SNAPPY = 1;
	else
		this->mcaches.SNAPPY = 0;
  
	int blocknum = -1;
	int blockn = 0;

	int rows = 0;
	 cols = (char***)malloc(colnum*sizeof(char **));
	 for(int mal=0; mal < colnum; mal++) cols[mal] = (char**)malloc(65535*sizeof(char*));

	while (totalcount1 < fileheader1.numofvals)
		 co_yield processing.scan_page(f1, cols, blocknum, initstep1, fileheader1, blockn, totalcount1, retcolumns, globaldict, global_len, this->mcaches);
	 
	for(int mal=0; mal < colnum; mal++) free(cols[mal]);
	free(cols);
	fclose(f1);

	globaldict.clear();
	/*for (vector <string> &lala: globaldict)
		lala.clear();
	globaldict.clear();
	globaldict.resize(0);
	globaldict.shrink_to_fit();*/
	}
	

}


int Arcade::print_columns(char*** &cols, int rows, int coln){
             for (int i=0; i<rows; i++){
     		     for (int j=0; j<coln; j++){
     		      if (j == coln-1)
        			printf("%s", cols[j][i]);
        		  else {
        		  printf("%s", cols[j][i]);
        		  cout << "\033[1;31m|\033[0m";
        		  }
        		  
    			}
    			if (coln>0) cout << endl;
    			//break;	
     		}
     		
     	

return 1;
}
