#include "arcade.h"

using namespace Arcade;
int extractattributes(std::string s, int*& columns) {
  
  string parsed;
  stringstream input_stringstream(s);
  int c = 0;
  while (getline(input_stringstream,parsed,',')){
     if (parsed == "count") break; 
     columns[c] = atoi(&parsed[0]);
     c++;
}  
  return c;
}


int compress(ArcadeWriter &arcadewriter){
	char* filename = new char[100];
	char*  outfile = new char[100];
	int* retcolumns = new int[1000];
	char* retcols = new char[1000];
	int retcolslen;
	int init;
	int row_count;
	double duration = 0.0;
    std::clock_t start;
	cin >> filename >> outfile >> init >> row_count >> retcols;
	retcolslen = extractattributes(retcols, retcolumns);
	start = std::clock();
	arcadewriter.compress(filename, outfile, init, row_count, retcolumns, retcolslen);
	duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
	cout << "Compressed " << row_count << " rows in " << duration << " seconds" << endl;
	delete [] filename;
	delete [] outfile;
	delete [] retcolumns;
	delete [] retcols;
	return 1;
}


int filter(ArcadeReader &arcadereader){
    char*** cols;
    int col_num;
    char* val = new char[200];
    char* filename = new char[100];
    int* retcolumns = new int[1000];
	char* retcols = new char[1000];
	int retcolslen;
    int row_count = 0;
    int rows;
    double duration = 0.0;
    std::clock_t start;
	cin >> filename >> col_num >> val >> retcols;
    retcolslen = extractattributes(retcols, retcolumns);
    start = std::clock();
    auto gen = arcadereader.equi_filter(filename,cols, col_num, val, retcolumns,retcolslen);
    rows = 0;
    while (gen){
        rows = gen();
        if (rows == -2) {cout << "The file is not a valid arcade file" << endl; break;}
        row_count+=rows;
        print_columns(cols, rows, retcolslen);
    }
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << "Returned " << row_count << " rows in " << duration << " seconds" << endl;

	delete [] filename;
	delete [] retcolumns;
	delete [] retcols;
	delete [] val;
	return 1;
}

int scan(ArcadeReader &arcadereader){
    char*** cols;
    char* filename = new char[100];
    int* retcolumns = new int[1000];
	char* retcols = new char[1000];
	int retcolslen;
    int row_count;
    int rows;
    double duration = 0.0;
    std::clock_t start;
	cin >> filename >> retcols;
	retcolslen = extractattributes(retcols, retcolumns);
	start = std::clock();
	auto gen = arcadereader.scan(filename,cols,retcolumns,retcolslen);
	row_count = 0;
	rows = 0;
	while (gen){
		rows = gen();
		if (rows == -2) {cout << "The file is not a valid arcade file" << endl; break;}
		row_count += rows;
		print_columns(cols, rows, retcolslen);
	}
	duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
	cout << "Returned " << row_count << " rows in " << duration << " seconds" << endl;
	delete [] filename;
	delete [] retcolumns;
	delete [] retcols;
	return 1;
}

int random_access(ArcadeReader &arcadereader){
	char* filename = new char[100];
	int col_num;
	int count_rows = 0;
	char* retcols = new char[1000];
	int* retcolumns = new int[1000];
	char* rids = new char[1000];
	int* rowids = new int[1000];
	int rowidsnum = 0;
	int rows = 0;
	char*** cols;
	int retcolslen = 0;
	cin >> filename >> rids >> retcols;
    retcolslen = extractattributes(retcols, retcolumns);
    rowidsnum = extractattributes(rids, rowids);
    double duration = 0.0;
    std::clock_t start = std::clock();
    auto gen = arcadereader.random_access(filename,cols, retcolumns,retcolslen, rowids, rowidsnum);
    while (gen){
        rows = gen();
        if (rows == -2) {cout << "The file is not a valid arcade file" << endl; break;}
        count_rows+=rows;
        print_columns(cols, rows, retcolslen);
    }
   
    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
    cout << "Returned " << count_rows << " rows in " << duration << " seconds" << endl;
    delete [] filename;
	delete [] retcolumns;
	delete [] retcols;
	delete [] rids;
	delete [] rowids;
	
	return 1;
}

int main(){
  char querytype; // C for compress, F for filter, R for random rows look-up, S for full materialized scan 
  ArcadeReader arcadereader;
  ArcadeWriter arcadewriter;
  while (true){
    cin >> querytype;
    switch (querytype)
	{
    	case 'C':
			compress(arcadewriter);
			break;
    	case 'F':
    		filter(arcadereader);
      		break;
        case 'R':
            random_access(arcadereader);
        	break;
        case 'S':
            scan(arcadereader);
            break;
    	default:
    	   cout << "not valid query type" << endl;
    }
  }
    
}


