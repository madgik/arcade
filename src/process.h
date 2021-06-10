#include "cache.h"

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


class Processing{
  size_t result;
  public:
    int get_column_value(FILE *, char*** &, int, long int, struct fileH, struct D, vector <int>, int*, int, int, long*, vector <unordered_map <int, vector <string>*>> &, Caches &);
    int filter_page(FILE *, char*** &, int &, unsigned long &, int, struct fileH, int &, string &, vector <int>, vector <unordered_map <int, vector <string>*>> &, int &, int &, long* (&), Caches &);
    int scan_page(FILE *, char*** &, int &, unsigned long &, struct fileH, int &, int &, vector <int>, vector<vector <string>> &, int &, Caches &);
};