#include "hps/hps.h"
#include "snappy.h"

using namespace std;

class Caches {

    size_t result;
    unordered_map<long int, vector<string>> values_cache;
    unordered_map<long int, unsigned short *> short_offsets_cache;
    unordered_map<long int, unsigned int *> int_offsets_cache;
    unordered_map<long int, unsigned char *> char_offsets_cache;

public:
    bool SNAPPY;

    int get_values(FILE *, vector<string> *&, long int, int);

    int get_short_offsets(FILE *, long int, int, int, unsigned short *(&));

    int get_int_offsets(FILE *, long int, int, int, unsigned int *(&));

    int get_char_offsets(FILE *, long int, int, int, unsigned char *(&));
};
