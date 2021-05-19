// We can use stl container list as a double
// ended queue to store the cache keys, with
// the descending time of reference from front
// to back and a set container to check presence
// of a key. But to fetch the address of the key
// in the list using find(), it takes O(N) time.
// This can be optimized by storing a reference
//	 (iterator) to each key in a hash map.
#include <bits/stdc++.h>
using namespace std;

class LRUCache {
	// store keys of cache
	list<int> dq1;
	list<int> dq;
	int ind=0;
	// store references of key in cache
	unordered_map<int, vector <string>> ma1;
	unordered_map<long int, int> ma;
	int csize; // maximum capacity of cache

public:
	LRUCache(int);
	void refer(int, vector <string>);
	int  get(int x, vector <string> (&));

};

// Declare the size
LRUCache::LRUCache(int n)
{
	csize = n;
}

// Refers key x with in the LRU cache
void LRUCache::refer(int x, vector <string> buffer)
{
	// not present in cache
	if (ma.find(x) == ma.end()) {
		// cache is full
		if (dq.size() == csize) {
			// delete least recently used element
			int last = dq.back();
			int last2 = dq1.back();
			// Pops the last element
			dq.pop_back();
			dq1.pop_back();
			// Erase the last
			ma.erase(last);
			ma1.erase(last2);
		}
		dq.push_front(x);
		int last = dq.back();
		ma[x] = dq1[0];
	    ma1[ma[x]] = buffer;
	}	
}


int LRUCache::get(int x, vector <string> (&buf))
{
	if (ma.find(x) == ma.end())
	    return -1;
	buf = ma1[ma[x]];
	return 0;
	 
}




