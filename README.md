# sigmod2021


Compile readdiff.cpp: 
g++ -std=c++11 -o read readdiff.cpp -lorc -lzstd -lprotobuf -lsnappy -llz4 -lz -lprotoc -lpthread -lparquet

Dependencies: all the libs that are in the above command + msgpack.hpp (https://github.com/msgpack/msgpack-c/tree/cpp_master)

It implements all read operations (full scan, filtered scan, range query, random row look-up) for all the formats that are mentioned in the paper.

Syntax:

Full scan
./read file.diff 0 (scans the 0 (first) column of the adaptively encoded file)
./read file.localdiff 0 (scans the 0 (first) column of the local dictionary encoded file)
./read file.parquet 0 (scans the 0 (first) column of the parquet file)
./read file.orc 0 (scans the 0 (first) column of the ORC file)
./read file.orderedglob 0 (scans the 0 (first) column of the order preserving global dictionary file)
./read file.unorderedglob 0 (scans the 0 (first) column of a global dictionary encoded file)
./read file.mostlyordered 0 (scans the 0 (first) column of a file encoded with MOPs)
./read file.indirect 0 (scans the 0 (first) column of an indirect coded file)
./

Filtered scan
./read file.diff 0 "value" 1 1 (searches "value" in 0 (first column) and returns the count, as for the 2 flags the first enables zone-maps, the second enables diff dict tighter min/max values)
The same command with different file endings (like above) processes the other file formats.

Range
./read file.diff 0 "value" ">" 1 1 (same as above but with the range symbol added before the 2 flags)

Random Access

./read file.diff 0 1000 (returns the value of row 1000)

./join file1.diff file2.diff 0 1 (join the first column of the first file to the 2 column of the 2nd file, both files are encoded with differential dictionaries)
./join file1.local file2.local 0 1 (same as above but with local dictionaries)




Compile join.cpp: 
g++ -std=c++11 -o join join.cpp
