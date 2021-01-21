<b>Compile readdiff.cpp</b>: <br>
g++ -std=c++11 -o read readdiff.cpp -lorc -lzstd -lprotobuf -lsnappy -llz4 -lz -lprotoc -lpthread -lparquet <br>

The above libs are dependencies for Apache ORC C++ and Arrow Parquet C++ implementations 
msgpack.hpp (https://github.com/msgpack/msgpack-c/tree/cpp_master) is also a dependency.


It implements all read operations (full scan, filtered scan, range query, random row look-up) for all the formats that are used in the paper.

Syntax:

<b>Full scan</b> <br>
./read file.diff 0 (scans the 0 (first) column of the adaptively encoded file) <br>
./read file.localdiff 0 (scans the 0 (first) column of the local dictionary encoded file)<br>
./read file.parquet 0 (scans the 0 (first) column of the parquet file)<br>
./read file.orc 0 (scans the 0 (first) column of the ORC file)<br>
./read file.orderedglob 0 (scans the 0 (first) column of the order preserving global dictionary file)<br>
./read file.unorderedglob 0 (scans the 0 (first) column of a global dictionary encoded file)<br>
./read file.mostlyordered 0 (scans the 0 (first) column of a file encoded with MOPs)<br>
./read file.indirect 0 (scans the 0 (first) column of an indirect coded file)<br>
<br>
<br>
<b>Filtered scan</b><br>
./read file.diff 0 "value" 1 1 (searches "value" in 0 (first column) and returns the count, as for the 2 flags the first enables zone-maps, the second enables diff dict tighter min/max values)<br>
The same command with different file endings (like above) processes the other file formats.<br>

<b>Range</b><br>
./read file.diff 0 "value" ">" 1 1 (same as above but with the range symbol added before the 2 flags)<br>

<b>Random Access</b><br>
<br>
./read file.diff 0 1000 (returns the value of row 1000)<br>

All the above run also on snappy compressed adaptive files if the file is named file.diffsnappy <br>

<br>


<b>Compile join.cpp:</b> <br>
g++ -std=c++11 -o join join.cpp<br>

./join file1.diff file2.diff 0 1 (joins first col of file1.diff to the 2nd of file2.diff and returns the count)<br>
./join file1.glob file2.glob 0 1 (same as above but with global encoded files)<br>
./join file1.local file2.local 0 1 (same as above but with local encoded files)<br>
<br>

<b>global_encoding.py</b><br>



<br>
Write files with Python libs<br>
dependencies: pandas, pyarrow<br>
<br>
import global_encoding <br>
global_encoding.write_parquet(df, "file.parquet", -1) #writes parquet default parquet file<br>
global_encoding.write_parquet(df, "file.parquet", 65535) # each parquet block consists of 64K records<br>
global_encoding.write_global(df,"file.unorderedglob",0,65535) # encodes with global dictionaries<br>
global_encoding.write_global(df,"file.unorderedglob",1,65535) # encodes with order preserving dictionaries<br>
global_encoding.write_indirect(df,"file.unindirect",0,65535) # encodes with global dictionaries and indirect coding<br>
global_encoding.write_indirect(df,"file.indirect",1,65535) # encodes with order preserving dictionaries and indirect coding<br>
global_encoding.mostlyordered(df,"file.mostlyordered",65535,1,0.1,20); # encode with MOP's 1, 0.1 and 20 are the MOP's parameters pitch, lookahead and batch size<br>

<br>
<b>Write adaptive dictionaries using fastparquet</b>:<br>
from fastparquet import write<br>
%time write("file.diff", df, row_group_offsets=65535)<br>
%time write("file.diffsnappy", df, row_group_offsets=65535, compression='SNAPPY')<br>
where df is a pandas dataframe and row_group_offsets is the size of a row group.<br>


<br>
<b>Write adaptive dictionaries with C (compress.cpp)</b>:<br>
Compilation: g++ -std=c++11 -o compress compress.cpp <br>
Execution: ./compress datafile.csv datafile.diff <br>
compression.cpp works with single column files with a string attribute and was used to compare encoding times using C with ORC and Parquet C++ implementation. 

The above Python implementation is more complete and works also with dataframes with many attributes. <br>

<br>

Local dictionaries are not implemented alone. If someone wants to encode with local dictionaries in python's implementation in file fastparquet/writer.py before line 682 a hard coded `diffdict = 0` statement should be added.
In C++ implementation in compress.cpp file the same should be added before line 136. Doing this the cost function can be disabled.
Also commenting lines 91-134 in C++ and 643-671 in python will remove the cost function calculations and its overheads. 
With `diffdict = 1` it is possible to run it without the cost function using only differential encoding.

The datasets used in the experiments are too big and could not be included in this repo. However, the data used is accessible through the links provided in the footnote of the paper and the exact datasets are reproducible either directly or given the explanations in the paper. 







