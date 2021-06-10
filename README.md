# Arcade

Arcade is a compressed columnar file format, with an implementation of basic operations without or partial decompression. 
Its first Alpha version supports string datatypes, compression, decompression, random row look-up, equi filter evaluation.

## Documentation

### Installation

Dependencies:

- g++ version 10 or newer with c++20 and coroutines
- git

Install
        git clone https://github.com/madgik/arcade.git
        cd arcade/src
        make depend # install dependencies
        make # creates arcade library (libarcade.a)
        make runner #creates runner executable to use for testing all features

### Examples

Arcade currently in its Alpha version is schemaless. There are no attribute names and types. All the input is considered as strings. 
Column names are numbered 0,1,2...

#### Write
Supports ',' delimited files.


#### Read

#### runner executable (src/test.cpp source code)

insert to terminal:

        C input.csv output.arcade 10 20 0,1 # compresses columns 0 and 1 and rows from 10 to 20 from input file and creates output.arcade
        F output.arcade 1 value 0,1 # filter scans column 1 from file output.arcade with value and projects columns 0 and 1
        R output.arcade 15,100 0,1 # projects columns 0 and 1 with row numbers 15 and 100
        S output.arcade 0,1 # projects columns 0 and 1 of file output.arcade


## Features 

The following features are either implemented or will be added in next versions

- <b>Data types</b>
    - [x] String data types
    - [ ] Other datatypes (e.g., integers, floats...) 
- <b>Write Operations</b>
    - [x] Encode multi-column CSV files
    - [x] Snappy Compression (currently for dictionary values)
    - [ ] ZStandard Compression
- <b>Read Operations</b>
    - [x] Attributes projection
    - [x] Equi Filters (single equi filter at time)
    - [x] Full Materialized Scans
    - [x] Multiple random row look-up
    - [ ] Range filters
    - [ ] Joins between compressed files
    - [ ] Sort
    - [ ] Aggregations
    - [ ] Group by's
- <b>APIs</b>
    - [x] C++ API
    - [ ] C API
    - [ ] Python API 
- <b>Distributed features</b>
    - [ ] Partitioning/broadcast of compressed file
    - [ ] Distributed integration (e.g., parallel scans, distributed join etc.)

## Papers

- <b>"Adaptive Compression for Fast Scans on String Columns"</b>, <br>
Yannis Foufoulas, Lefteris Sidirourgos, Lefteris Stamatogiannakis, Yannis Ioannidis (University of Athens - MaDgIK Group), <br>
In Proceedings of the 2021 ACM SIGMOD International Conference on Management of Data
