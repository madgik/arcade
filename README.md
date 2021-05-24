# Arcade

Arcade is a compressed columnar file format, with an implementation of basic operations without or partial decompression. 
Its first version supports string datatypes, compression, decompression, random row look-up, equi filter evaluation.

## Documentation

### Installation

### Examples

#### Write

#### Read

## Features 

The following features are either implemented or will be added in next versions

- Data types
    - [x] String data types
    - [ ] Other datatypes (e.g., integers, floats...) 
- Write Operations
    - [x] Encode multi-column CSV files
    - [x] Snappy Compression
    - [ ] ZStandard Compression
- Read Operations
    - [x] Attributes projection
    - [x] Equi Filters (a single equi filter at time)
    - [x] Full Materialized Scans
    - [x] Multiple random row look-up
    - [ ] Joins between compressed files
    - [ ] Range filters
    - [ ] Sort
    - [ ] Group by's
    - [ ] Aggregations
    - [ ] Multiple equi and range fiters at a time
- Distributed features
    - [ ] Partitioning/broadcast of compressed file
    - [ ] Distributed operations (e.g., parallel scans, distributed join etc.)
- APIs
  - [x] C API
  - [ ] Python API 

## Papers

- "Adaptive Compression for Fast Scans on String Columns", <br>
Yannis Foufoulas, Lefteris Sidirourgos, Lefteris Stamatogiannakis, Yannis Ioannidis (University of Athens - MaDgIK Group), <br>
In Proceedings of the 2021 ACM SIGMOD International Conference on Management of Data
