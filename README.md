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

- <b>Data types</b>
    - [x] String data types
    - [ ] Other datatypes (e.g., integers, floats...) 
- <b>Write Operations</b>
    - [x] Encode multi-column CSV files
    - [x] Snappy Compression
    - [ ] ZStandard Compression
- <b>Read Operations</b>
    - [x] Attributes projection
    - [x] Equi Filters (single equi filter at time)
    - [x] Full Materialized Scans
    - [x] Multiple random row look-up
    - [ ] Range filters
    - [ ] Joins between compressed files
    - [ ] Sort
    - [ ] Group by's
    - [ ] Aggregations
    - [ ] Multiple equi and range fiters
    - [ ] Multiple operators at a time
- <b>Distributed features</b>
    - [ ] Partitioning/broadcast of compressed file
    - [ ] Distributed operations (e.g., parallel scans, distributed join etc.)
- <b>APIs</b>
  - [x] C API
  - [ ] Python API 

## Papers

- <b>"Adaptive Compression for Fast Scans on String Columns"</b>, <br>
Yannis Foufoulas, Lefteris Sidirourgos, Lefteris Stamatogiannakis, Yannis Ioannidis (University of Athens - MaDgIK Group), <br>
In Proceedings of the 2021 ACM SIGMOD International Conference on Management of Data
