# include "csv.h"

int main(){
  io::CSVReader<3> in("/home/openaire/phd/datasets/yelp/friendship2.tsv");
  in.read_header(io::ignore_missing_column, "vendor", "size", "speed");
  std::string vendor; int size; double speed;
  while(in.read_row(vendor, size, speed)){
    // do stuff with the data
  }
}

