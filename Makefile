CXX=g++
CXXFLAGS=-std=c++11 -g -ggdb -O0
INCLUDE=-I../../flatbuffers/include -I../root/include
LIBS=-lprotobuf -L../root/lib -lparquet -larrow
PROTOC=protoc
FLATC=../../flatbuffers/build/flatc

all: make_sample_tile query_sample_tile query_sample_tile_pbf convert_fb_to_parquet query_sample_tile_parquet
clean:
	rm -f make_sample_tile query_sample_tile query_sample_tile_pbf convert_fb_to_parquet query_sample_tile_parquet \
		histogram_tile.pb.h histogram_tile.pb.cc \
		histogram_tile_generated.h

make_sample_tile: make_sample_tile.cpp histogram_tile.pb.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

query_sample_tile: query_sample_tile.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

query_sample_tile_pbf: query_sample_tile_pbf.cpp histogram_tile.pb.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

convert_fb_to_parquet: convert_fb_to_parquet.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

query_sample_tile_parquet: query_sample_tile_parquet.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

histogram_tile.pb.cc: histogram_tile.proto
	$(PROTOC) --cpp_out=. $<

histogram_tile_generated.h: histogram_tile.fbs
	$(FLATC) -c $<

make_sample_tile: histogram_tile_generated.h
query_sample_tile: histogram_tile_generated.h
convert_fb_to_parquet: histogram_tile_generated.h

.PHONY: all
