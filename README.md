# parquet4seastar

parquet4sestar is an implementation of the Apache Parquet format
for Seastar projects.

The project consists mainly of the `parquet4seastar` library.
See `examples/example.cc` for a usage example of the basic library
functionality, which is writing/reading `.parquet` files in batches of
(definition level, repetition level, value) triplets to/from chosen columns.
See `src/cql_reader.cc` and `apps/main.cc` for a usage of the record reader
functionality (consuming assembled records by providing callbacks for each part
of the record).

The `apps` directory contains a `parquet2cql` tool which can be used to
print `.parquet` files to CQL. It can be invoked with:

```
BUILDDIR/apps/parquet2cql/parquet2cql --table TABLENAME --pk ROW_INDEX_COLUMN_NAME --file PARQUET_FILE_PATH
```

This project is not battle-tested and should not yet be considered stable.
The interface of the library is subject to change.

## Build instructions

The library follows standard CMake practices.

Install the dependencies: GZIP, Snappy and Thrift >= 0.11. 
```
pushd /tmp
git clone https://github.com/scylladb/seastar.git
popd

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DSEASTAR_PATH=/tmp/seastar  -DENABLE_PARQUET4SEASTAR_TEST=ON
make -j 30
```

tests and apps will be then be built in `build`.

The library can then be optionally installed with `make install` or consumed
directly from the build directory. Use of CMake for consuming the library
is recommended.

GZIP and Snappy are the only compression libraries used by default.
Support for other compression libraries used in Parquet files
can be added by merging #2.

```run test
cd tests/
./byte_stream_split_test          
./compression_test                
./cql_reader_test                
./delta_binary_packed_test       
./delta_length_byte_array_test   
./file_writer_test               
./rle_encoding_test              
./thrift_serdes_test_test               
./column_chunk_writer_test       
./cql_reader_alltypes_test       
./delta_byte_array_test          
./dictionary_encoder_test        
```

```testcase
byte_stream_split_test          1/1
compression_test                3/3
cql_reader_test                 1/1
delta_binary_packed_test        4/4
delta_length_byte_array_test    1/1
file_writer_test                1/1
rle_encoding_test               12/12
thrift_serdes_test_test         1/1       
column_chunk_writer_test        1/1
cql_reader_alltypes_test        6/6
delta_byte_array_test           1/1
dictionary_encoder_test         2/2
```