/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

#include <parquet4seastar/exception.hh>
#include <parquet4seastar/file_reader.hh>
#include <seastar/core/seastar.hh>

namespace parquet4seastar {

seastar::future<std::unique_ptr<format::FileMetaData>> file_reader::read_file_metadata(IReader& file) {
    auto size = co_await file.size();
    if (size < 8) {
        throw parquet_exception::corrupted_file(seastar::format("File too small ({}B) to be a parquet file", size));
    }
    // Parquet file structure:
    // ...
    // File Metadata (serialized with thrift compact protocol)
    // 4-byte length in bytes of file metadata (little endian)
    // 4-byte magic number "PAR1"
    // EOF
    auto footer = co_await file.dma_read_exactly(size - 8, 8);
    if (std::memcmp(footer.get() + 4, "PARE", 4) == 0) {
        throw parquet_exception("Parquet encryption is currently unsupported");
    }
    if (std::memcmp(footer.get() + 4, "PAR1", 4) != 0) {
        throw parquet_exception::corrupted_file("Magic bytes not found in footer");
    }
    uint32_t metadata_len;
    std::memcpy(&metadata_len, footer.get(), 4);
    if (metadata_len + 8 > size) {
        throw parquet_exception::corrupted_file(seastar::format(
          "Metadata size reported by footer ({}B) greater than file size ({}B)", metadata_len + 8, size));
    }
    auto serialized_metadata = co_await file.dma_read_exactly(size - 8 - metadata_len, metadata_len);
    auto deserialized_metadata = std::make_unique<format::FileMetaData>();
    deserialize_thrift_msg(serialized_metadata.get(), serialized_metadata.size(), *deserialized_metadata);
    co_return deserialized_metadata;
}

seastar::future<file_reader> file_reader::open(std::unique_ptr<IReader> file) {
    assert(file != nullptr);
    try {
        auto metadata = co_await read_file_metadata(*file);
        co_return file_reader(std::move(file), std::move(metadata));
    } catch (const std::exception& e) {
        throw parquet_exception(seastar::format("Could not open parquet file for reading: {}", e.what()));
    }
}

namespace {

seastar::future<std::unique_ptr<format::ColumnMetaData>> read_chunk_metadata(std::unique_ptr<IPeekableStream> stream) {
    assert(stream != nullptr);
    auto column_metadata = std::make_unique<format::ColumnMetaData>();
    const auto read_success = co_await read_thrift_from_stream(*stream, *column_metadata);
    if (not read_success) {
        throw parquet_exception::corrupted_file("Could not deserialize ColumnMetaData: empty stream");
    }
    co_return column_metadata;
}

}  // namespace

/* ColumnMetaData is a structure that has to be read in order to find the beginning of a column chunk.
 * It is written directly after the chunk it describes, and its offset is saved to the FileMetaData.
 * Optionally, the entire ColumnMetaData might be embedded in the FileMetaData.
 * That's what the documentation says. However, Arrow always assumes that ColumnMetaData is always
 * present in the FileMetaData, and doesn't bother reading it from it's required location.
 * One of the tests in parquet-testing also gets this wrong (the offset saved in FileMetaData points to something
 * different than ColumnMetaData), so I'm not sure whether this entire function is needed.
 */
template <format::Type::type T>
seastar::future<column_chunk_reader<T>> file_reader::open_column_chunk_reader_internal(uint32_t row_group,
                                                                                       uint32_t column) {
    assert(column < raw_schema().leaves.size());
    assert(row_group < metadata().row_groups.size());
    if (column >= metadata().row_groups[row_group].columns.size()) {
        // return seastar::make_exception_future<column_chunk_reader<T>>(
        //                 parquet_exception::corrupted_file(seastar::format(
        //                         "Selected column metadata is missing from row group metadata: {}",
        //                         metadata().row_groups[row_group])));
        co_return co_await seastar::make_exception_future<column_chunk_reader<T>>(parquet_exception::corrupted_file(
          seastar::format("Selected column metadata is missing from row group metadata")));
    }
    const format::ColumnChunk& column_chunk = metadata().row_groups[row_group].columns[column];
    const reader_schema::raw_node& leaf = *raw_schema().leaves[column];
    assert(not column_chunk.__isset.file_path);
    std::unique_ptr<format::ColumnMetaData> column_metadata;
    if (column_chunk.__isset.meta_data) {
        column_metadata = std::make_unique<format::ColumnMetaData>(column_chunk.meta_data);
    } else {
        auto peek_stream = file().make_peekable_stream(column_chunk.file_offset, {8192, 16});
        column_metadata = co_await read_chunk_metadata(std::move(peek_stream));
    }
    size_t file_offset = column_metadata->__isset.dictionary_page_offset ? column_metadata->dictionary_page_offset
                                                                         : column_metadata->data_page_offset;
    auto peek_stream = file().make_peekable_stream(file_offset, column_metadata->total_compressed_size, {8192, 16});
    co_return column_chunk_reader<T>{
      page_reader{std::move(peek_stream)}, column_metadata->codec, leaf.def_level, leaf.rep_level,
      (leaf.info.__isset.type_length ? std::optional<uint32_t>(leaf.info.type_length) : std::optional<uint32_t>{})};
}

template <format::Type::type T>
seastar::future<column_chunk_reader<T>> file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column) {
    return open_column_chunk_reader_internal<T>(row_group, column)
      .handle_exception([column, row_group](std::exception_ptr eptr) {
          try {
              std::rethrow_exception(eptr);
          } catch (const std::exception& e) {
              return seastar::make_exception_future<column_chunk_reader<T>>(parquet_exception(
                seastar::format("Could not open column chunk {} in row group {}: {}", column, row_group, e.what())));
          }
      });
}

template seastar::future<column_chunk_reader<format::Type::INT32>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::INT64>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::INT96>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::FLOAT>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::DOUBLE>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::BOOLEAN>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);

}  // namespace parquet4seastar
