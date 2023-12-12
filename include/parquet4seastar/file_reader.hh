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

#pragma once

#include <parquet4seastar/column_chunk_reader.hh>
#include <parquet4seastar/reader_schema.hh>
#include <seastar/core/file.hh>

namespace parquet4seastar {

class IReader
{
   public:
    virtual ~IReader() = default;
    virtual auto close() -> seastar::future<> = 0;
    virtual auto size() -> seastar::future<size_t> = 0;
    virtual auto dma_read_exactly(uint64_t pos, size_t len) -> seastar::future<seastar::temporary_buffer<uint8_t>> = 0;

    virtual auto make_peekable_stream(uint64_t offset, uint64_t len, seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> = 0;
    virtual auto make_peekable_stream(uint64_t offset, seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> = 0;

    virtual auto make_peekable_stream(seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> = 0;
};

class SeastarFile : public IReader
{
    seastar::file _file;

   public:
    explicit SeastarFile(seastar::file file) : _file(std::move(file)) {}

    auto close() -> seastar::future<> override { return _file.close(); }
    auto size() -> seastar::future<size_t> override { return _file.size(); }
    auto dma_read_exactly(uint64_t pos, size_t len) -> seastar::future<seastar::temporary_buffer<uint8_t>> override {
        return _file.dma_read_exactly<uint8_t>(pos, len);
    }

    auto make_peekable_stream(seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> override {
        auto stream = peekable_stream(seastar::make_file_input_stream(_file, options));
        return std::make_unique<peekable_stream>(std::move(stream));
    }

    auto make_peekable_stream(uint64_t offset, uint64_t len, seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> override {
        auto stream = peekable_stream(seastar::make_file_input_stream(_file, offset, len, options));
        return std::make_unique<peekable_stream>(std::move(stream));
    }

    auto make_peekable_stream(uint64_t offset, seastar::file_input_stream_options options = {})
      -> std::unique_ptr<IPeekableStream> override {
        auto stream = peekable_stream(seastar::make_file_input_stream(_file, offset, options));
        return std::make_unique<peekable_stream>(std::move(stream));
    }
};

class file_reader
{
    std::unique_ptr<IReader> _file;
    std::unique_ptr<format::FileMetaData> _metadata = nullptr;
    std::unique_ptr<reader_schema::schema> _schema = nullptr;
    std::unique_ptr<reader_schema::raw_schema> _raw_schema = nullptr;

    static seastar::future<std::unique_ptr<format::FileMetaData>> read_file_metadata(IReader& file);
    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>> open_column_chunk_reader_internal(uint32_t row_group, uint32_t column);

   public:
    file_reader() = delete;
    file_reader(std::unique_ptr<IReader> file, std::unique_ptr<format::FileMetaData> meta)
        : _file(std::move(file)), _metadata(std::move(meta)){};

    // The entry point to this library.
    static seastar::future<file_reader> open(std::unique_ptr<IReader> file);

    seastar::future<> close() { return _file->close(); };

    auto file() const noexcept -> IReader& { return *_file; }

    const format::FileMetaData& metadata() const { return *_metadata; }
    // The schemata are computed lazily (not on open) for robustness.
    // This way lower-level operations (i.e. inspecting metadata,
    // reading raw data with column_chunk_reader) can be done even if
    // higher level metadata cannot be understood/validated by our reader.
    const reader_schema::raw_schema& raw_schema() {
        if (!_raw_schema) {
            _raw_schema =
              std::make_unique<reader_schema::raw_schema>(reader_schema::flat_schema_to_raw_schema(metadata().schema));
        }
        return *_raw_schema;
    }
    const reader_schema::schema& schema() {
        if (!_schema) {
            _schema = std::make_unique<reader_schema::schema>(reader_schema::raw_schema_to_schema(raw_schema()));
        }
        return *_schema;
    }

    template <format::Type::type T>
    seastar::future<column_chunk_reader<T>> open_column_chunk_reader(uint32_t row_group, uint32_t column);
};

extern template seastar::future<column_chunk_reader<format::Type::INT32>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::INT64>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::INT96>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::FLOAT>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::DOUBLE>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::BOOLEAN>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::BYTE_ARRAY>> file_reader::open_column_chunk_reader(
  uint32_t row_group, uint32_t column);
extern template seastar::future<column_chunk_reader<format::Type::FIXED_LEN_BYTE_ARRAY>>
file_reader::open_column_chunk_reader(uint32_t row_group, uint32_t column);

}  // namespace parquet4seastar
