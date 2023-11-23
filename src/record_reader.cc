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

#include <parquet4seastar/file_reader.hh>
#include <parquet4seastar/overloaded.hh>
#include <parquet4seastar/record_reader.hh>

namespace parquet4seastar::record {
map_reader::map_reader(const reader_schema::map_node& node, std::unique_ptr<field_reader> key_reader,
                       std::unique_ptr<field_reader> value_reader)
    : _key_reader(std::move(key_reader)),
      _value_reader(std::move(value_reader)),
      _def_level{node.def_level},
      _rep_level{node.rep_level},
      _name(node.info.name) {}

struct_reader::struct_reader(const reader_schema::struct_node& node, std::vector<field_reader>&& readers)
    : _readers(std::move(readers)), _def_level{node.def_level}, _rep_level{node.rep_level}, _name(node.info.name) {}

list_reader::list_reader(const reader_schema::list_node& node, std::unique_ptr<field_reader> reader)
    : _reader(std::move(reader)), _def_level{node.def_level}, _rep_level{node.rep_level}, _name(node.info.name) {}

optional_reader::optional_reader(const reader_schema::optional_node& node, std::unique_ptr<field_reader> reader)
    : _reader(std::move(reader)), _def_level{node.def_level}, _rep_level{node.rep_level}, _name(node.info.name) {}

namespace {

auto make_reader(file_reader& fr, const reader_schema::primitive_node& node, int row_group) -> seastar::future<field_reader> {
    co_return std::visit(
      [&, row_group](auto logical_type) -> seastar::future<field_reader> {
          auto ccr = co_await fr.open_column_chunk_reader<logical_type.physical_type>(row_group, node.column_index);
          auto rr = typed_primitive_reader<decltype(logical_type)>{node, std::move(ccr)};
          co_return rr;
      },
      node.logical_type);
}
auto make_reader(file_reader& fr, const reader_schema::list_node& node, int row_group) -> seastar::future<field_reader> {
    auto child = co_await field_reader::make(fr, *node.element, row_group);
    co_return list_reader{node, std::make_unique<field_reader>(std::move(child))};
}

auto make_reader(file_reader& fr, const reader_schema::optional_node& node, int row_group) -> seastar::future<field_reader> {
    auto child = co_await field_reader::make(fr, *node.child, row_group);
    co_return optional_reader{node, std::make_unique<field_reader>(std::move(child))};
}

auto make_reader(file_reader& fr, const reader_schema::map_node& node, int row_group) -> seastar::future<field_reader> {
    auto key = co_await field_reader::make(fr, *node.key, row_group);
    auto value = co_await field_reader::make(fr, *node.value, row_group);
    co_return map_reader{node, std::make_unique<field_reader>(std::move(key)),
                         std::make_unique<field_reader>(std::move(value))};
}

auto make_reader(file_reader& fr, const reader_schema::struct_node& node, int row_group) -> seastar::future<field_reader> {
    std::vector<field_reader> field_readers;
    field_readers.reserve(node.fields.size());
    for (const reader_schema::node& child : node.fields) {
        field_readers.push_back(co_await field_reader::make(fr, child, row_group));
    }
    co_return struct_reader{node, std::move(field_readers)};
}

}  // namespace

seastar::future<field_reader> field_reader::make(file_reader& fr, const reader_schema::node& node_variant,
                                                 int row_group) {
    auto f = [&fr, row_group](const auto& node) -> seastar::future<field_reader> {
        co_return co_await make_reader(fr, node, row_group);
    };
    return std::visit(f, node_variant);
}

seastar::future<record_reader> record_reader::make(file_reader& fr, int row_group) {
    std::vector<field_reader> field_readers;

    for (const reader_schema::node& field_node : fr.schema().fields) {
        field_readers.emplace_back(co_await field_reader::make(fr, field_node, row_group));
    }
    co_return record_reader{fr.schema(), std::move(field_readers)};
}
}  // namespace parquet4seastar::record
