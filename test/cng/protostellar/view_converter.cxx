/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026. Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// Unit tests for the view <-> couchbase.view.v1 converter (CXXCBC-899). Pure, no server.

#include "framework/test_runner.hxx"

#include "core/protostellar/view_converter.hxx"

#include <string>

namespace couchbase::test
{
namespace
{
namespace pv = ::couchbase::core::protostellar::view;
namespace ops = ::couchbase::core::operations;
namespace v1 = ::couchbase::view::v1;

void
encode_maps_core_fields()
{
  ops::document_view_request request;
  request.bucket_name = "travel";
  request.document_name = "dev_ddoc";
  request.view_name = "by_country";
  request.ns = couchbase::core::design_document_namespace::development;
  request.keys = { R"("a")", R"("b")" };
  request.limit = 10;
  request.skip = 2;
  request.reduce = false;
  request.group = true;
  request.group_level = 1;
  request.debug = true;
  request.consistency = couchbase::core::view_scan_consistency::request_plus;
  request.order = couchbase::core::view_sort_order::descending;
  request.on_error = couchbase::core::view_on_error::stop;

  const auto proto = pv::encode(request);
  assert_eq(proto.bucket_name(), std::string{ "travel" }, "bucket mapped");
  assert_eq(proto.design_document_name(), std::string{ "dev_ddoc" }, "design doc mapped");
  assert_eq(proto.view_name(), std::string{ "by_country" }, "view name mapped");
  assert_true(proto.namespace_() == v1::ViewQueryRequest_Namespace_NAMESPACE_DEVELOPMENT,
              "development namespace mapped");
  assert_eq(proto.keys_size(), 2, "keys mapped");
  assert_eq(proto.limit(), std::uint32_t{ 10 }, "limit mapped");
  assert_eq(proto.skip(), std::uint32_t{ 2 }, "skip mapped");
  assert_false(proto.reduce(), "reduce mapped");
  assert_true(proto.group(), "group mapped");
  assert_eq(proto.group_level(), std::uint32_t{ 1 }, "group_level mapped");
  assert_true(proto.debug(), "debug mapped");
  assert_true(proto.scan_consistency() ==
                v1::ViewQueryRequest_ScanConsistency_SCAN_CONSISTENCY_REQUEST_PLUS,
              "consistency mapped");
  assert_true(proto.order() == v1::ViewQueryRequest_Order_ORDER_DESCENDING, "order mapped");
  assert_true(proto.on_error() == v1::ViewQueryRequest_ErrorMode_ERROR_MODE_STOP,
              "on_error stop -> STOP");
}

void
can_encode_rejects_unsupported_features()
{
  ops::document_view_request base;
  base.bucket_name = "b";
  base.document_name = "d";
  base.view_name = "v";
  assert_true(pv::can_encode(base), "plain view encodes");

  ops::document_view_request with_raw = base;
  with_raw.raw.emplace("stale", "false");
  assert_false(pv::can_encode(with_raw), "raw passthrough is not supported");

  ops::document_view_request full = base;
  full.full_set = true;
  assert_false(pv::can_encode(full), "full_set is not supported");

  ops::document_view_request with_callback = base;
  with_callback.row_callback = [](std::string) {
    return couchbase::core::utils::json::stream_control{};
  };
  assert_false(pv::can_encode(with_callback), "streaming row_callback is not supported");
}

void
decode_rows_maps_rows_and_meta()
{
  v1::ViewQueryResponse message;
  auto* row = message.add_rows();
  row->set_id("doc-1");
  row->set_key(R"("k")");
  row->set_value(R"({"v":1})");
  auto* meta = message.mutable_meta_data();
  meta->set_total_rows(7);
  meta->set_debug("dbg");

  ops::document_view_response response;
  pv::decode_rows(message, response);

  assert_eq(response.rows.size(), std::size_t{ 1 }, "one row decoded");
  assert_true(response.rows.at(0).id.has_value(), "row id present");
  assert_eq(response.rows.at(0).id.value(), std::string{ "doc-1" }, "row id decoded");
  assert_eq(response.rows.at(0).key, std::string{ R"("k")" }, "row key decoded");
  assert_eq(response.rows.at(0).value, std::string{ R"({"v":1})" }, "row value decoded");
  assert_true(response.meta.total_rows.has_value(), "total_rows present");
  assert_eq(response.meta.total_rows.value(), std::uint64_t{ 7 }, "total_rows decoded");
  assert_true(response.meta.debug_info.has_value(), "debug decoded");
}

} // namespace

auto
tests() -> test_suite
{
  return {
    "protostellar_view_converter",
    {
      { "encode_maps_core_fields", encode_maps_core_fields },
      { "can_encode_rejects_unsupported_features", can_encode_rejects_unsupported_features },
      { "decode_rows_maps_rows_and_meta", decode_rows_maps_rows_and_meta },
    },
  };
}

} // namespace couchbase::test
