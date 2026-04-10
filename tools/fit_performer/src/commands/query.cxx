/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "query.hxx"
#include "../exceptions.hxx"
#include "common.hxx"

#include <core/meta/features.hxx>

namespace fit_cxx::commands::query
{
couchbase::query_options
to_query_options(const protocol::sdk::query::Command& cmd, observability::span_owner* spans)
{
  couchbase::query_options options{};

  if (!cmd.has_options()) {
    return options;
  }

  if (cmd.options().has_scan_consistency()) {
    switch (cmd.options().scan_consistency()) {
      case protocol::shared::REQUEST_PLUS:
        options.scan_consistency(couchbase::query_scan_consistency::request_plus);
        break;
      case protocol::shared::NOT_BOUNDED:
        options.scan_consistency(couchbase::query_scan_consistency::not_bounded);
        break;
      default:
        throw performer_exception::unimplemented("query scan consistency type not recognised");
    }
  }
  for (const auto& [key, val] : cmd.options().raw()) {
    options.raw(key, val);
  }
  if (cmd.options().has_adhoc()) {
    options.adhoc(cmd.options().adhoc());
  }
  if (cmd.options().has_profile()) {
    if (cmd.options().profile() == "off") {
      options.profile(couchbase::query_profile::off);
    } else if (cmd.options().profile() == "phases") {
      options.profile(couchbase::query_profile::phases);
    } else if (cmd.options().profile() == "timings") {
      options.profile(couchbase::query_profile::timings);
    } else {
      throw performer_exception::unimplemented("query profile type not recognised");
    }
  }
  if (cmd.options().has_readonly()) {
    options.readonly(cmd.options().readonly());
  }
  if (cmd.options().parameters_positional_size() > 0) {
#ifdef COUCHBASE_CXX_CLIENT_QUERY_OPTIONS_HAVE_ADD_PARAMETER
    for (const auto& param : cmd.options().parameters_positional()) {
      options.add_positional_parameter(param);
    }
#else
    std::vector<std::vector<std::byte>> positional_params{};
    for (const auto& param : cmd.options().parameters_positional()) {
      positional_params.emplace_back(couchbase::codec::tao_json_serializer::serialize(param));
    }
    options.encoded_positional_parameters(positional_params);
#endif
  }
  if (cmd.options().parameters_named_size() > 0) {
#ifdef COUCHBASE_CXX_CLIENT_QUERY_OPTIONS_HAVE_ADD_PARAMETER
    for (const auto& [key, val] : cmd.options().parameters_named()) {
      options.add_named_parameter(key, val);
    }
#else
    std::map<std::string, std::vector<std::byte>, std::less<>> named_params{};
    std::map<std::string, std::string> m{};
    for (const auto& [key, val] : cmd.options().parameters_named()) {
      named_params.emplace(key, couchbase::codec::tao_json_serializer::serialize(val));
    }
    options.encoded_named_parameters(named_params);
#endif
  }
  if (cmd.options().has_flex_index()) {
    options.flex_index(cmd.options().flex_index());
  }
  if (cmd.options().has_pipeline_cap()) {
    options.pipeline_cap(static_cast<std::uint64_t>(cmd.options().pipeline_cap()));
  }
  if (cmd.options().has_pipeline_batch()) {
    options.pipeline_batch(static_cast<std::uint64_t>(cmd.options().pipeline_batch()));
  }
  if (cmd.options().has_scan_cap()) {
    options.scan_cap(static_cast<std::uint64_t>(cmd.options().scan_cap()));
  }
  if (cmd.options().has_scan_wait_millis()) {
    options.scan_wait(std::chrono::milliseconds{ cmd.options().scan_wait_millis() });
  }
  if (cmd.options().has_timeout_millis()) {
    options.timeout(std::chrono::milliseconds{ cmd.options().timeout_millis() });
  }
  if (cmd.options().has_max_parallelism()) {
    options.max_parallelism(static_cast<std::uint64_t>(cmd.options().max_parallelism()));
  }
  if (cmd.options().has_metrics()) {
    options.metrics(cmd.options().metrics());
  }
  if (cmd.options().has_client_context_id()) {
    options.client_context_id(cmd.options().client_context_id());
  }
  if (cmd.options().has_preserve_expiry()) {
    options.preserve_expiry(cmd.options().preserve_expiry());
  }
  if (cmd.options().has_consistent_with()) {
    couchbase::mutation_state state{};
    for (auto& t : cmd.options().consistent_with().tokens()) {
      // Create a dummy mutation result to add the token to the state
      couchbase::mutation_token token{ static_cast<std::uint64_t>(t.partition_uuid()),
                                       static_cast<std::uint64_t>(t.sequence_number()),
                                       static_cast<std::uint16_t>(t.partition_id()),
                                       t.bucket_name() };
      couchbase::mutation_result mutation_result{ couchbase::cas{ 0 }, token };
      state.add(mutation_result);
    }
    options.consistent_with(state);
  }
  if (cmd.options().has_use_replica()) {
    options.use_replica(cmd.options().use_replica());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    options.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return options;
}

void
convert_query_metrics(const couchbase::query_metrics& metrics,
                      protocol::sdk::query::QueryMetrics* proto_metrics)
{
  proto_metrics->mutable_elapsed_time()->set_seconds(
    static_cast<std::int32_t>(metrics.elapsed_time().count() / 1'000'000'000));
  proto_metrics->mutable_elapsed_time()->set_nanos(
    static_cast<std::int32_t>(metrics.elapsed_time().count() % 1'000'000'000));
  proto_metrics->mutable_execution_time()->set_seconds(
    static_cast<std::int32_t>(metrics.execution_time().count() / 1'000'000'000));
  proto_metrics->mutable_execution_time()->set_nanos(
    static_cast<std::int32_t>(metrics.execution_time().count() % 1'000'000'000));
  proto_metrics->set_sort_count(metrics.sort_count());
  proto_metrics->set_result_count(metrics.result_count());
  proto_metrics->set_result_size(metrics.result_size());
  proto_metrics->set_mutation_count(metrics.mutation_count());
  proto_metrics->set_error_count(metrics.error_count());
  proto_metrics->set_warning_count(metrics.warning_count());
}

void
convert_query_warning(const couchbase::query_warning& warning,
                      protocol::sdk::query::QueryWarning* proto_warning)
{
  proto_warning->set_code(static_cast<std::int32_t>(warning.code()));
  proto_warning->set_message(warning.message());
}

void
convert_query_meta_data(const couchbase::query_meta_data& meta,
                        protocol::sdk::query::QueryMetaData* proto_meta)
{
  proto_meta->set_request_id(meta.request_id());
  proto_meta->set_client_context_id(meta.client_context_id());
  switch (meta.status()) {
    case couchbase::query_status::running:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::RUNNING);
      break;
    case couchbase::query_status::success:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::SUCCESS);
      break;
    case couchbase::query_status::errors:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::ERRORS);
      break;
    case couchbase::query_status::completed:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::COMPLETED);
      break;
    case couchbase::query_status::stopped:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::STOPPED);
      break;
    case couchbase::query_status::timeout:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::TIMEOUT);
      break;
    case couchbase::query_status::closed:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::CLOSED);
      break;
    case couchbase::query_status::fatal:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::FATAL);
      break;
    case couchbase::query_status::aborted:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::ABORTED);
      break;
    case couchbase::query_status::unknown:
      proto_meta->set_status(protocol::sdk::query::QueryStatus::UNKNOWN);
      break;
  }
  if (meta.signature().has_value()) {
    proto_meta->set_signature(reinterpret_cast<const char*>(meta.signature().value().data()),
                              meta.signature().value().size());
  }
  if (meta.profile().has_value()) {
    proto_meta->set_profile(reinterpret_cast<const char*>(meta.profile().value().data()),
                            meta.profile().value().size());
  }
  if (meta.metrics().has_value()) {
    convert_query_metrics(meta.metrics().value(), proto_meta->mutable_metrics());
  }
  for (const auto& warning : meta.warnings()) {
    auto proto_warning = proto_meta->add_warnings();
    convert_query_warning(warning, proto_warning);
  }
}

void
from_query_result(const couchbase::query_result& result,
                  const protocol::shared::ContentAs& content_as,
                  protocol::sdk::query::QueryResult* proto_result)
{
  if (content_as.has_as_byte_array()) {
    for (auto row : result.rows_as_binary()) {
      proto_result->add_content()->set_content_as_bytes(reinterpret_cast<const char*>(row.data()),
                                                        row.size());
    }
  } else if (content_as.has_as_json_object() || content_as.has_as_json_array()) {
    for (const auto& row : result.rows_as<couchbase::codec::tao_json_serializer>()) {
      proto_result->add_content()->set_content_as_bytes(
        couchbase::core::utils::json::generate(row));
    }
  } else if (content_as.has_as_string()) {
    for (const auto& row : result.rows_as<couchbase::codec::tao_json_serializer>()) {
      proto_result->add_content()->set_content_as_string(
        couchbase::core::utils::json::generate(row));
    }
  } else if (content_as.has_as_boolean()) {
    for (const auto& row : result.rows_as<couchbase::codec::tao_json_serializer>()) {
      proto_result->add_content()->set_content_as_bool(row.get_boolean());
    }
  } else if (content_as.has_as_integer()) {
    for (const auto& row : result.rows_as<couchbase::codec::tao_json_serializer>()) {
      proto_result->add_content()->set_content_as_int64(row.get_signed());
    }
  } else if (content_as.has_as_floating_point()) {
    for (const auto& row : result.rows_as<couchbase::codec::tao_json_serializer>()) {
      proto_result->add_content()->set_content_as_double(row.get_double());
    }
  }
  convert_query_meta_data(result.meta_data(), proto_result->mutable_meta_data());
}

protocol::run::Result
execute_command(const protocol::sdk::query::Command& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();
  auto content_as = cmd.content_as();
  auto options = to_query_options(cmd, args.spans);
  couchbase::error err;
  couchbase::query_result res;
  if (args.scope.has_value()) {
    auto scope = args.scope.value();
    auto start = std::chrono::high_resolution_clock::now();
    auto resp = scope.query(cmd.statement(), options).get();
    auto end = std::chrono::high_resolution_clock ::now();
    err = resp.first;
    res = resp.second;
    proto_res.set_elapsednanos(std::chrono::duration<int64_t, std::nano>(end - start).count());
  } else {
    auto start = std::chrono::high_resolution_clock::now();
    auto resp = args.cluster->query(cmd.statement(), options).get();
    auto end = std::chrono::high_resolution_clock ::now();
    err = resp.first;
    res = resp.second;
    proto_res.set_elapsednanos(std::chrono::duration<int64_t, std::nano>(end - start).count());
  }
  if (err.ec()) {
    fit_cxx::commands::common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_query_result(res, content_as, proto_res.mutable_sdk()->mutable_query_result());
    } else {
      proto_res.mutable_sdk()->set_success(true);
    }
  }
  return proto_res;
}
} // namespace fit_cxx::commands::query
