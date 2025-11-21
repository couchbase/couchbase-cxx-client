/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include "meter_wrapper.hxx"

#include <couchbase/error_codes.hxx>

#include "core/metrics/constants.hxx"
#include "core/tracing/constants.hxx"

#include <mutex>
#include <system_error>

namespace couchbase::core::metrics
{
namespace
{
auto
snake_case_to_camel_case(const std::string& s) -> std::string
{
  std::string res{};
  bool capitalize{ true }; // First letter should be capitalized
  for (const char c : s) {
    if (c == '_') {
      capitalize = true; // Next letter should be capitalized
      continue;
    }
    if (capitalize) {
      res += static_cast<char>(std::toupper(c));
      capitalize = false;
    } else {
      res += c;
    }
  }
  return res;
}

auto
extract_error_name(std::error_code ec) -> std::string
{
  const std::string::size_type pos = ec.message().find(' ');
  if (pos != std::string::npos) {
    return ec.message().substr(0, pos);
  }
  return ec.message();
}

auto
get_standardized_error_type(std::error_code ec) -> std::string
{
  if (!ec) {
    return {};
  }

  // SDK-specific errors
  if (ec.value() >= 1000) {
    return "CouchbaseError";
  }

  // Errors where message and RFC-message don't match
  if (ec == errc::field_level_encryption::generic_cryptography_failure) {
    return "CryptoError";
  }

  static const std::array<const std::error_category*, 12> cb_categories = {
    &impl::common_category(),      &impl::key_value_category(),
    &impl::query_category(),       &impl::analytics_category(),
    &impl::search_category(),      &impl::view_category(),
    &impl::management_category(),  &impl::field_level_encryption_category(),
    &impl::network_category(),     &impl::streaming_json_lexer_category(),
    &impl::transaction_category(), &impl::transaction_op_category(),
  };

  if (std::any_of(
        cb_categories.begin(), cb_categories.end(), [&ec](const std::error_category* cat) {
          return &ec.category() == cat;
        })) {
    return snake_case_to_camel_case(extract_error_name(ec));
  }

  return "_OTHER";
}
} // namespace

auto
metric_attributes::encode() const -> std::map<std::string, std::string>
{
  std::map<std::string, std::string> tags = {
    { tracing::attributes::common::system, "couchbase" },
    { tracing::attributes::op::service, service },
    { tracing::attributes::op::operation_name, operation },
  };

  if (internal.cluster_name.has_value()) {
    tags.emplace(tracing::attributes::common::cluster_name, internal.cluster_name.value());
  }
  if (internal.cluster_uuid.has_value()) {
    tags.emplace(tracing::attributes::common::cluster_uuid, internal.cluster_uuid.value());
  }
  if (bucket_name) {
    tags.emplace(tracing::attributes::op::bucket_name, bucket_name.value());
  }
  if (scope_name) {
    tags.emplace(tracing::attributes::op::scope_name, scope_name.value());
  }
  if (collection_name) {
    tags.emplace(tracing::attributes::op::collection_name, collection_name.value());
  }
  if (ec) {
    tags.emplace(tracing::attributes::op::error_type, get_standardized_error_type(ec));
  }

  return tags;
}

meter_wrapper::meter_wrapper(std::shared_ptr<couchbase::metrics::meter> meter)
  : meter_{ std::move(meter) }
{
}

void
meter_wrapper::start()
{
  meter_->start();
}

void
meter_wrapper::stop()
{
  meter_->stop();
}

void
meter_wrapper::record_value(metric_attributes attrs,
                            std::chrono::steady_clock::time_point start_time)
{
  {
    const std::shared_lock lock{ cluster_labels_mutex_ };

    if (cluster_name_) {
      attrs.internal.cluster_name = cluster_name_;
    }
    if (cluster_uuid_) {
      attrs.internal.cluster_uuid = cluster_uuid_;
    }
  }

  meter_->get_value_recorder(operation_meter_name, attrs.encode())
    ->record_value(std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - start_time)
                     .count());
}

void
meter_wrapper::update_config(topology::configuration config)
{
  const std::scoped_lock<std::shared_mutex> lock{ cluster_labels_mutex_ };
  if (config.cluster_uuid.has_value()) {
    cluster_uuid_ = config.cluster_uuid;
  }
  if (config.cluster_name.has_value()) {
    cluster_name_ = config.cluster_name;
  }
}

auto
meter_wrapper::create(std::shared_ptr<couchbase::metrics::meter> meter)
  -> std::shared_ptr<meter_wrapper>
{
  return std::make_shared<meter_wrapper>(std::move(meter));
}
} // namespace couchbase::core::metrics
