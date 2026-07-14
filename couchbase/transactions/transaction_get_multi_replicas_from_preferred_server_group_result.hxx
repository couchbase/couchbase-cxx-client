/*
 *     Copyright 2021-Present Couchbase, Inc.
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
#pragma once

#include <couchbase/codec/lenient_json_transcoder.hxx>

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace couchbase
{
namespace core::transactions
{
class attempt_context_impl;
} // namespace core::transactions

namespace transactions
{
class transaction_get_multi_replicas_from_preferred_server_group_result
{
public:
  transaction_get_multi_replicas_from_preferred_server_group_result(
    const transaction_get_multi_replicas_from_preferred_server_group_result&) = default;
  ~transaction_get_multi_replicas_from_preferred_server_group_result() = default;
  transaction_get_multi_replicas_from_preferred_server_group_result(
    transaction_get_multi_replicas_from_preferred_server_group_result&&) = default;
  auto operator=(const transaction_get_multi_replicas_from_preferred_server_group_result&)
    -> transaction_get_multi_replicas_from_preferred_server_group_result& = default;
  auto operator=(transaction_get_multi_replicas_from_preferred_server_group_result&&)
    -> transaction_get_multi_replicas_from_preferred_server_group_result& = default;

  /**
   * Content of the document.
   *
   * When no transcoder is given, the document is decoded leniently, without validating its
   * common flags. Pass a transcoder explicitly to enforce its flag handling instead.
   *
   * @return content of the document.
   */
  template<typename Document, std::enable_if_t<!codec::is_transcoder_v<Document>, bool> = true>
  [[nodiscard]] auto content_as(std::size_t spec_index) const -> Document
  {
    return codec::default_lenient_json_transcoder::decode<Document>(content_at(spec_index));
  }

  /**
   * Content of the document, decoded with the given transcoder.
   *
   * @return content of the document.
   */
  template<typename Document,
           typename Transcoder,
           std::enable_if_t<!codec::is_transcoder_v<Document>, bool> = true,
           std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
  [[nodiscard]] auto content_as(std::size_t spec_index) const -> Document
  {
    return Transcoder::template decode<Document>(content_at(spec_index));
  }

  /**
   * Content of the document, decoded with the given transcoder.
   *
   * @return content of the document.
   */
  template<typename Transcoder, std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
  [[nodiscard]] auto content_as(std::size_t spec_index) const -> typename Transcoder::document_type
  {
    return Transcoder::decode(content_at(spec_index));
  }

  /**
   * Check if spec returned any content
   *
   * @return the id of this document.
   */
  [[nodiscard]] auto exists(std::size_t spec_index) const -> bool
  {
    if (spec_index >= content_.size()) {
      throw std::invalid_argument("spec index " + std::to_string(spec_index) + " is not valid");
    }
    return content_[spec_index].has_value();
  }

private:
  friend core::transactions::attempt_context_impl;

  explicit transaction_get_multi_replicas_from_preferred_server_group_result(
    std::vector<std::optional<codec::encoded_value>> content)
    : content_{ std::move(content) }
  {
  }

  [[nodiscard]] auto content_at(std::size_t spec_index) const -> const codec::encoded_value&;

  std::vector<std::optional<codec::encoded_value>> content_;
};
} // namespace transactions
} // namespace couchbase
