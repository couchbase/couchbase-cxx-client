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

#include <string>
#include <vector>

#include <couchbase/cas.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/collection.hxx>
#include <couchbase/result.hxx>

// forward declarations...
namespace couchbase::core::transactions
{
class attempt_context_impl;
class transaction_get_result;
class transaction_links;
class document_metadata;
} // namespace couchbase::core::transactions

namespace couchbase::transactions
{

class transaction_get_result
{
public:
  /**
   * @private
   */
  transaction_get_result();

  /**
   * Content of the document.
   *
   * @return content of the document.
   */
  template<typename Document,
           typename Transcoder = codec::default_json_transcoder,
           std::enable_if_t<!codec::is_transcoder_v<Document>, bool> = true,
           std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
  [[nodiscard]] auto content_as() const -> Document
  {
    return Transcoder::template decode<Document>(content());
  }

  template<typename Transcoder, std::enable_if_t<codec::is_transcoder_v<Transcoder>, bool> = true>
  [[nodiscard]] auto content_as() const -> typename Transcoder::document_type
  {
    return Transcoder::decode(content());
  }

  /**
   * Get document id.
   *
   * @return the id of this document.
   */
  [[nodiscard]] auto id() const -> const std::string&;

private:
  std::shared_ptr<couchbase::core::transactions::transaction_get_result> base_{};

  explicit transaction_get_result(
    std::shared_ptr<couchbase::core::transactions::transaction_get_result> base)
    : base_(std::move(base))
  {
  }

  [[nodiscard]] auto content() const -> const codec::encoded_value&;

  friend class couchbase::core::transactions::transaction_get_result;
  friend class couchbase::core::transactions::attempt_context_impl;

  [[nodiscard]] auto bucket() const -> const std::string&;
  [[nodiscard]] auto scope() const -> const std::string&;
  [[nodiscard]] auto collection() const -> const std::string&;
  [[nodiscard]] auto cas() const -> couchbase::cas;
};
} // namespace couchbase::transactions
