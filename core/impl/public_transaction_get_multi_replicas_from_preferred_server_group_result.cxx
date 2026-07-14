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

#include <couchbase/transactions/transaction_get_multi_replicas_from_preferred_server_group_result.hxx>

#include <couchbase/codec/encoded_value.hxx>
#include <couchbase/error_codes.hxx>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <system_error>

namespace couchbase::transactions
{
auto
transaction_get_multi_replicas_from_preferred_server_group_result::content_at(
  std::size_t spec_index) const -> const codec::encoded_value&
{
  if (spec_index >= content_.size()) {
    throw std::invalid_argument("spec index " + std::to_string(spec_index) + " is not valid");
  }
  if (const auto& content = content_[spec_index]; content.has_value()) {
    return content.value();
  }
  throw std::system_error(errc::key_value::document_not_found,
                          "document was not found for index " + std::to_string(spec_index));
}
} // namespace couchbase::transactions
