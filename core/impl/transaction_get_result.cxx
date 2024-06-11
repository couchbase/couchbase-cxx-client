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

#include <couchbase/transactions/transaction_get_result.hxx>

#include <core/transactions/transaction_get_result.hxx>
#include <couchbase/codec/encoded_value.hxx>

namespace couchbase::transactions
{

transaction_get_result::transaction_get_result()
  : base_(std::make_shared<couchbase::core::transactions::transaction_get_result>())
{
}

auto
transaction_get_result::content() const -> const codec::encoded_value&
{
  return base_->content();
}

void
transaction_get_result::content(const codec::encoded_value& content)
{
  return base_->content(content);
}

void
transaction_get_result::content(codec::encoded_value&& content)
{
  return base_->content(std::move(content));
}

auto
transaction_get_result::key() const -> const std::string&
{
  return base_->key();
}

auto
transaction_get_result::bucket() const -> const std::string&
{
  return base_->bucket();
}

auto
transaction_get_result::scope() const -> const std::string&
{
  return base_->scope();
}

auto
transaction_get_result::collection() const -> const std::string&
{
  return base_->collection();
}

auto
transaction_get_result::cas() const -> couchbase::cas
{
  return base_->cas();
}
} // namespace couchbase::transactions
