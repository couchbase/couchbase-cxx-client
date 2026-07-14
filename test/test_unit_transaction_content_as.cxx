/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024-Present Couchbase, Inc.
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

#include "test_helper.hxx"

#include <couchbase/codec/raw_binary_transcoder.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/transactions/transaction_get_multi_replicas_from_preferred_server_group_result.hxx>
#include <couchbase/transactions/transaction_get_multi_result.hxx>

#include <cstddef>
#include <string>
#include <vector>

// The transaction get-multi results expose three content_as() overloads:
//
//   content_as<Document>(idx)               -> Document (no transcoder, lenient)
//   content_as<Document, Transcoder>(idx)   -> Document
//   content_as<Transcoder>(idx)             -> Transcoder::document_type
//
// The transcoder overloads decode content_[idx], which is a
// std::optional<encoded_value>, and must pass content.value() rather than the
// optional itself. The transcoder-only overload once passed the optional
// (CXXCBC-847); it compiled only because nothing instantiated it, so the type
// error stayed dormant.
//
// The results have private constructors (only attempt_context_impl builds one),
// so this cannot construct an instance. It does not need to: instantiating every
// overload is enough to type-check its body. The regression was a compile error,
// so if it returns, this test binary simply fails to build.
namespace
{
template<typename Result>
void
instantiate_content_as_overloads(Result* result)
{
  // `result` is always null at the only call sites below; the branch is never
  // taken. Its sole purpose is to force the compiler to instantiate the bodies
  // of every content_as overload (a plain function template instantiates its
  // whole body on use, unlike `if constexpr`).
  if (result != nullptr) {
    (void)result->template content_as<std::string>(0);
    (void)result
      ->template content_as<std::vector<std::byte>, couchbase::codec::raw_binary_transcoder>(0);
    (void)result->template content_as<couchbase::codec::raw_binary_transcoder>(0);
  }
}
} // namespace

TEST_CASE("unit: transaction get-multi results instantiate both content_as overloads", "[unit]")
{
  instantiate_content_as_overloads<couchbase::transactions::transaction_get_multi_result>(nullptr);
  instantiate_content_as_overloads<
    couchbase::transactions::transaction_get_multi_replicas_from_preferred_server_group_result>(
    nullptr);
  SUCCEED("both content_as overloads instantiate for the transaction get-multi results");
}
