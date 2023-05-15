/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "core/operations/document_search.hxx"

#include <couchbase/search_meta_data.hxx>

namespace couchbase
{
class internal_search_meta_data
{
  public:
    explicit internal_search_meta_data(const core::operations::search_response::search_meta_data& meta);

    [[nodiscard]] auto client_context_id() const -> const std::string&;
    [[nodiscard]] auto errors() const -> const std::map<std::string, std::string>&;
    [[nodiscard]] auto metrics() const -> const couchbase::search_metrics&;

  private:
    std::string client_context_id_{};
    couchbase::search_metrics metrics_{};
    std::map<std::string, std::string> errors_{};
};

} // namespace couchbase
