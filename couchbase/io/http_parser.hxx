/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#include <couchbase/io/http_message.hxx>
#include <algorithm>

namespace couchbase::io
{
struct http_parser_state;

struct http_parser {
    enum class status { ok, failure };

    http_response response;
    std::string header_field;
    bool complete{ false };

    http_parser();

    void reset();

    status feed(const char* data, size_t data_len);

  private:
    std::shared_ptr<http_parser_state> state_{};
};
} // namespace couchbase::io
