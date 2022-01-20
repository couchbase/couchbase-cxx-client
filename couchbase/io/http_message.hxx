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

#include <couchbase/service_type.hxx>
#include <couchbase/utils/json_streaming_lexer.hxx>

#include <map>
#include <optional>
#include <string>

namespace couchbase::io
{

struct streaming_settings {
    std::string pointer_expression;
    std::uint32_t depth;
    std::function<utils::json::stream_control(std::string&& row)> row_handler;
};

struct http_request {
    service_type type;
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
    std::optional<streaming_settings> streaming{};
};

class http_response_body
{
  public:
    void use_json_streaming(streaming_settings&& settings)
    {
        lexer_ = std::make_unique<utils::json::streaming_lexer>(settings.pointer_expression, settings.depth);
        lexer_->on_row(std::move(settings.row_handler));
        lexer_->on_complete([this](std::error_code ec, std::size_t number_of_rows, std::string&& meta) {
            ec_ = ec;
            number_of_rows_ = number_of_rows;
            data_ = std::move(meta);
        });
    }

    void append(std::string_view chunk)
    {
        if (lexer_) {
            lexer_->feed(chunk);
        } else {
            data_.append(chunk);
        }
    }

    [[nodiscard]] const std::string& data() const
    {
        return data_;
    }

    [[nodiscard]] const std::size_t& number_of_rows() const
    {
        return number_of_rows_;
    }

    [[nodiscard]] const std::error_code& ec() const
    {
        return ec_;
    }

  private:
    std::string data_{};
    std::error_code ec_{};
    std::size_t number_of_rows_{};
    std::unique_ptr<utils::json::streaming_lexer> lexer_{};
};

struct http_response {
    uint32_t status_code;
    std::string status_message;
    std::map<std::string, std::string> headers;
    http_response_body body{};

    [[nodiscard]] bool must_close_connection() const
    {
        if (const auto it = headers.find("connection"); it != headers.end()) {
            return it->second == "close";
        }
        return false;
    }
};
} // namespace couchbase::io
