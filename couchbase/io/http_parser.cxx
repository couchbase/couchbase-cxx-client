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
#include <algorithm>

#include <spdlog/fmt/bundled/core.h>

#include <http_parser.h>

#include <couchbase/io/http_parser.hxx>

static inline int
static_on_status(::http_parser* parser, const char* at, std::size_t length)
{
    auto* wrapper = static_cast<couchbase::io::http_parser*>(parser->data);
    wrapper->response.status_message.assign(at, length);
    wrapper->response.status_code = parser->status_code;
    return 0;
}

static inline int
static_on_header_field(::http_parser* parser, const char* at, std::size_t length)
{
    auto* wrapper = static_cast<couchbase::io::http_parser*>(parser->data);
    wrapper->header_field.assign(at, length);
    std::transform(wrapper->header_field.begin(), wrapper->header_field.end(), wrapper->header_field.begin(), [](unsigned char c) {
        return std::tolower(c);
    });
    return 0;
}

static inline int
static_on_header_value(::http_parser* parser, const char* at, std::size_t length)
{
    auto* wrapper = static_cast<couchbase::io::http_parser*>(parser->data);
    wrapper->response.headers[wrapper->header_field] = std::string(at, length);
    return 0;
}

static inline int
static_on_body(::http_parser* parser, const char* at, std::size_t length)
{
    auto* wrapper = static_cast<couchbase::io::http_parser*>(parser->data);
    wrapper->response.body.append(at, length);
    return 0;
}

static inline int
static_on_message_complete(::http_parser* parser)
{
    auto* wrapper = static_cast<couchbase::io::http_parser*>(parser->data);
    wrapper->complete = true;
    return 0;
}

namespace couchbase::io
{
struct http_parser_state {
    ::http_parser_settings settings_{};
    ::http_parser parser_{};
};

http_parser::http_parser()
{
    state_ = std::make_shared<http_parser_state>();
    state_->parser_.data = this;
    state_->settings_.on_status = static_on_status;
    state_->settings_.on_header_field = static_on_header_field;
    state_->settings_.on_header_value = static_on_header_value;
    state_->settings_.on_body = static_on_body;
    state_->settings_.on_message_complete = static_on_message_complete;
    ::http_parser_init(&state_->parser_, HTTP_RESPONSE);
}

void
http_parser::reset()
{
    complete = false;
    response = {};
    header_field = {};
    ::http_parser_init(&state_->parser_, HTTP_RESPONSE);
}

std::string
http_parser::error_message() const
{
#define HTTP_ERRNO_GEN(n, s)                                                                                                               \
    case HPE_##n:                                                                                                                          \
        return fmt::format("HPE_" #n " ({})", s);

    switch (state_->parser_.http_errno) {
        HTTP_ERRNO_MAP(HTTP_ERRNO_GEN)
    };
#undef HTTP_ERRNO_GEN
    return "unknown error: " + std::to_string(state_->parser_.http_errno);
}

http_parser::status
http_parser::feed(const char* data, size_t data_len)
{
    if (std::size_t bytes_parsed = ::http_parser_execute(&state_->parser_, &state_->settings_, data, data_len); bytes_parsed != data_len) {
        return status::failure;
    }
    return status::ok;
}
} // namespace couchbase::io
