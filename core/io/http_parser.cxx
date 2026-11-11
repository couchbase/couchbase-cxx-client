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

#include "http_parser.hxx"

#include <llhttp.h>
#include <spdlog/fmt/bundled/core.h>

#include <algorithm>
#include <utility>

namespace couchbase::core::io
{
struct http_parser_state {
  llhttp_settings_t settings_{};
  llhttp_t parser_{};
  bool reading_header_value_{ false };
};
} // namespace couchbase::core::io

namespace
{
// NOLINTBEGIN(misc-const-correctness): these are llhttp callbacks whose signatures must match
// llhttp_data_cb / llhttp_cb exactly (int(llhttp_t*, ...)); the parser pointer cannot be const or
// the assignment to the llhttp_settings_t members below would be ill-formed.
inline auto
static_on_message_begin(llhttp_t* parser) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  auto* state = reinterpret_cast<couchbase::core::io::http_parser_state*>(
    reinterpret_cast<char*>(parser) - offsetof(couchbase::core::io::http_parser_state, parser_));
  wrapper->complete = false;
  wrapper->response.status_code = 0;
  wrapper->response.status_message.clear();
  wrapper->response.headers.clear();
  wrapper->header_field.clear();
  state->reading_header_value_ = false;
  return 0;
}

inline auto
static_on_status(llhttp_t* parser, const char* at, std::size_t length) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  wrapper->response.status_message.append(at, length);
  wrapper->response.status_code = parser->status_code;
  return 0;
}

inline auto
static_on_header_field(llhttp_t* parser, const char* at, std::size_t length) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  auto* state = reinterpret_cast<couchbase::core::io::http_parser_state*>(
    reinterpret_cast<char*>(parser) - offsetof(couchbase::core::io::http_parser_state, parser_));
  if (state->reading_header_value_) {
    wrapper->header_field.clear();
    state->reading_header_value_ = false;
  }
  const auto old_size = wrapper->header_field.size();
  wrapper->header_field.append(at, length);
  std::transform(wrapper->header_field.begin() + static_cast<std::ptrdiff_t>(old_size),
                 wrapper->header_field.end(),
                 wrapper->header_field.begin() + static_cast<std::ptrdiff_t>(old_size),
                 [](unsigned char c) {
                   return static_cast<char>(std::tolower(c));
                 });
  return 0;
}

inline auto
static_on_header_value(llhttp_t* parser, const char* at, std::size_t length) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  auto* state = reinterpret_cast<couchbase::core::io::http_parser_state*>(
    reinterpret_cast<char*>(parser) - offsetof(couchbase::core::io::http_parser_state, parser_));
  auto& slot = wrapper->response.headers[wrapper->header_field];
  if (!state->reading_header_value_) {
    if (wrapper->header_field == "set-cookie") {
      slot.clear();
    } else if (!slot.empty()) {
      slot.append(", ");
    }
    state->reading_header_value_ = true;
  }
  slot.append(at, length);
  return 0;
}

inline auto
static_on_headers_complete(llhttp_t* parser) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  wrapper->response.status_code = parser->status_code;
  return 0;
}

inline auto
static_on_body(llhttp_t* parser, const char* at, std::size_t length) -> int
{
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  wrapper->response.body.append(std::string_view{ at, length });
  return 0;
}

inline auto
static_on_message_complete(llhttp_t* parser) -> int
{
  if (parser->status_code >= 100 && parser->status_code < 200 && parser->status_code != 101) {
    return 0;
  }
  auto* wrapper = static_cast<couchbase::core::io::http_parser*>(parser->data);
  wrapper->complete = true;
  return 0;
}
// NOLINTEND(misc-const-correctness)

void
init_parser_state(std::shared_ptr<couchbase::core::io::http_parser_state>& state,
                  couchbase::core::io::http_parser* owner)
{
  if (!state) {
    state = std::make_shared<couchbase::core::io::http_parser_state>();
    llhttp_settings_init(&state->settings_);
    state->settings_.on_message_begin = static_on_message_begin;
    state->settings_.on_status = static_on_status;
    state->settings_.on_header_field = static_on_header_field;
    state->settings_.on_header_value = static_on_header_value;
    state->settings_.on_headers_complete = static_on_headers_complete;
    state->settings_.on_body = static_on_body;
    state->settings_.on_message_complete = static_on_message_complete;
  }
  state->reading_header_value_ = false;
  llhttp_init(&state->parser_, HTTP_RESPONSE, &state->settings_);
  state->parser_.data = owner;
}
} // namespace

namespace couchbase::core::io
{
http_parser::http_parser()
{
  init_parser_state(state_, this);
}

http_parser::http_parser(http_parser&& other) noexcept
  : response(std::move(other.response))
  , header_field(std::move(other.header_field))
  , complete(std::exchange(other.complete, false))
  , state_(std::move(other.state_))
{
  if (state_) {
    state_->parser_.data = this;
  }
}

auto
http_parser::operator=(http_parser&& other) noexcept -> http_parser&
{
  if (this == &other) {
    return *this;
  }
  response = std::move(other.response);
  header_field = std::move(other.header_field);
  complete = std::exchange(other.complete, false);
  state_ = std::move(other.state_);
  if (state_) {
    state_->parser_.data = this;
  }
  return *this;
}
void
http_parser::reset()
{
  complete = false;
  response = {};
  header_field.clear();
  init_parser_state(state_, this);
}

auto
http_parser::error_message() const -> const char*
{
  if (!state_) {
    return "uninitialized parser";
  }
  return llhttp_errno_name(llhttp_get_errno(&state_->parser_));
}

auto
http_parser::feed(const char* data, size_t data_len) const -> http_parser::feeding_result
{
  if (!state_) {
    return { true, complete, error_message() };
  }
  auto error = llhttp_execute(&state_->parser_, data, data_len);
  if (error != HPE_OK) {
    return { true, complete, error_message() };
  }
  return { false, complete };
}
} // namespace couchbase::core::io
