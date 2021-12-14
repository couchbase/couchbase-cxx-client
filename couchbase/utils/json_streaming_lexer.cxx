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

#include "json_streaming_lexer.hxx"

#include <jsonsl.h>

#include <stdexcept>

namespace couchbase::utils::json
{
namespace priv
{
struct streaming_lexer_impl {
    streaming_lexer_impl(jsonsl_t lexer, jsonsl_jpr_t pointer)
      : lexer_(lexer)
      , pointer_(pointer)
    {
    }

    streaming_lexer_impl(streaming_lexer_impl& other) = delete;
    const streaming_lexer_impl& operator=(const streaming_lexer_impl& other) = delete;
    streaming_lexer_impl(streaming_lexer_impl&& other) noexcept = delete;
    const streaming_lexer_impl& operator=(streaming_lexer_impl&& other) = delete;

    ~streaming_lexer_impl()
    {
        jsonsl_jpr_destroy(pointer_);
        jsonsl_destroy(lexer_);
    }

    jsonsl_t lexer_{};
    jsonsl_jpr_t pointer_{};
    std::string buffer_{};
    std::string last_key_{};
};
} // namespace priv

static void
action_callback(jsonsl_t lexer, jsonsl_action_t action, struct jsonsl_state_st* state, const jsonsl_char_t* /* at */)
{
    auto* impl = static_cast<priv::streaming_lexer_impl*>(lexer->data);
    jsonsl_jpr_match_t match = JSONSL_MATCH_UNKNOWN;

    if (action == JSONSL_ACTION_PUSH) {
        if (state->type != JSONSL_T_HKEY) {
            auto key = std::move(impl->last_key_);
            jsonsl_jpr_match_state(lexer, state, key.data(), key.size(), &match);
        }
    } else if (action == JSONSL_ACTION_POP) {
        if (state->type == JSONSL_T_HKEY) {
            impl->last_key_ = impl->buffer_.substr(state->pos_begin + 1, state->pos_cur - state->pos_begin - 1);
        }
        /*
        bool include_value_with_container = state->level == lexer->max_callback_level - 1;
        if (match == JSONSL_MATCH_POSSIBLE) {

        }
         */
    }
}

static int
error_callback(jsonsl_t /* lexer */, jsonsl_error_t error, struct jsonsl_state_st* /* state */, jsonsl_char_t* /* at */)
{
    fprintf(stderr, "error: %s\n", jsonsl_strerror(error));
    return 0;
}

json::streaming_lexer::streaming_lexer(const std::string& pointer_expression, std::uint32_t depth)
{
    jsonsl_error_t error = JSONSL_ERROR_SUCCESS;
    jsonsl_jpr_t ptr = jsonsl_jpr_new(pointer_expression.c_str(), &error);
    if (ptr == nullptr) {
        throw std::invalid_argument("unable to allocate JSON pointer");
    }
    if (error != JSONSL_ERROR_SUCCESS) {
        throw std::invalid_argument(std::string("unable to create JSON pointer: ") + jsonsl_strerror(error));
    }

    impl_ = std::make_shared<priv::streaming_lexer_impl>(jsonsl_new(512), ptr);
    impl_->lexer_->data = impl_.get();
    impl_->lexer_->action_callback = action_callback;
    impl_->lexer_->error_callback = error_callback;
    jsonsl_jpr_match_state_init(impl_->lexer_, &impl_->pointer_, 1);
    jsonsl_enable_all_callbacks(impl_->lexer_);
    impl_->lexer_->max_callback_level = depth;
}

void
streaming_lexer::feed(std::string_view data)
{
    impl_->buffer_.append(data);
    jsonsl_feed(impl_->lexer_, data.data(), data.size());
}
} // namespace couchbase::utils::json
