/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021 Couchbase, Inc.
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

#include "core/utils/json_streaming_lexer.hxx"

#include <jsonsl.h>
#include <ostream>

enum class parser_event_type {
    start_object,
    finish_object,
    start_array,
    finish_array,
    key,
    constant,
    string,
    integer,
    real,
};

std::ostream&
operator<<(std::ostream& os, const parser_event_type& type)
{
    switch (type) {
        case parser_event_type::start_object:
            return os << "start_object";
        case parser_event_type::finish_object:
            return os << "finish_object";
        case parser_event_type::start_array:
            return os << "start_array";
        case parser_event_type::finish_array:
            return os << "finish_array";
        case parser_event_type::key:
            return os << "key";
        case parser_event_type::constant:
            return os << "constant";
        case parser_event_type::string:
            return os << "string";
        case parser_event_type::integer:
            return os << "integer";
        case parser_event_type::real:
            return os << "real";
    }
    return os;
}

struct parser_event {
    parser_event_type type_;
    std::string value_{};
    jsonsl_jpr_match_t match_{ JSONSL_MATCH_UNKNOWN };

    explicit parser_event(parser_event_type type)
      : type_(type)
    {
    }

    parser_event(parser_event_type type, jsonsl_jpr_match_t match)
      : type_(type)
      , match_{ match }
    {
    }

    parser_event(parser_event_type type, std::string_view value)
      : type_(type)
      , value_{ value }
    {
    }

    parser_event(parser_event_type type, std::string_view value, jsonsl_jpr_match_t match)
      : type_(type)
      , value_{ value }
      , match_{ match }
    {
    }

    bool operator==(const parser_event& other) const
    {
        return type_ == other.type_ && value_ == other.value_ && match_ == other.match_;
    }

    friend std::ostream& operator<<(std::ostream& os, const parser_event& event)
    {
        os << "{type: " << event.type_ << ", value: " << event.value_ << ", match: " << jsonsl_strmatchtype(event.match_) << "}";
        return os;
    }
};

struct parser_state {
    std::string buffer{};
    std::vector<parser_event> events{};

    std::string last_key_{};
};

static void
action_callback(jsonsl_t lexer, jsonsl_action_t action, struct jsonsl_state_st* state, const jsonsl_char_t* /* at */)
{
    auto* parser = static_cast<parser_state*>(lexer->data);
    jsonsl_jpr_match_t match = JSONSL_MATCH_UNKNOWN;

    if (action == JSONSL_ACTION_PUSH) {
        if (state->type != JSONSL_T_HKEY) {
            auto key = std::move(parser->last_key_);
            jsonsl_jpr_match_state(lexer, state, key.data(), key.size(), &match);
        }
        switch (state->type) {
            case JSONSL_T_OBJECT:
                parser->events.emplace_back(parser_event_type::start_object, match);
                break;

            case JSONSL_T_LIST:
                parser->events.emplace_back(parser_event_type::start_array, match);
                break;

            case JSONSL_T_HKEY:
                parser->events.emplace_back(parser_event_type::key, match);
                break;

            case JSONSL_T_STRING:
                parser->events.emplace_back(parser_event_type::string, match);
                break;

            case JSONSL_T_SPECIAL:
                parser->events.emplace_back(parser_event_type::constant, match);
                break;
        }
    } else if (action == JSONSL_ACTION_POP) {
        switch (state->type) {
            case JSONSL_T_STRING:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin + 1, state->pos_cur - state->pos_begin - 1);
                break;

            case JSONSL_T_HKEY:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin + 1, state->pos_cur - state->pos_begin - 1);
                parser->last_key_ = parser->events[parser->events.size() - 1].value_;
                break;

            case JSONSL_T_SPECIAL:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin);
                if ((state->special_flags & JSONSL_SPECIALf_NUMNOINT) != 0) {
                    parser->events[parser->events.size() - 1].type_ = parser_event_type::real;
                } else if ((state->special_flags & JSONSL_SPECIALf_NUMERIC) != 0) {
                    parser->events[parser->events.size() - 1].type_ = parser_event_type::integer;
                }
                break;

            case JSONSL_T_OBJECT:
                parser->events.emplace_back(parser_event_type::finish_object, match);
                break;

            case JSONSL_T_LIST:
                parser->events.emplace_back(parser_event_type::finish_array, match);
                break;
        }
    }
}

static int
error_callback(jsonsl_t /* lexer */, jsonsl_error_t error, struct jsonsl_state_st* /* state */, jsonsl_char_t* /* at */)
{
    fprintf(stderr, "error: %s\n", jsonsl_strerror(error));
    return 0;
}

void
jsonsl_feed(jsonsl_t lexer, std::string_view bytes)
{
    auto* parser = static_cast<parser_state*>(lexer->data);
    parser->buffer.append(bytes);
    return jsonsl_feed(lexer, bytes.data(), bytes.size());
}

TEST_CASE("unit: jsonsl parse whole document", "[unit]")
{
    jsonsl_t lexer = jsonsl_new(512);
    REQUIRE(lexer != nullptr);
    REQUIRE(lexer->levels_max == 512);
    REQUIRE(lexer->jpr_count == 0);

    parser_state state{};
    lexer->action_callback = action_callback;
    lexer->error_callback = error_callback;
    lexer->data = &state;
    jsonsl_enable_all_callbacks(lexer);

    REQUIRE(lexer->action_callback == action_callback);
    REQUIRE(lexer->error_callback == error_callback);

    jsonsl_feed(lexer, "{\"meta\"");
    jsonsl_feed(lexer, ":{");
    jsonsl_feed(lexer, "\"count\":5");
    jsonsl_feed(lexer, "}, \"resul");
    jsonsl_feed(lexer, "ts\": [");
    jsonsl_feed(lexer, "42,\"43");
    jsonsl_feed(lexer, "\",44,[3");
    jsonsl_feed(lexer, ".14,null,false],true]}");

    jsonsl_destroy(lexer);

    REQUIRE(state.buffer == R"({"meta":{"count":5}, "results": [42,"43",44,[3.14,null,false],true]})");
    REQUIRE(state.events.size() == 19);
    REQUIRE(state.events[0] == parser_event{ parser_event_type::start_object });
    REQUIRE(state.events[1] == parser_event{ parser_event_type::key, "meta" });
    REQUIRE(state.events[2] == parser_event{ parser_event_type::start_object });
    REQUIRE(state.events[3] == parser_event{ parser_event_type::key, "count" });
    REQUIRE(state.events[4] == parser_event{ parser_event_type::integer, "5" });
    REQUIRE(state.events[5] == parser_event{ parser_event_type::finish_object });
    REQUIRE(state.events[6] == parser_event{ parser_event_type::key, "results" });
    REQUIRE(state.events[7] == parser_event{ parser_event_type::start_array });
    REQUIRE(state.events[8] == parser_event{ parser_event_type::integer, "42" });
    REQUIRE(state.events[9] == parser_event{ parser_event_type::string, "43" });
    REQUIRE(state.events[10] == parser_event{ parser_event_type::integer, "44" });
    REQUIRE(state.events[11] == parser_event{ parser_event_type::start_array });
    REQUIRE(state.events[12] == parser_event{ parser_event_type::real, "3.14" });
    REQUIRE(state.events[13] == parser_event{ parser_event_type::constant, "null" });
    REQUIRE(state.events[14] == parser_event{ parser_event_type::constant, "false" });
    REQUIRE(state.events[15] == parser_event{ parser_event_type::finish_array });
    REQUIRE(state.events[16] == parser_event{ parser_event_type::constant, "true" });
    REQUIRE(state.events[17] == parser_event{ parser_event_type::finish_array });
    REQUIRE(state.events[18] == parser_event{ parser_event_type::finish_object });
}

TEST_CASE("unit: jsonsl parse with JSON pointer", "[unit]")
{
    jsonsl_error_t error;

    error = JSONSL_ERROR_SUCCESS;
    REQUIRE(jsonsl_jpr_new(nullptr, &error) == nullptr);
    REQUIRE(error == JSONSL_ERROR_JPR_NOROOT);

    error = JSONSL_ERROR_SUCCESS;
    REQUIRE(jsonsl_jpr_new("results/^", &error) == nullptr);
    REQUIRE(error == JSONSL_ERROR_JPR_NOROOT);

    error = JSONSL_ERROR_SUCCESS;
    REQUIRE(jsonsl_jpr_new("/%A", &error) == nullptr);
    REQUIRE(error == JSONSL_ERROR_JPR_BADPATH);

    error = JSONSL_ERROR_SUCCESS;
    jsonsl_jpr_t pointer = jsonsl_jpr_new("/results/^", &error);
    REQUIRE(pointer != nullptr);
    REQUIRE(error == JSONSL_ERROR_SUCCESS);
    REQUIRE(std::string(pointer->orig, pointer->norig) == "/results/^");
    REQUIRE(pointer->ncomponents == 3);
    REQUIRE(pointer->components[0].ptype == JSONSL_PATH_ROOT);
    REQUIRE(pointer->components[1].ptype == JSONSL_PATH_STRING);
    REQUIRE(std::string(pointer->components[1].pstr, pointer->components[1].len) == "results");
    REQUIRE(pointer->components[2].ptype == JSONSL_PATH_WILDCARD);

    jsonsl_t lexer = jsonsl_new(512);
    REQUIRE(lexer != nullptr);
    REQUIRE(lexer->levels_max == 512);
    REQUIRE(lexer->jpr_count == 0);

    parser_state state{};
    lexer->action_callback = action_callback;
    lexer->error_callback = error_callback;
    lexer->data = &state;
    jsonsl_enable_all_callbacks(lexer);

    REQUIRE(lexer->action_callback == action_callback);
    REQUIRE(lexer->error_callback == error_callback);

    jsonsl_jpr_match_state_init(lexer, &pointer, 1);
    REQUIRE(lexer->jpr_count == 1);
    REQUIRE(lexer->jprs[0] == pointer);

    jsonsl_feed(lexer, "{\"meta\"");
    jsonsl_feed(lexer, ":{");
    jsonsl_feed(lexer, "\"count\":5");
    jsonsl_feed(lexer, "}, \"resul");
    jsonsl_feed(lexer, "ts\": [");
    jsonsl_feed(lexer, "42,\"43");
    jsonsl_feed(lexer, "\",44,[3");
    jsonsl_feed(lexer, ".14,null,false],true]}");

    jsonsl_jpr_match_state_cleanup(lexer);
    jsonsl_destroy(lexer);
    jsonsl_jpr_destroy(pointer);

    REQUIRE(state.buffer == R"({"meta":{"count":5}, "results": [42,"43",44,[3.14,null,false],true]})");
    REQUIRE(state.events.size() == 19);
    REQUIRE(state.events[0] == parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE));
    REQUIRE(state.events[1] == parser_event(parser_event_type::key, "meta"));
    REQUIRE(state.events[2] == parser_event(parser_event_type::start_object));
    REQUIRE(state.events[3] == parser_event(parser_event_type::key, "count"));
    REQUIRE(state.events[4] == parser_event(parser_event_type::integer, "5"));
    REQUIRE(state.events[5] == parser_event(parser_event_type::finish_object));
    REQUIRE(state.events[6] == parser_event(parser_event_type::key, "results"));
    REQUIRE(state.events[7] == parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE));
    REQUIRE(state.events[8] == parser_event(parser_event_type::integer, "42", JSONSL_MATCH_COMPLETE));
    REQUIRE(state.events[9] == parser_event(parser_event_type::string, "43", JSONSL_MATCH_COMPLETE));
    REQUIRE(state.events[10] == parser_event(parser_event_type::integer, "44", JSONSL_MATCH_COMPLETE));
    REQUIRE(state.events[11] == parser_event(parser_event_type::start_array, "", JSONSL_MATCH_COMPLETE));
    REQUIRE(state.events[12] == parser_event(parser_event_type::real, "3.14"));
    REQUIRE(state.events[13] == parser_event(parser_event_type::constant, "null"));
    REQUIRE(state.events[14] == parser_event(parser_event_type::constant, "false"));
    REQUIRE(state.events[15] == parser_event(parser_event_type::finish_array));
    REQUIRE(state.events[16] == parser_event(parser_event_type::constant, "true", JSONSL_MATCH_COMPLETE));
    REQUIRE(state.events[17] == parser_event(parser_event_type::finish_array));
    REQUIRE(state.events[18] == parser_event(parser_event_type::finish_object));
}

static void
shallow_action_callback(jsonsl_t lexer, jsonsl_action_t action, struct jsonsl_state_st* state, const jsonsl_char_t* /* at */)
{
    auto* parser = static_cast<parser_state*>(lexer->data);
    jsonsl_jpr_match_t match = JSONSL_MATCH_UNKNOWN;

    if (action == JSONSL_ACTION_PUSH) {
        if (state->type != JSONSL_T_HKEY) {
            auto key = std::move(parser->last_key_);
            jsonsl_jpr_match_state(lexer, state, key.data(), key.size(), &match);
        }
        switch (state->type) {
            case JSONSL_T_OBJECT:
                parser->events.emplace_back(parser_event_type::start_object, match);
                break;

            case JSONSL_T_LIST:
                parser->events.emplace_back(parser_event_type::start_array, match);
                break;

            case JSONSL_T_HKEY:
                parser->events.emplace_back(parser_event_type::key, match);
                break;

            case JSONSL_T_STRING:
                parser->events.emplace_back(parser_event_type::string, match);
                break;

            case JSONSL_T_SPECIAL:
                parser->events.emplace_back(parser_event_type::constant, match);
                break;
        }
    } else if (action == JSONSL_ACTION_POP) {
        bool include_value_with_container = state->level == lexer->max_callback_level - 1;

        switch (state->type) {
            case JSONSL_T_STRING:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin + 1, state->pos_cur - state->pos_begin - 1);
                break;

            case JSONSL_T_HKEY:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin + 1, state->pos_cur - state->pos_begin - 1);
                parser->last_key_ = parser->events[parser->events.size() - 1].value_;
                break;

            case JSONSL_T_SPECIAL:
                parser->events[parser->events.size() - 1].value_ =
                  parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin);
                if ((state->special_flags & JSONSL_SPECIALf_NUMNOINT) != 0) {
                    parser->events[parser->events.size() - 1].type_ = parser_event_type::real;
                } else if ((state->special_flags & JSONSL_SPECIALf_NUMERIC) != 0) {
                    parser->events[parser->events.size() - 1].type_ = parser_event_type::integer;
                }
                break;

            case JSONSL_T_OBJECT:
                parser->events.emplace_back(
                  parser_event_type::finish_object,
                  include_value_with_container ? parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin + 1) : "",
                  match);
                break;

            case JSONSL_T_LIST:
                parser->events.emplace_back(
                  parser_event_type::finish_array,
                  include_value_with_container ? parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin + 1) : "",
                  match);
                break;
        }
    }
}

TEST_CASE("unit: jsonsl parse with limited depth and JSON pointer", "[unit]")
{
    jsonsl_error_t error;

    error = JSONSL_ERROR_SUCCESS;
    jsonsl_jpr_t pointer = jsonsl_jpr_new("/results/^", &error);
    REQUIRE(pointer != nullptr);
    REQUIRE(error == JSONSL_ERROR_SUCCESS);
    REQUIRE(std::string(pointer->orig, pointer->norig) == "/results/^");
    REQUIRE(pointer->ncomponents == 3);
    REQUIRE(pointer->components[0].ptype == JSONSL_PATH_ROOT);
    REQUIRE(pointer->components[1].ptype == JSONSL_PATH_STRING);
    REQUIRE(std::string(pointer->components[1].pstr, pointer->components[1].len) == "results");
    REQUIRE(pointer->components[2].ptype == JSONSL_PATH_WILDCARD);

    // depth = 3
    {
        jsonsl_t lexer = jsonsl_new(512);
        REQUIRE(lexer != nullptr);
        REQUIRE(lexer->levels_max == 512);
        REQUIRE(lexer->jpr_count == 0);

        parser_state state{};
        lexer->action_callback = shallow_action_callback;
        lexer->error_callback = error_callback;
        lexer->data = &state;
        jsonsl_enable_all_callbacks(lexer);

        REQUIRE(lexer->action_callback == shallow_action_callback);
        REQUIRE(lexer->error_callback == error_callback);

        jsonsl_jpr_match_state_init(lexer, &pointer, 1);
        REQUIRE(lexer->jpr_count == 1);
        REQUIRE(lexer->jprs[0] == pointer);
        lexer->max_callback_level = 3;

        jsonsl_feed(lexer, "{\"meta\"");
        jsonsl_feed(lexer, ":{");
        jsonsl_feed(lexer, "\"count\":5");
        jsonsl_feed(lexer, "}, \"resul");
        jsonsl_feed(lexer, "ts\": [");
        jsonsl_feed(lexer, "42,\"43");
        jsonsl_feed(lexer, "\",44,[3");
        jsonsl_feed(lexer, ".14,null,false],true]}");

        jsonsl_jpr_match_state_cleanup(lexer);
        jsonsl_destroy(lexer);

        REQUIRE(state.buffer == R"({"meta":{"count":5}, "results": [42,"43",44,[3.14,null,false],true]})");
        REQUIRE(state.events.size() == 8);
        REQUIRE(state.events[0] == parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE));
        REQUIRE(state.events[1] == parser_event(parser_event_type::key, "meta"));
        REQUIRE(state.events[2] == parser_event(parser_event_type::start_object, JSONSL_MATCH_NOMATCH));
        REQUIRE(state.events[3] == parser_event(parser_event_type::finish_object, R"({"count":5})"));
        REQUIRE(state.events[4] == parser_event(parser_event_type::key, "results"));
        REQUIRE(state.events[5] == parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE));
        REQUIRE(state.events[6] == parser_event(parser_event_type::finish_array, R"([42,"43",44,[3.14,null,false],true])"));
        REQUIRE(state.events[7] == parser_event(parser_event_type::finish_object));
    }

    // depth = 4
    {
        jsonsl_t lexer = jsonsl_new(512);
        REQUIRE(lexer != nullptr);
        REQUIRE(lexer->levels_max == 512);
        REQUIRE(lexer->jpr_count == 0);

        parser_state state{};
        lexer->action_callback = shallow_action_callback;
        lexer->error_callback = error_callback;
        lexer->data = &state;
        jsonsl_enable_all_callbacks(lexer);

        REQUIRE(lexer->action_callback == shallow_action_callback);
        REQUIRE(lexer->error_callback == error_callback);

        jsonsl_jpr_match_state_init(lexer, &pointer, 1);
        REQUIRE(lexer->jpr_count == 1);
        REQUIRE(lexer->jprs[0] == pointer);
        lexer->max_callback_level = 4;

        jsonsl_feed(lexer, "{\"meta\"");
        jsonsl_feed(lexer, ":{");
        jsonsl_feed(lexer, "\"count\":5");
        jsonsl_feed(lexer, "}, \"resul");
        jsonsl_feed(lexer, "ts\": [");
        jsonsl_feed(lexer, "42,\"43");
        jsonsl_feed(lexer, "\",44,[3");
        jsonsl_feed(lexer, ".14,null,false],true]}");

        jsonsl_jpr_match_state_cleanup(lexer);
        jsonsl_destroy(lexer);

        REQUIRE(state.buffer == R"({"meta":{"count":5}, "results": [42,"43",44,[3.14,null,false],true]})");
        REQUIRE(state.events.size() == 16);
        REQUIRE(state.events[0] == parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE));
        REQUIRE(state.events[1] == parser_event(parser_event_type::key, "meta"));
        REQUIRE(state.events[2] == parser_event(parser_event_type::start_object));
        REQUIRE(state.events[3] == parser_event(parser_event_type::key, "count"));
        REQUIRE(state.events[4] == parser_event(parser_event_type::integer, "5"));
        REQUIRE(state.events[5] == parser_event(parser_event_type::finish_object));
        REQUIRE(state.events[6] == parser_event(parser_event_type::key, "results"));
        REQUIRE(state.events[7] == parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE));
        REQUIRE(state.events[8] == parser_event(parser_event_type::integer, "42", JSONSL_MATCH_COMPLETE));
        REQUIRE(state.events[9] == parser_event(parser_event_type::string, "43", JSONSL_MATCH_COMPLETE));
        REQUIRE(state.events[10] == parser_event(parser_event_type::integer, "44", JSONSL_MATCH_COMPLETE));
        REQUIRE(state.events[11] == parser_event(parser_event_type::start_array, "", JSONSL_MATCH_COMPLETE));
        REQUIRE(state.events[12] == parser_event(parser_event_type::finish_array, "[3.14,null,false]"));
        REQUIRE(state.events[13] == parser_event(parser_event_type::constant, "true", JSONSL_MATCH_COMPLETE));
        REQUIRE(state.events[14] == parser_event(parser_event_type::finish_array));
        REQUIRE(state.events[15] == parser_event(parser_event_type::finish_object));
    }

    jsonsl_jpr_destroy(pointer);
}
