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

#include "framework/test_registry.hxx"

#include <jsonsl.h>

#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace couchbase::test
{
namespace
{
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

auto
event_type_name(parser_event_type type) -> std::string
{
  switch (type) {
    case parser_event_type::start_object:
      return "start_object";
    case parser_event_type::finish_object:
      return "finish_object";
    case parser_event_type::start_array:
      return "start_array";
    case parser_event_type::finish_array:
      return "finish_array";
    case parser_event_type::key:
      return "key";
    case parser_event_type::constant:
      return "constant";
    case parser_event_type::string:
      return "string";
    case parser_event_type::integer:
      return "integer";
    case parser_event_type::real:
      return "real";
  }
  return "unknown";
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
};

struct parser_state {
  std::string buffer{};
  std::vector<parser_event> events{};

  std::string last_key_{};
};
} // namespace

template<>
struct operand_printer<parser_event> {
  static constexpr bool available = true;
  [[nodiscard]] static auto to_text(const parser_event& event) -> std::string
  {
    return "{type: " + event_type_name(event.type_) + ", value: " + event.value_ +
           ", match: " + jsonsl_strmatchtype(event.match_) + "}";
  }
};

namespace
{
void
action_callback(jsonsl_t lexer,
                jsonsl_action_t action,
                struct jsonsl_state_st* state,
                const jsonsl_char_t* /* at */)
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

// Reports a container's own text with the event that closes it, for the container one level above
// the callback cut-off. That is the level at which a caller that stops descending still needs the
// bytes it did not receive events for.
void
shallow_action_callback(jsonsl_t lexer,
                        jsonsl_action_t action,
                        struct jsonsl_state_st* state,
                        const jsonsl_char_t* /* at */)
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
          include_value_with_container
            ? parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin + 1)
            : "",
          match);
        break;

      case JSONSL_T_LIST:
        parser->events.emplace_back(
          parser_event_type::finish_array,
          include_value_with_container
            ? parser->buffer.substr(state->pos_begin, state->pos_cur - state->pos_begin + 1)
            : "",
          match);
        break;
    }
  }
}

int
error_callback(jsonsl_t /* lexer */,
               jsonsl_error_t error,
               struct jsonsl_state_st* /* state */,
               jsonsl_char_t* /* at */)
{
  std::fprintf(stderr, "error: %s\n", jsonsl_strerror(error));
  return 0;
}

// The lexer is fed the raw bytes; the buffer the callbacks index into is kept beside it, because
// jsonsl reports positions in the stream rather than in the chunk it was given.
void
feed(jsonsl_t lexer, std::string_view bytes)
{
  auto* parser = static_cast<parser_state*>(lexer->data);
  parser->buffer.append(bytes);
  jsonsl_feed(lexer, bytes.data(), bytes.size());
}

// One document, split so that a key, a string and a number each straddle a chunk boundary.
void
feed_sample_document(jsonsl_t lexer)
{
  feed(lexer, R"({"meta")");
  feed(lexer, ":{");
  feed(lexer, R"("count":5)");
  feed(lexer, R"(}, "resul)");
  feed(lexer, R"(ts": [)");
  feed(lexer, R"(42,"43)");
  feed(lexer, R"(",44,[3)");
  feed(lexer, ".14,null,false],true]}");
}

const std::string sample_document{
  R"({"meta":{"count":5}, "results": [42,"43",44,[3.14,null,false],true]})"
};

// Asserts the shape a "/results/^" pointer has once parsed, so a failure downstream is attributed
// to the pointer rather than to the document.
void
assert_results_pointer(jsonsl_jpr_t pointer)
{
  assert_eq(std::string(pointer->orig, pointer->norig),
            std::string{ "/results/^" },
            "the pointer keeps the expression it was built from");
  assert_eq(pointer->ncomponents, std::size_t{ 3 }, "root, key and wildcard");
  assert_eq(pointer->components[0].ptype, JSONSL_PATH_ROOT, "the first component");
  assert_eq(pointer->components[1].ptype, JSONSL_PATH_STRING, "the second component");
  assert_eq(std::string(pointer->components[1].pstr, pointer->components[1].len),
            std::string{ "results" },
            "the second component's key");
  assert_eq(pointer->components[2].ptype, JSONSL_PATH_WILDCARD, "the third component");
}

void
assert_new_lexer(jsonsl_t lexer)
{
  assert_true(lexer != nullptr, "the lexer is allocated");
  assert_eq(lexer->levels_max, 512U, "the nesting limit it was asked for");
  assert_eq(lexer->jpr_count, std::size_t{ 0 }, "a new lexer carries no pointer");
}

void
every_token_of_a_chunked_document_is_reported([[maybe_unused]] context& ctx)
{
  jsonsl_t lexer = jsonsl_new(512);
  assert_new_lexer(lexer);

  parser_state state{};
  lexer->action_callback = action_callback;
  lexer->error_callback = error_callback;
  lexer->data = &state;
  jsonsl_enable_all_callbacks(lexer);

  assert_true(lexer->action_callback == action_callback, "the action callback is installed");
  assert_true(lexer->error_callback == error_callback, "the error callback is installed");

  feed_sample_document(lexer);
  jsonsl_destroy(lexer);

  assert_eq(state.buffer, sample_document, "every chunk reached the lexer");
  assert_eq(state.events.size(), std::size_t{ 19 }, "the number of events");
  assert_eq(state.events[0], parser_event{ parser_event_type::start_object }, "the root object");
  assert_eq(state.events[1], parser_event{ parser_event_type::key, "meta" }, "the meta key");
  assert_eq(state.events[2], parser_event{ parser_event_type::start_object }, "the meta object");
  assert_eq(state.events[3], parser_event{ parser_event_type::key, "count" }, "the count key");
  assert_eq(state.events[4], parser_event{ parser_event_type::integer, "5" }, "the count value");
  assert_eq(
    state.events[5], parser_event{ parser_event_type::finish_object }, "the meta object closes");
  assert_eq(state.events[6], parser_event{ parser_event_type::key, "results" }, "the results key");
  assert_eq(state.events[7], parser_event{ parser_event_type::start_array }, "the results array");
  assert_eq(state.events[8], parser_event{ parser_event_type::integer, "42" }, "the first row");
  assert_eq(state.events[9],
            parser_event{ parser_event_type::string, "43" },
            "the second row, split across two chunks");
  assert_eq(state.events[10], parser_event{ parser_event_type::integer, "44" }, "the third row");
  assert_eq(state.events[11], parser_event{ parser_event_type::start_array }, "the fourth row");
  assert_eq(state.events[12],
            parser_event{ parser_event_type::real, "3.14" },
            "a number with a fraction is a real");
  assert_eq(state.events[13], parser_event{ parser_event_type::constant, "null" }, "a null");
  assert_eq(state.events[14], parser_event{ parser_event_type::constant, "false" }, "a false");
  assert_eq(
    state.events[15], parser_event{ parser_event_type::finish_array }, "the fourth row closes");
  assert_eq(state.events[16], parser_event{ parser_event_type::constant, "true" }, "the fifth row");
  assert_eq(
    state.events[17], parser_event{ parser_event_type::finish_array }, "the results array closes");
  assert_eq(
    state.events[18], parser_event{ parser_event_type::finish_object }, "the root object closes");
}

void
a_json_pointer_marks_the_events_it_matches([[maybe_unused]] context& ctx)
{
  jsonsl_error_t error = JSONSL_ERROR_SUCCESS;
  assert_true(jsonsl_jpr_new(nullptr, &error) == nullptr, "an absent expression is rejected");
  assert_eq(error, JSONSL_ERROR_JPR_NOROOT, "an absent expression names no root");

  error = JSONSL_ERROR_SUCCESS;
  assert_true(jsonsl_jpr_new("results/^", &error) == nullptr,
              "an expression with no leading slash is rejected");
  assert_eq(error, JSONSL_ERROR_JPR_NOROOT, "a relative expression names no root");

  error = JSONSL_ERROR_SUCCESS;
  assert_true(jsonsl_jpr_new("/%A", &error) == nullptr, "an incomplete percent escape is rejected");
  assert_eq(error, JSONSL_ERROR_JPR_BADPATH, "an incomplete percent escape is a bad path");

  error = JSONSL_ERROR_SUCCESS;
  jsonsl_jpr_t pointer = jsonsl_jpr_new("/results/^", &error);
  assert_true(pointer != nullptr, "a well-formed expression yields a pointer");
  assert_eq(error, JSONSL_ERROR_SUCCESS, "and reports no error");
  assert_results_pointer(pointer);

  jsonsl_t lexer = jsonsl_new(512);
  assert_new_lexer(lexer);

  parser_state state{};
  lexer->action_callback = action_callback;
  lexer->error_callback = error_callback;
  lexer->data = &state;
  jsonsl_enable_all_callbacks(lexer);

  assert_true(lexer->action_callback == action_callback, "the action callback is installed");
  assert_true(lexer->error_callback == error_callback, "the error callback is installed");

  jsonsl_jpr_match_state_init(lexer, &pointer, 1);
  assert_eq(lexer->jpr_count, std::size_t{ 1 }, "the lexer holds one pointer");
  assert_true(lexer->jprs[0] == pointer, "and it is the one that was installed");

  feed_sample_document(lexer);

  jsonsl_jpr_match_state_cleanup(lexer);
  jsonsl_destroy(lexer);
  jsonsl_jpr_destroy(pointer);

  assert_eq(state.buffer, sample_document, "every chunk reached the lexer");
  assert_eq(state.events.size(), std::size_t{ 19 }, "the number of events");
  assert_eq(state.events[0],
            parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE),
            "the root object could still contain a match");
  assert_eq(state.events[1], parser_event(parser_event_type::key, "meta"), "the meta key");
  assert_eq(state.events[2], parser_event(parser_event_type::start_object), "the meta object");
  assert_eq(state.events[3], parser_event(parser_event_type::key, "count"), "the count key");
  assert_eq(state.events[4], parser_event(parser_event_type::integer, "5"), "the count value");
  assert_eq(
    state.events[5], parser_event(parser_event_type::finish_object), "the meta object closes");
  assert_eq(state.events[6], parser_event(parser_event_type::key, "results"), "the results key");
  assert_eq(state.events[7],
            parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE),
            "the results array could still contain a match");
  assert_eq(state.events[8],
            parser_event(parser_event_type::integer, "42", JSONSL_MATCH_COMPLETE),
            "the first row matches the wildcard");
  assert_eq(state.events[9],
            parser_event(parser_event_type::string, "43", JSONSL_MATCH_COMPLETE),
            "the second row matches the wildcard");
  assert_eq(state.events[10],
            parser_event(parser_event_type::integer, "44", JSONSL_MATCH_COMPLETE),
            "the third row matches the wildcard");
  assert_eq(state.events[11],
            parser_event(parser_event_type::start_array, "", JSONSL_MATCH_COMPLETE),
            "the fourth row matches the wildcard");
  assert_eq(state.events[12],
            parser_event(parser_event_type::real, "3.14"),
            "a value below a matched row does not itself match");
  assert_eq(state.events[13], parser_event(parser_event_type::constant, "null"), "a null");
  assert_eq(state.events[14], parser_event(parser_event_type::constant, "false"), "a false");
  assert_eq(
    state.events[15], parser_event(parser_event_type::finish_array), "the fourth row closes");
  assert_eq(state.events[16],
            parser_event(parser_event_type::constant, "true", JSONSL_MATCH_COMPLETE),
            "the fifth row matches the wildcard");
  assert_eq(
    state.events[17], parser_event(parser_event_type::finish_array), "the results array closes");
  assert_eq(
    state.events[18], parser_event(parser_event_type::finish_object), "the root object closes");
}

void
the_callback_level_bounds_how_far_the_lexer_descends([[maybe_unused]] context& ctx)
{
  jsonsl_error_t error = JSONSL_ERROR_SUCCESS;
  jsonsl_jpr_t pointer = jsonsl_jpr_new("/results/^", &error);
  assert_true(pointer != nullptr, "a well-formed expression yields a pointer");
  assert_eq(error, JSONSL_ERROR_SUCCESS, "and reports no error");
  assert_results_pointer(pointer);

  {
    jsonsl_t lexer = jsonsl_new(512);
    assert_new_lexer(lexer);

    parser_state state{};
    lexer->action_callback = shallow_action_callback;
    lexer->error_callback = error_callback;
    lexer->data = &state;
    jsonsl_enable_all_callbacks(lexer);

    assert_true(lexer->action_callback == shallow_action_callback,
                "the action callback is installed");
    assert_true(lexer->error_callback == error_callback, "the error callback is installed");

    jsonsl_jpr_match_state_init(lexer, &pointer, 1);
    assert_eq(lexer->jpr_count, std::size_t{ 1 }, "the lexer holds one pointer");
    assert_true(lexer->jprs[0] == pointer, "and it is the one that was installed");
    lexer->max_callback_level = 3;

    feed_sample_document(lexer);

    jsonsl_jpr_match_state_cleanup(lexer);
    jsonsl_destroy(lexer);

    assert_eq(state.buffer, sample_document, "every chunk reached the lexer");
    assert_eq(state.events.size(), std::size_t{ 8 }, "the number of events at level 3");
    assert_eq(state.events[0],
              parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE),
              "the root object could still contain a match");
    assert_eq(state.events[1], parser_event(parser_event_type::key, "meta"), "the meta key");
    assert_eq(state.events[2],
              parser_event(parser_event_type::start_object, JSONSL_MATCH_NOMATCH),
              "the meta object cannot contain a match");
    assert_eq(state.events[3],
              parser_event(parser_event_type::finish_object, R"({"count":5})"),
              "the meta object is reported whole rather than token by token");
    assert_eq(state.events[4], parser_event(parser_event_type::key, "results"), "the results key");
    assert_eq(state.events[5],
              parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE),
              "the results array could still contain a match");
    assert_eq(
      state.events[6],
      parser_event(parser_event_type::finish_array, R"([42,"43",44,[3.14,null,false],true])"),
      "the results array is reported whole rather than row by row");
    assert_eq(
      state.events[7], parser_event(parser_event_type::finish_object), "the root object closes");
  }

  {
    jsonsl_t lexer = jsonsl_new(512);
    assert_new_lexer(lexer);

    parser_state state{};
    lexer->action_callback = shallow_action_callback;
    lexer->error_callback = error_callback;
    lexer->data = &state;
    jsonsl_enable_all_callbacks(lexer);

    assert_true(lexer->action_callback == shallow_action_callback,
                "the action callback is installed");
    assert_true(lexer->error_callback == error_callback, "the error callback is installed");

    jsonsl_jpr_match_state_init(lexer, &pointer, 1);
    assert_eq(lexer->jpr_count, std::size_t{ 1 }, "the lexer holds one pointer");
    assert_true(lexer->jprs[0] == pointer, "and it is the one that was installed");
    lexer->max_callback_level = 4;

    feed_sample_document(lexer);

    jsonsl_jpr_match_state_cleanup(lexer);
    jsonsl_destroy(lexer);

    assert_eq(state.buffer, sample_document, "every chunk reached the lexer");
    assert_eq(state.events.size(), std::size_t{ 16 }, "the number of events at level 4");
    assert_eq(state.events[0],
              parser_event(parser_event_type::start_object, JSONSL_MATCH_POSSIBLE),
              "the root object could still contain a match");
    assert_eq(state.events[1], parser_event(parser_event_type::key, "meta"), "the meta key");
    assert_eq(state.events[2], parser_event(parser_event_type::start_object), "the meta object");
    assert_eq(state.events[3], parser_event(parser_event_type::key, "count"), "the count key");
    assert_eq(state.events[4], parser_event(parser_event_type::integer, "5"), "the count value");
    assert_eq(
      state.events[5], parser_event(parser_event_type::finish_object), "the meta object closes");
    assert_eq(state.events[6], parser_event(parser_event_type::key, "results"), "the results key");
    assert_eq(state.events[7],
              parser_event(parser_event_type::start_array, JSONSL_MATCH_POSSIBLE),
              "the results array could still contain a match");
    assert_eq(state.events[8],
              parser_event(parser_event_type::integer, "42", JSONSL_MATCH_COMPLETE),
              "the first row matches the wildcard");
    assert_eq(state.events[9],
              parser_event(parser_event_type::string, "43", JSONSL_MATCH_COMPLETE),
              "the second row matches the wildcard");
    assert_eq(state.events[10],
              parser_event(parser_event_type::integer, "44", JSONSL_MATCH_COMPLETE),
              "the third row matches the wildcard");
    assert_eq(state.events[11],
              parser_event(parser_event_type::start_array, "", JSONSL_MATCH_COMPLETE),
              "the fourth row matches the wildcard");
    assert_eq(state.events[12],
              parser_event(parser_event_type::finish_array, "[3.14,null,false]"),
              "the fourth row is reported whole rather than element by element");
    assert_eq(state.events[13],
              parser_event(parser_event_type::constant, "true", JSONSL_MATCH_COMPLETE),
              "the fifth row matches the wildcard");
    assert_eq(
      state.events[14], parser_event(parser_event_type::finish_array), "the results array closes");
    assert_eq(
      state.events[15], parser_event(parser_event_type::finish_object), "the root object closes");
  }

  jsonsl_jpr_destroy(pointer);
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(every_token_of_a_chunked_document_is_reported) },
      { CASE(a_json_pointer_marks_the_events_it_matches) },
      { CASE(the_callback_level_bounds_how_far_the_lexer_descends) },
    },
  };
}

} // namespace couchbase::test
