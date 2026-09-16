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

#include "framework/test_registry.hxx"

#include <couchbase/logger.hxx>

#include "core/document_id.hxx"
#include "core/document_id_fmt.hxx"
#include "core/document_id_redaction.hxx"
#include "core/logger/logger.hxx"
#include "core/logger/redaction.hxx"
#include "core/origin.hxx"
#include "core/utils/connection_string.hxx"

#include <spdlog/fmt/bin_to_hex.h>
#include <spdlog/fmt/bundled/format.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace couchbase::test
{
namespace
{
// Counts how many times its fmt formatter runs, so a case can assert whether the logging macro
// evaluated (formatted) its arguments at all.
std::atomic<int> probe_format_count{ 0 };
struct format_probe {
};
} // namespace
} // namespace couchbase::test

template<>
struct fmt::formatter<couchbase::test::format_probe> : fmt::formatter<std::string_view> {
  auto format(couchbase::test::format_probe /*probe*/, fmt::format_context& ctx) const
    -> decltype(ctx.out())
  {
    couchbase::test::probe_format_count.fetch_add(1, std::memory_order_relaxed);
    return fmt::formatter<std::string_view>::format("probe", ctx);
  }
};

namespace couchbase::test
{
namespace
{
auto
capture_entry(std::string_view msg, couchbase::logger::log_location location) -> std::string
{
  return std::string(msg) + " [" + location.file + ":" + std::to_string(location.line) + " " +
         location.function + "]";
}

// Restores the logger state the cases mutate, however the case leaves. The callback and the levels
// are process-wide: a callback left registered holds a reference to a vector that dies with the
// case, a level left at off is inherited by whatever runs next, and an assertion failure unwinds
// past any restore written at the end of a body.
//
// Each logger's level is saved on its own because the core API offers no per-logger accessor:
// get_lowest_log_level() is a minimum across every registered logger and set_log_levels() then
// writes one value to all of them, so saving and restoring through that pair collapses loggers
// sitting at different levels onto the most verbose of them.
class logger_state_guard
{
public:
  logger_state_guard()
  {
    spdlog::apply_all([this](const std::shared_ptr<spdlog::logger>& logger) {
      levels_.emplace_back(logger->name(), logger->level());
    });
  }

  logger_state_guard(const logger_state_guard&) = delete;
  logger_state_guard(logger_state_guard&&) = delete;
  auto operator=(const logger_state_guard&) -> logger_state_guard& = delete;
  auto operator=(logger_state_guard&&) -> logger_state_guard& = delete;

  ~logger_state_guard()
  {
    couchbase::logger::unregister_log_callback();
    for (const auto& [name, level] : levels_) {
      if (const auto logger = spdlog::get(name); logger != nullptr) {
        logger->set_level(level);
      }
    }
  }

private:
  std::vector<std::pair<std::string, spdlog::level::level_enum>> levels_{};
};

// The annotation wrappers live in the core logger, and this file also drives couchbase::logger
// above. Aliased rather than qualified at every call so the two never read as the same thing.
namespace core_logger = couchbase::core::logger;

// Log redaction is process-wide state, so restore it even if an assertion fails part way through
// a case, otherwise the leak shows up as an unrelated failure elsewhere in the suite.
class scoped_log_redaction
{
public:
  explicit scoped_log_redaction(bool enable)
    : previous_{ core_logger::is_log_redaction_enabled() }
  {
    core_logger::set_log_redaction(enable);
  }

  scoped_log_redaction(const scoped_log_redaction&) = delete;
  scoped_log_redaction(scoped_log_redaction&&) = delete;
  auto operator=(const scoped_log_redaction&) -> scoped_log_redaction& = delete;
  auto operator=(scoped_log_redaction&&) -> scoped_log_redaction& = delete;

  ~scoped_log_redaction()
  {
    core_logger::set_log_redaction(previous_);
  }

private:
  bool previous_;
};

void
a_registered_callback_receives_every_logged_message([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level /*level*/,
                                                            couchbase::logger::log_location loc) {
    captured_logs.push_back(capture_entry(msg, loc));
  });

  CB_LOG_INFO("Test log message 1");
  CB_LOG_WARNING("Test log message 2");

  assert_eq(captured_logs.size(), std::size_t{ 2 }, "both messages reach the callback");
  assert_contains(captured_logs[0], "Test log message 1", "the first message");
  assert_contains(captured_logs[1], "Test log message 2", "the second message");
}

void
the_callback_receives_the_level_of_each_message([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });

  CB_LOG_INFO("Test log message 1");
  CB_LOG_ERROR("Test log message 2");

  assert_eq(captured_logs.size(), std::size_t{ 1 }, "a callback filtering on error keeps one");
  assert_contains(captured_logs[0], "Test log message 2", "the message logged at error");
}

void
logging_with_no_callback_registered_does_not_throw([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  // register_log_callback(nullptr) installs nothing -- the public API returns before reaching the
  // core one -- so unregister_log_callback() is what leaves the logger with no callback.
  couchbase::logger::unregister_log_callback();
  couchbase::logger::register_log_callback(nullptr);

  assert_no_throw(
    [&]() {
      CB_LOG_INFO("Test log message 1");
    },
    "logging with no callback registered reaches no callback");
}

void
registering_a_second_callback_replaces_the_first([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;

  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });
  couchbase::logger::register_log_callback([&captured_logs](std::string_view msg,
                                                            couchbase::logger::log_level level,
                                                            couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::trace) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  });

  CB_LOG_ERROR("Test error message");
  CB_LOG_TRACE("Test trace message");

  assert_eq(captured_logs.size(), std::size_t{ 1 }, "only the second callback is delivered to");
  assert_contains(captured_logs[0], "Test trace message", "what the second callback kept");
}

void
unregistering_stops_delivery_until_a_callback_is_registered_again([[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured_logs;

  auto callback = [&captured_logs](std::string_view msg,
                                   couchbase::logger::log_level level,
                                   couchbase::logger::log_location loc) {
    if (level == couchbase::logger::log_level::error) {
      captured_logs.push_back(capture_entry(msg, loc));
    }
  };

  couchbase::logger::register_log_callback(callback);
  CB_LOG_ERROR("Test error message");

  couchbase::logger::unregister_log_callback();
  CB_LOG_ERROR("Test error message 2");

  couchbase::logger::register_log_callback(callback);
  CB_LOG_ERROR("Test error message 3");

  assert_eq(captured_logs.size(), std::size_t{ 2 }, "nothing is delivered while unregistered");
  assert_contains(captured_logs[0], "Test error message", "logged before unregistering");
  assert_contains(captured_logs[1], "Test error message 3", "logged after registering again");
}

void
arguments_are_not_formatted_when_logging_is_off_and_no_callback_is_registered(
  [[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  couchbase::logger::unregister_log_callback();
  couchbase::logger::set_level(couchbase::logger::log_level::off);
  probe_format_count.store(0);

  CB_LOG_TRACE("value={}", format_probe{});

  assert_eq(probe_format_count.load(), 0, "a suppressed message does not format its arguments");
}

void
a_registered_callback_receives_formatted_arguments_while_logging_is_off(
  [[maybe_unused]] context& ctx)
{
  const logger_state_guard guard;

  std::vector<std::string> captured;
  couchbase::logger::register_log_callback(
    [&captured](std::string_view msg,
                couchbase::logger::log_level /*level*/,
                couchbase::logger::log_location /*location*/) {
      captured.emplace_back(msg);
    });
  couchbase::logger::set_level(couchbase::logger::log_level::off);
  probe_format_count.store(0);

  CB_LOG_TRACE("value={}", format_probe{});

  assert_eq(probe_format_count.load(), 1, "the argument is formatted exactly once");
  assert_false(captured.empty(), "the message reaches the callback");
  assert_contains(captured.back(), "value=probe", "the formatted argument");
}

void
redaction_annotations_are_inert_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  assert_eq(fmt::format("key={}", core_logger::user_data("my_key")), "key=my_key", "user data");
  assert_eq(
    fmt::format("bucket={}", core_logger::metadata("my_bucket")), "bucket=my_bucket", "metadata");
  assert_eq(
    fmt::format("host={}", core_logger::system_data("127.0.0.1")), "host=127.0.0.1", "system data");
}

void
redaction_annotations_wrap_values_while_redaction_is_enabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  assert_eq(
    fmt::format("key={}", core_logger::user_data("my_key")), "key=<ud>my_key</ud>", "user data");
  assert_eq(fmt::format("bucket={}", core_logger::metadata("my_bucket")),
            "bucket=<md>my_bucket</md>",
            "metadata");
  assert_eq(fmt::format("host={}", core_logger::system_data("127.0.0.1")),
            "host=<sd>127.0.0.1</sd>",
            "system data");
}

// The tool that consumes these tags is line-oriented. A value carrying a newline puts the opening
// tag and the closing tag on different lines, and the tool then reports an unmatched tag and emits
// the value in clear text -- the one outcome the annotations exist to prevent. Observed against
// cblogredaction: an HTTP error body spanning lines came through the redacted log fully readable.
const std::string multiline_body{ "<html><head>\n<title>301</title></head>\r\n</html>" };

void
a_tagged_value_never_spans_log_lines([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("body={}", core_logger::user_data(multiline_body));
  assert_eq(tagged,
            R"(body=<ud><html><head>\n<title>301</title></head>\r\n</html></ud>)",
            "both line breaks are escaped inside the span");
  assert_eq(tagged.find('\n'), std::string::npos, "no newline survives");
  assert_eq(tagged.find('\r'), std::string::npos, "no carriage return survives");

  assert_eq(fmt::format("{}", core_logger::metadata(multiline_body)).find('\n'),
            std::string::npos,
            "metadata escapes the same way");
  assert_eq(fmt::format("{}", core_logger::system_data(multiline_body)).find('\n'),
            std::string::npos,
            "system data escapes the same way");
}

void
every_entry_of_a_tagged_list_is_escaped([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::vector<std::string> values{ "one\ntwo", "three" };
  assert_eq(core_logger::user_data_list(values),
            R"(<ud>one\ntwo</ud>, <ud>three</ud>)",
            "an entry is escaped like any other tagged value");
}

void
a_value_with_newlines_is_untouched_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  // Annotating a statement must not change what it prints for anyone who has not opted in.
  assert_eq(fmt::format("body={}", core_logger::user_data(multiline_body)),
            "body=" + multiline_body,
            "nothing is escaped while redaction is off");
}

void
the_exclusion_markers_never_escape_their_value([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // not_redacted() exists so a configuration dump shows the exact bytes that crossed the wire.
  assert_eq(fmt::format("{}", core_logger::not_redacted(multiline_body)),
            multiline_body,
            "not_redacted passes the value through");
  assert_eq(fmt::format("{}", core_logger::not_sensitive(multiline_body)),
            multiline_body,
            "not_sensitive passes the value through");
}

void
a_conditionally_tagged_value_is_only_tagged_on_the_sensitive_branch([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // Tagging a constant hashes it to a fixed digest, which protects nothing and leaves a reader
  // unable to tell a value the SDK withheld from one the redaction tool replaced.
  //
  // The values are typed rather than bare literals, matching what the call sites pass: cppcheck
  // reads a string literal beside a bool parameter as a boolean conversion and reports it.
  const std::string_view hidden{ "[hidden]" };
  const std::string_view body{ R"({"a":1})" };
  const std::string_view multi{ "one\ntwo" };

  assert_eq(fmt::format("body={}", core_logger::user_data_if(false, hidden)),
            "body=[hidden]",
            "the insensitive branch carries no tag");
  assert_eq(fmt::format("body={}", core_logger::user_data_if(true, body)),
            R"(body=<ud>{"a":1}</ud>)",
            "the sensitive branch is tagged");

  // The tagged branch goes through the same formatter as user_data(), so it escapes identically.
  assert_eq(fmt::format("{}", core_logger::user_data_if(true, multi)),
            R"(<ud>one\ntwo</ud>)",
            "and escapes like user_data()");
}

// A value carrying a tag sequence would end its span early and leave the rest of itself outside any
// tag. Not an attack, since whoever controls the value already knows it, but a silent failure: an
// application whose document happens to contain one of these hands out a log it believes was
// redacted.
const std::string_view injected_tag{ "</ud>secret<ud>" };

void
a_tagged_value_cannot_close_its_own_span([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("key={}", core_logger::user_data(injected_tag));
  assert_eq(tagged,
            R"(key=<ud>\u003c/ud>secret\u003cud></ud>)",
            "only the \"<\" of a tag sequence is escaped");

  // The span has to be the only one, and it has to close exactly once, at the end.
  assert_eq(tagged.find("</ud>"), tagged.size() - 5, "the span closes once, at the end");
  assert_eq(tagged.find("<ud>"), tagged.find("key=") + 4, "and opens once, at the start");
}

void
markup_that_is_not_a_tag_sequence_is_left_alone([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // Escaping every "<" would buy nothing here and would mangle any logged body carrying markup.
  assert_eq(fmt::format("{}", core_logger::user_data("<html><body>")),
            "<ud><html><body></ud>",
            "ordinary markup survives unescaped");
}

void
tag_injection_is_escaped_in_every_category_and_in_a_list([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const auto tagged = fmt::format("{}", core_logger::metadata(injected_tag));
  assert_eq(tagged.find("</md>"), tagged.size() - 5, "metadata closes once, at the end");
  assert_eq(core_logger::user_data_list(std::vector<std::string_view>{ injected_tag }),
            R"(<ud>\u003c/ud>secret\u003cud></ud>)",
            "a list entry is escaped like any other tagged value");
}

void
a_dynamic_format_specification_still_resolves_inside_a_tag([[maybe_unused]] context& ctx)
{
  // The tagged branch formats through a second context, which has to carry the caller's arguments:
  // a width given as "{:{}}" is looked up through the context, so without them this threw while the
  // unredacted branch worked. Annotating a statement must not depend on the redaction level.
  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("[{:{}}]", core_logger::metadata("ab"), 6),
              "[ab    ]",
              "the width is applied while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("[{:{}}]", core_logger::metadata("ab"), 6),
            "[<md>ab    </md>]",
            "and inside the tag while it is on");
}

void
a_serialised_document_carries_no_annotations_of_its_own([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // The origin dump is annotated as a whole, by the cluster open paths that log it. A tag written
  // into one of its values instead would be read back as part of that value by anything that parses
  // the line as JSON, so the document has to come out of here clean even with redaction on.
  const auto connstr =
    couchbase::core::utils::parse_connection_string("couchbase://10.0.0.1,10.0.0.2");
  const auto dump = couchbase::core::origin({}, connstr).to_json();

  assert_ne(dump.find("10.0.0.1"), std::string::npos, "the address is in the dump");
  assert_eq(dump.find('<'), std::string::npos, "and nothing in the document is tagged");
}

void
redaction_annotations_work_for_non_string_values([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::string host{ "10.0.0.1" };
  const std::string_view bucket{ "travel-sample" };

  assert_eq(fmt::format("{}", core_logger::system_data(11210)), "<sd>11210</sd>", "an integer");
  assert_eq(
    fmt::format("{}", core_logger::system_data(host)), "<sd>10.0.0.1</sd>", "a std::string");
  assert_eq(fmt::format("{}", core_logger::metadata(bucket)),
            "<md>travel-sample</md>",
            "a std::string_view");
}

void
redaction_annotations_may_be_mixed_in_a_single_statement([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  assert_eq(fmt::format("[{}/{}] <{}:{}> key={}",
                        "client-id",
                        core_logger::metadata("travel-sample"),
                        core_logger::system_data("10.0.0.1"),
                        core_logger::system_data(11210),
                        core_logger::user_data("airline_10")),
            "[client-id/<md>travel-sample</md>] <<sd>10.0.0.1</sd>:<sd>11210</sd>> "
            "key=<ud>airline_10</ud>",
            "each argument carries its own category");
}

// One tag around a joined list would hash the whole list as a single token, so an entry would match
// nothing else in the log, not even the same value logged beside it in a span of its own. Both of
// those shapes occur: dns_config.cxx logs the server list and the selected server on one line, and
// cluster.cxx logs bootstrap nodes that mcbp_session.cxx logs individually.
const std::vector<std::string> list_addresses{ "10.0.0.1:11210", "10.0.0.2:11210" };

void
a_list_is_inert_while_redaction_is_disabled([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ false };

  assert_eq(core_logger::system_data_list(list_addresses),
            "10.0.0.1:11210, 10.0.0.2:11210",
            "no tags while redaction is off");
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted),
            R"("10.0.0.1:11210", "10.0.0.2:11210")",
            "and the quotes are still rendered");
}

void
a_list_is_tagged_one_entry_at_a_time([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  const std::vector<std::string> buckets{ "travel-sample", "beer-sample" };

  assert_eq(core_logger::system_data_list(list_addresses),
            "<sd>10.0.0.1:11210</sd>, <sd>10.0.0.2:11210</sd>",
            "system data");
  assert_eq(core_logger::metadata_list(buckets),
            "<md>travel-sample</md>, <md>beer-sample</md>",
            "metadata");
  assert_eq(core_logger::user_data_list(buckets),
            "<ud>travel-sample</ud>, <ud>beer-sample</ud>",
            "user data");
}

void
a_list_tag_sits_inside_the_quotes_an_entry_renders([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // A span that swallowed the punctuation would hash to something matching no other line.
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted),
            R"("<sd>10.0.0.1:11210</sd>", "<sd>10.0.0.2:11210</sd>")",
            "the quotes stay outside the span");
}

void
a_list_uses_the_separator_the_caller_gives([[maybe_unused]] context& ctx)
{
  const scoped_log_redaction redaction{ true };

  // The separator is the caller's, since the existing lines disagree about it.
  assert_eq(core_logger::system_data_list(list_addresses, core_logger::list_entries::quoted, ","),
            R"("<sd>10.0.0.1:11210</sd>","<sd>10.0.0.2:11210</sd>")",
            "the given separator is used verbatim");
}

void
an_empty_list_renders_nothing([[maybe_unused]] context& ctx)
{
  const std::vector<std::string> empty{};
  {
    const scoped_log_redaction redaction{ false };
    assert_true(core_logger::system_data_list(empty).empty(), "empty while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_true(core_logger::system_data_list(empty).empty(), "empty while redaction is on");
  assert_true(core_logger::system_data_list(empty, core_logger::list_entries::quoted).empty(),
              "and no stray quotes are rendered");
}

void
a_document_id_splits_its_redaction_categories([[maybe_unused]] context& ctx)
{
  // A document id renders as bucket/scope.collection/key, and those parts do not share a category:
  // the names are metadata and only the key is user data. Wrapping the rendered form as a whole
  // would put a bucket name inside a <ud> span.
  const couchbase::core::document_id id{ "travel-sample", "inventory", "airline", "airline_10" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("{}", core_logger::document(id)),
              "travel-sample/inventory.airline/airline_10",
              "the rendered text while redaction is off");

    // The plain formatter is also used outside logging, so the two must agree while redaction is
    // off. If they ever diverge, a log line changes text for users who never enabled anything.
    assert_eq(fmt::format("{}", core_logger::document(id)),
              fmt::format("{}", id),
              "and it matches the plain formatter");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("{}", core_logger::document(id)),
            "<md>travel-sample</md>/<md>inventory</md>.<md>airline</md>/<ud>airline_10</ud>",
            "each component takes its own category");
}

void
a_document_id_built_without_a_collection_keeps_its_shape([[maybe_unused]] context& ctx)
{
  // This constructor leaves the collection path empty, so the id renders with an empty middle
  // component. Keep that shape rather than joining an empty scope and collection into a ".".
  const couchbase::core::document_id id{ "travel-sample", "airline_10" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("{}", core_logger::document(id)),
              "travel-sample//airline_10",
              "the empty middle component is kept");
    assert_eq(fmt::format("{}", core_logger::document(id)),
              fmt::format("{}", id),
              "and it matches the plain formatter");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("{}", core_logger::document(id)),
            "<md>travel-sample</md>//<ud>airline_10</ud>",
            "and the shape survives tagging");
}

void
exclusion_markers_never_change_what_is_printed([[maybe_unused]] context& ctx)
{
  // Both markers record a reviewed decision for the annotation checker and for anyone reading the
  // statement. Neither may alter the output, in either redaction state.
  const std::string host{ "10.0.0.1" };

  {
    const scoped_log_redaction redaction{ false };
    assert_eq(fmt::format("host={}", core_logger::not_sensitive(host)),
              "host=10.0.0.1",
              "not_sensitive while redaction is off");
    assert_eq(fmt::format("host={}", core_logger::not_redacted(host)),
              "host=10.0.0.1",
              "not_redacted while redaction is off");
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(fmt::format("host={}", core_logger::not_sensitive(host)),
            "host=10.0.0.1",
            "not_sensitive while redaction is on");
  assert_eq(fmt::format("host={}", core_logger::not_redacted(host)),
            "host=10.0.0.1",
            "not_redacted while redaction is on");
}

void
format_specifications_survive_the_wrappers([[maybe_unused]] context& ctx)
{
  const std::string bucket{ "bucket" };
  const std::vector<std::byte> body{ std::byte{ 0xde }, std::byte{ 0xad } };

  const scoped_log_redaction redaction{ true };

  // Every wrapper inherits parse() from the formatter of the value, so a specification keeps
  // working. It applies to the value alone, which means padding lands inside the tags.
  assert_eq(fmt::format("{:>8}", core_logger::metadata(bucket)),
            "<md>  bucket</md>",
            "the padding lands inside the span");

  // That inheritance is what lets a value keep a spec as unusual as the "{:a}" of a hex dump. No
  // site pairs the two today, since a hex dump is never tagged and never marked, but the wrappers
  // have to be spec-transparent in general and this is the strongest spec to pin that with.
  assert_eq(fmt::format("{:a}", core_logger::not_redacted(spdlog::to_hex(body))),
            fmt::format("{:a}", spdlog::to_hex(body)),
            "a hex dump renders identically through the marker");
}

void
connection_context_prefixes_bake_in_their_tags([[maybe_unused]] context& ctx)
{
  // Connection-context prefixes are formatted once, when a bucket or session is constructed, and
  // reused for the lifetime of that object. Their tags are therefore fixed at construction time and
  // do not follow later changes to the redaction setting. This is why redaction must be enabled
  // before connecting; see couchbase::core::logger::set_log_redaction().
  std::string prefix;
  {
    const scoped_log_redaction redaction{ true };
    prefix = fmt::format("[{}/{}]", "client-id", core_logger::metadata("travel-sample"));
    assert_eq(prefix, "[client-id/<md>travel-sample</md>]", "built while redaction was on");
  }

  assert_false(core_logger::is_log_redaction_enabled(), "redaction is off again");
  assert_eq(prefix, "[client-id/<md>travel-sample</md>]", "and the prefix keeps its tags");

  // Conversely, a prefix built while redaction was disabled stays untagged even once it is on.
  std::string untagged_prefix;
  {
    const scoped_log_redaction redaction{ false };
    untagged_prefix = fmt::format("[{}/{}]", "client-id", core_logger::metadata("travel-sample"));
  }

  const scoped_log_redaction redaction{ true };
  assert_eq(untagged_prefix, "[client-id/travel-sample]", "and an untagged prefix stays untagged");
}

void
enabling_redaction_is_observable([[maybe_unused]] context& ctx)
{
  assert_false(core_logger::is_log_redaction_enabled(), "off to begin with");
  {
    const scoped_log_redaction redaction{ true };
    assert_true(core_logger::is_log_redaction_enabled(), "on while the guard is alive");
  }
  assert_false(core_logger::is_log_redaction_enabled(), "and off again afterwards");
}
} // namespace

auto
tests() -> test_suite
{
  return {
    suite_name,
    {
      { CASE(a_registered_callback_receives_every_logged_message) },
      { CASE(the_callback_receives_the_level_of_each_message) },
      { CASE(logging_with_no_callback_registered_does_not_throw) },
      { CASE(registering_a_second_callback_replaces_the_first) },
      { CASE(unregistering_stops_delivery_until_a_callback_is_registered_again) },
      { CASE(arguments_are_not_formatted_when_logging_is_off_and_no_callback_is_registered) },
      { CASE(a_registered_callback_receives_formatted_arguments_while_logging_is_off) },
      { CASE(redaction_annotations_are_inert_while_redaction_is_disabled) },
      { CASE(redaction_annotations_wrap_values_while_redaction_is_enabled) },
      { CASE(a_tagged_value_never_spans_log_lines) },
      { CASE(every_entry_of_a_tagged_list_is_escaped) },
      { CASE(a_value_with_newlines_is_untouched_while_redaction_is_disabled) },
      { CASE(the_exclusion_markers_never_escape_their_value) },
      { CASE(a_conditionally_tagged_value_is_only_tagged_on_the_sensitive_branch) },
      { CASE(a_tagged_value_cannot_close_its_own_span) },
      { CASE(markup_that_is_not_a_tag_sequence_is_left_alone) },
      { CASE(tag_injection_is_escaped_in_every_category_and_in_a_list) },
      { CASE(a_dynamic_format_specification_still_resolves_inside_a_tag) },
      { CASE(a_serialised_document_carries_no_annotations_of_its_own) },
      { CASE(redaction_annotations_work_for_non_string_values) },
      { CASE(redaction_annotations_may_be_mixed_in_a_single_statement) },
      { CASE(a_list_is_inert_while_redaction_is_disabled) },
      { CASE(a_list_is_tagged_one_entry_at_a_time) },
      { CASE(a_list_tag_sits_inside_the_quotes_an_entry_renders) },
      { CASE(a_list_uses_the_separator_the_caller_gives) },
      { CASE(an_empty_list_renders_nothing) },
      { CASE(a_document_id_splits_its_redaction_categories) },
      { CASE(a_document_id_built_without_a_collection_keeps_its_shape) },
      { CASE(exclusion_markers_never_change_what_is_printed) },
      { CASE(format_specifications_survive_the_wrappers) },
      { CASE(connection_context_prefixes_bake_in_their_tags) },
      { CASE(enabling_redaction_is_observable) },
    },
  };
}

} // namespace couchbase::test
