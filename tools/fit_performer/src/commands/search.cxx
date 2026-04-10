/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "search.hxx"
#include "../exceptions.hxx"
#include "common.hxx"

#include "core/meta/features.hxx"

#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/date_range_facet.hxx>
#include <couchbase/numeric_range_facet.hxx>
#include <couchbase/search_row_location.hxx>
#include <couchbase/search_sort_field.hxx>
#include <couchbase/search_sort_geo_distance.hxx>
#include <couchbase/search_sort_id.hxx>
#include <couchbase/search_sort_score.hxx>
#include <couchbase/term_facet.hxx>

#include <google/protobuf/util/time_util.h>

#include <variant>

namespace fit_cxx::commands::search
{
void
fields_to_content(const couchbase::search_row& result,
                  const protocol::shared::ContentAs& content_as,
                  common::serializer serializer,
                  protocol::sdk::search::SearchRow* proto_result)
{
  std::visit(
    common::overloaded{
      [&](auto s) {
        switch (content_as.as_case()) {
          case protocol::shared::ContentAs::kAsString:
            proto_result->mutable_fields()->set_content_as_string(
              result.fields_as<decltype(s), std::string>());
            break;
          case protocol::shared::ContentAs::kAsByteArray: {
            auto value = result.fields_as<decltype(s), std::vector<std::byte>>();
            proto_result->mutable_fields()->set_content_as_bytes(
              std::string{ reinterpret_cast<const char*>(value.data()), value.size() });
            break;
          }
          case protocol::shared::ContentAs::kAsJsonObject:
          case protocol::shared::ContentAs::kAsJsonArray:
            proto_result->mutable_fields()->set_content_as_bytes(
              couchbase::core::utils::json::generate(
                result.fields_as<decltype(s), tao::json::value>()));
            break;
          case protocol::shared::ContentAs::kAsBoolean:
            proto_result->mutable_fields()->set_content_as_bool(
              result.fields_as<decltype(s), bool>());
            break;
          case protocol::shared::ContentAs::kAsInteger:
            proto_result->mutable_fields()->set_content_as_int64(
              result.fields_as<decltype(s), std::int64_t>());
            break;
          case protocol::shared::ContentAs::kAsFloatingPoint:
            proto_result->mutable_fields()->set_content_as_double(
              result.fields_as<decltype(s), double>());
            break;
          default:
            throw performer_exception::unimplemented(
              fmt::format("unsupported fields_as type {}", content_as.DebugString()));
        }
      },
    },
    serializer);
}

void
to_highlight(couchbase::search_options& opts, const protocol::sdk::search::Highlight& highlight)
{
  std::vector<std::string> fields(highlight.fields().begin(), highlight.fields().end());

  if (highlight.has_style()) {
    if (highlight.style() == protocol::sdk::search::HighlightStyle::HIGHLIGHT_STYLE_HTML) {
      opts.highlight(couchbase::highlight_style::html, fields);
    } else if (highlight.style() == protocol::sdk::search::HighlightStyle::HIGHLIGHT_STYLE_ANSI) {
      opts.highlight(couchbase::highlight_style::ansi, fields);
    }
    return;
  }
  opts.highlight(fields);
}

void
to_scan_consistency(couchbase::search_options& opts,
                    const protocol::sdk::search::SearchScanConsistency& consistency)
{
  if (consistency == protocol::sdk::search::SEARCH_SCAN_CONSISTENCY_NOT_BOUNDED) {
    opts.scan_consistency(couchbase::search_scan_consistency::not_bounded);
  }
}

void
to_field_type(const std::shared_ptr<couchbase::search_sort_field>& field_ptr, std::string type)
{
  if (type == "auto") {
    field_ptr->type(couchbase::search_sort_field_type::automatic);
  } else if (type == "string") {
    field_ptr->type(couchbase::search_sort_field_type::string);
  } else if (type == "number") {
    field_ptr->type(couchbase::search_sort_field_type::number);
  } else if (type == "date") {
    field_ptr->type(couchbase::search_sort_field_type::date);
  } else {
    throw performer_exception::unimplemented(
      fmt::format("Unknown search sort field type received: {}", type));
  }
}

void
to_field_mode(const std::shared_ptr<couchbase::search_sort_field>& field_ptr, std::string mode)
{
  if (mode == "default") {
    field_ptr->mode(couchbase::search_sort_field_mode::server_default);
  } else if (mode == "min") {
    field_ptr->mode(couchbase::search_sort_field_mode::min);
  } else if (mode == "max") {
    field_ptr->mode(couchbase::search_sort_field_mode::max);
  } else {
    throw performer_exception::unimplemented(
      fmt::format("Unknown search sort field mode received: {}", mode));
  }
}

void
to_field_missing(const std::shared_ptr<couchbase::search_sort_field>& field_ptr,
                 std::string missing)
{
  if (missing == "first") {
    field_ptr->missing(couchbase::search_sort_field_missing::first);
  } else if (missing == "last") {
    field_ptr->missing(couchbase::search_sort_field_missing::last);
  } else {
    throw performer_exception::unimplemented(
      fmt::format("Unknown search sort field missing received: {}", missing));
  }
}

void
to_geo_distance_unit(const std::shared_ptr<couchbase::search_sort_geo_distance>& geo_distance_ptr,
                     protocol::sdk::search::SearchGeoDistanceUnits units)
{
  switch (units) {
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_METERS:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::meters);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_MILES:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::miles);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_CENTIMETERS:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::centimeters);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_MILLIMETERS:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::millimeters);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_NAUTICAL_MILES:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::nautical_miles);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_KILOMETERS:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::kilometers);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_FEET:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::feet);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_YARDS:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::yards);
      break;
    case protocol::sdk::search::SEARCH_GEO_DISTANCE_UNITS_INCHES:
      geo_distance_ptr->unit(couchbase::search_geo_distance_units::inch);
      break;
    default:
      throw performer_exception::unimplemented("Unknown geo distance unit provided");
  }
}

void
to_search_sort_score(std::vector<std::shared_ptr<couchbase::search_sort>>& sort_types,
                     const protocol::sdk::search::SearchSortScore& score)
{
  couchbase::search_sort_score score_cb;
  auto score_ptr = std::make_shared<couchbase::search_sort_score>(score_cb);
  if (score.has_desc()) {
    score_ptr->descending(score.desc());
  }
  sort_types.emplace_back(score_ptr);
}

void
to_search_sort_id(std::vector<std::shared_ptr<couchbase::search_sort>>& sort_types,
                  const protocol::sdk::search::SearchSortId& id)
{
  couchbase::search_sort_id id_cb;
  auto id_ptr = std::make_shared<couchbase::search_sort_id>(id_cb);
  if (id.has_desc()) {
    id_ptr->descending(id.desc());
  }
  sort_types.emplace_back(id_ptr);
}

void
to_search_sort_field(std::vector<std::shared_ptr<couchbase::search_sort>>& sort_types,
                     const protocol::sdk::search::SearchSortField& field)
{
  couchbase::search_sort_field field_cb(field.field());
  auto field_ptr = std::make_shared<couchbase::search_sort_field>(field_cb);
  if (field.has_desc()) {
    field_ptr->descending(field.desc());
  }
  if (field.has_type()) {
    to_field_type(field_ptr, field.type());
  }
  if (field.has_mode()) {
    to_field_mode(field_ptr, field.mode());
  }
  if (field.has_missing()) {
    to_field_missing(field_ptr, field.missing());
  }
  sort_types.emplace_back(field_ptr);
}

void
to_search_sort_geo_distance(std::vector<std::shared_ptr<couchbase::search_sort>>& sort_types,
                            const protocol::sdk::search::SearchSortGeoDistance& geo_distance)
{
  const auto& location = geo_distance.location();
  couchbase::search_sort_geo_distance geo_distance_cb(
    location.lat(), location.lon(), geo_distance.field());
  auto geo_distance_ptr = std::make_shared<couchbase::search_sort_geo_distance>(geo_distance_cb);
  if (geo_distance.has_desc()) {
    geo_distance_ptr->descending(geo_distance.desc());
  }
  if (geo_distance.has_unit()) {
    to_geo_distance_unit(geo_distance_ptr, geo_distance.unit());
  }
  sort_types.emplace_back(geo_distance_ptr);
}

void
to_search_sort(couchbase::search_options& opts,
               const google::protobuf::RepeatedPtrField<protocol::sdk::search::SearchSort>& sorting)
{
  std::vector<std::string> string_sort;
  std::vector<std::shared_ptr<couchbase::search_sort>> sort_types;
  for (const auto& sort : sorting) {
    if (sort.has_score()) {
      to_search_sort_score(sort_types, sort.score());
    } else if (sort.has_id()) {
      to_search_sort_id(sort_types, sort.id());
    } else if (sort.has_field()) {
      to_search_sort_field(sort_types, sort.field());
    } else if (sort.has_geo_distance()) {
      to_search_sort_geo_distance(sort_types, sort.geo_distance());
    } else if (sort.has_raw()) {
      string_sort.emplace_back(sort.raw());
    }
  }
  opts.sort(string_sort);
  opts.sort(sort_types);
}

auto
to_numeric_ranges(
  const google::protobuf::RepeatedPtrField<protocol::sdk::search::NumericRange>& ranges)
  -> std::vector<couchbase::numeric_range>
{
  std::vector<couchbase::numeric_range> numeric_ranges;

  for (const auto& range : ranges) {
    if (range.has_min() && range.has_max()) {
      couchbase::numeric_range numeric_range(range.name(), range.min(), range.max());
      numeric_ranges.emplace_back(numeric_range);
    } else if (range.has_min() && !range.has_max()) {
      auto numeric_range = couchbase::numeric_range::with_min(range.name(), range.min());
      numeric_ranges.emplace_back(numeric_range);
    } else if (!range.has_min() && range.has_max()) {
      auto numeric_range = couchbase::numeric_range::with_max(range.name(), range.max());
      numeric_ranges.emplace_back(numeric_range);
    } else {
      throw performer_exception::unimplemented(
        "Neither min or max numeric range provided - no valid constructor");
    }
  }
  return numeric_ranges;
}

auto
to_date_ranges(const google::protobuf::RepeatedPtrField<protocol::sdk::search::DateRange>& ranges)
  -> std::vector<couchbase::date_range>
{
  std::vector<couchbase::date_range> date_ranges;

  for (const auto& range : ranges) {
    if (range.has_start() && range.has_end()) {
      auto start = fit_cxx::commands::common::to_time_point(range.start());
      auto end = fit_cxx::commands::common::to_time_point(range.end());
      couchbase::date_range date_range(range.name(), start, end);
      date_ranges.emplace_back(date_range);
    } else if (range.has_start() && !range.has_end()) {
      auto start = fit_cxx::commands::common::to_time_point(range.start());
      couchbase::date_range::with_start(range.name(), start);
    } else if (!range.has_start() && range.has_end()) {
      auto end = fit_cxx::commands::common::to_time_point(range.end());
      couchbase::date_range::with_end(range.name(), end);
    } else {
      throw performer_exception::unimplemented(
        "Neither start or end date range provided - no valid constructor");
    }
  }
  return date_ranges;
}

auto
to_facet(const protocol::sdk::search::SearchFacet& facet)
  -> std::shared_ptr<couchbase::search_facet>
{
  if (facet.has_term()) {
    if (facet.term().has_size()) {
      couchbase::term_facet term_facet(facet.term().field(), facet.term().size());
      return std::make_shared<couchbase::term_facet>(term_facet);
    }
    couchbase::term_facet term_facet(facet.term().field());
    return std::make_shared<couchbase::term_facet>(term_facet);
  }
  if (facet.has_numeric_range()) {
    auto numeric_ranges = to_numeric_ranges(facet.numeric_range().numeric_ranges());
    if (facet.numeric_range().has_size()) {
      couchbase::numeric_range_facet numeric_range_facet(
        facet.numeric_range().field(), facet.numeric_range().size(), numeric_ranges);
      return std::make_shared<couchbase::numeric_range_facet>(numeric_range_facet);
    }
    couchbase::numeric_range_facet numeric_range_facet(facet.numeric_range().field(),
                                                       numeric_ranges);
    return std::make_shared<couchbase::numeric_range_facet>(numeric_range_facet);
  }
  if (facet.has_date_range()) {
    auto date_ranges = to_date_ranges(facet.date_range().date_ranges());
    if (facet.date_range().has_size()) {
      couchbase::date_range_facet date_range_facet(
        facet.date_range().field(), facet.date_range().size(), date_ranges);
      return std::make_shared<couchbase::date_range_facet>(date_range_facet);
    }
    couchbase::date_range_facet date_range_facet(facet.date_range().field(), date_ranges);
    return std::make_shared<couchbase::date_range_facet>(date_range_facet);
  }
  throw performer_exception::unimplemented("Unknown facet type provided");
}

void
to_facets(
  couchbase::search_options& opts,
  const google::protobuf::Map<std::string, protocol::sdk::search::SearchFacet>& search_facets)
{
  for (const auto& [name, search_facet] : search_facets) {
    auto facet = to_facet(search_facet);
    opts.facet(name, facet);
  }
}

void
to_raw(couchbase::search_options& opts, const google::protobuf::Map<std::string, std::string>& raw)
{
  for (const auto& [name, value] : raw) {
    opts.raw(name, value);
  }
}

template<typename SearchCommand>
auto
to_search_options(const SearchCommand cmd, observability::span_owner* spans)
  -> couchbase::search_options
{
  couchbase::search_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_limit()) {
    opts.limit(cmd.options().limit());
  }
  if (cmd.options().has_skip()) {
    opts.skip(cmd.options().skip());
  }
  if (cmd.options().has_explain()) {
    opts.explain(cmd.options().explain());
  }
  if (cmd.options().has_highlight()) {
    to_highlight(opts, cmd.options().highlight());
  }
  if (!cmd.options().fields().empty()) {
    std::vector<std::string> fields(cmd.options().fields().begin(), cmd.options().fields().end());
    opts.fields(fields);
  }
  if (cmd.options().has_scan_consistency()) {
    to_scan_consistency(opts, cmd.options().scan_consistency());
  }
  if (cmd.options().has_consistent_with()) {
    auto state = fit_cxx::commands::common::to_mutation_state(cmd.options().consistent_with());
    opts.consistent_with(state);
  }
  if (!cmd.options().sort().empty()) {
    to_search_sort(opts, cmd.options().sort());
  }
  if (!cmd.options().facets().empty()) {
    to_facets(opts, cmd.options().facets());
  }
  if (cmd.options().has_timeout_millis()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_millis()));
  }
  if (!cmd.options().raw().empty()) {
    to_raw(opts, cmd.options().raw());
  }
  if (cmd.options().has_include_locations()) {
    opts.include_locations(cmd.options().include_locations());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

template<typename SearchCommand>
auto
to_serializer(const SearchCommand cmd) -> common::serializer
{
  if (!cmd.has_options() || !cmd.options().has_serialize()) {
    return couchbase::codec::tao_json_serializer{};
  }

  protocol::shared::JsonSerializer proto_serializer = cmd.options().serialize();
  switch (proto_serializer.serializer_case()) {
    case protocol::shared::JsonSerializer::kDefault:
      return couchbase::codec::tao_json_serializer{};
    case protocol::shared::JsonSerializer::kCustomSerializer:
      return common::custom_json_serializer{};
    default:
      throw performer_exception::unimplemented(
        fmt::format("Serializer type not supported {}", proto_serializer.DebugString()));
  }
}

auto
to_match_query(const protocol::sdk::search::MatchQuery& query) -> search_query_types
{
  couchbase::match_query match_query(query.match());
  if (query.has_field()) {
    match_query.field(query.field());
  }
  if (query.has_analyzer()) {
    match_query.analyzer(query.analyzer());
  }
  if (query.has_prefix_length()) {
    match_query.prefix_length(query.prefix_length());
  }
  if (query.has_fuzziness()) {
    match_query.fuzziness(query.fuzziness());
  }
  if (query.has_boost()) {
    match_query.boost(query.boost());
  }
  if (query.has_operator_()) {
    if (query.operator_() == protocol::sdk::search::MatchOperator::SEARCH_MATCH_OPERATOR_AND) {
      match_query.match_operator(couchbase::match_operator::logical_and);
    } else if (query.operator_() ==
               protocol::sdk::search::MatchOperator::SEARCH_MATCH_OPERATOR_OR) {
      match_query.match_operator(couchbase::match_operator::logical_or);
    }
  }
  return match_query;
}

auto
to_match_phrase_query(const protocol::sdk::search::MatchPhraseQuery& query)
  -> couchbase::match_phrase_query
{
  couchbase::match_phrase_query match_phrase_query(query.match_phrase());
  if (query.has_field()) {
    match_phrase_query.field(query.field());
  }
  if (query.has_analyzer()) {
    match_phrase_query.analyzer(query.analyzer());
  }
  if (query.has_boost()) {
    match_phrase_query.boost(query.boost());
  }
  return match_phrase_query;
}

auto
to_regexp_query(const protocol::sdk::search::RegexpQuery& query) -> couchbase::regexp_query
{
  couchbase::regexp_query regexp_query(query.regexp());
  if (query.has_field()) {
    regexp_query.field(query.field());
  }
  if (query.has_boost()) {
    regexp_query.boost(query.boost());
  }
  return regexp_query;
}

auto
to_query_string_query(const protocol::sdk::search::QueryStringQuery& query)
  -> couchbase::query_string_query
{
  couchbase::query_string_query query_string_query(query.query());
  if (query.has_boost()) {
    query_string_query.boost(query.boost());
  }
  return query_string_query;
}

auto
to_wildcard_query(const protocol::sdk::search::WildcardQuery& query) -> couchbase::wildcard_query
{
  couchbase::wildcard_query wildcard_query(query.wildcard());
  if (query.has_field()) {
    wildcard_query.field(query.field());
  }
  if (query.has_boost()) {
    wildcard_query.boost(query.boost());
  }
  return wildcard_query;
}

auto
to_doc_id_query(const protocol::sdk::search::DocIdQuery& query) -> couchbase::doc_id_query
{
  couchbase::doc_id_query doc_id_query;
  for (const auto& id : query.ids()) {
    doc_id_query.doc_id(id);
  }
  if (query.has_boost()) {
    doc_id_query.boost(query.boost());
  }
  return doc_id_query;
}

auto
to_boolean_field_query(const protocol::sdk::search::BooleanFieldQuery& query)
  -> couchbase::boolean_field_query
{
  couchbase::boolean_field_query boolean_field_query(query.bool_());

  if (query.has_field()) {
    boolean_field_query.field(query.field());
  }
  if (query.has_boost()) {
    boolean_field_query.boost(query.boost());
  }
  return boolean_field_query;
}

auto
to_date_range_query(const protocol::sdk::search::DateRangeQuery& query)
  -> couchbase::date_range_query
{
  couchbase::date_range_query date_range_query;
  if (query.has_start() && query.has_inclusive_start()) {
    date_range_query.start(query.start(), query.inclusive_start());
  } else if (query.has_start() && !query.has_inclusive_start()) {
    date_range_query.start(query.start());
  }

  if (query.has_end() && query.has_inclusive_end()) {
    date_range_query.end(query.end(), query.inclusive_end());
  } else if (query.has_end() && !query.has_inclusive_end()) {
    date_range_query.end(query.end());
  }

  if (query.has_datetime_parser()) {
    date_range_query.date_time_parser(query.datetime_parser());
  }
  if (query.has_field()) {
    date_range_query.field(query.field());
  }
  if (query.has_boost()) {
    date_range_query.boost((query.boost()));
  }
  return date_range_query;
}

auto
to_numeric_range_query(const protocol::sdk::search::NumericRangeQuery& query)
  -> couchbase::numeric_range_query
{
  couchbase::numeric_range_query numeric_range_query;

  if (query.has_min() && query.has_inclusive_min()) {
    numeric_range_query.min(query.min(), query.inclusive_min());
  } else if (query.has_min() && !query.has_inclusive_min()) {
    numeric_range_query.min(query.min());
  }

  if (query.has_max() && query.has_inclusive_max()) {
    numeric_range_query.max(query.max(), query.inclusive_max());
  } else if (query.has_max() && !query.has_inclusive_max()) {
    numeric_range_query.max(query.max());
  }

  if (query.has_field()) {
    numeric_range_query.field(query.field());
  }
  if (query.has_boost()) {
    numeric_range_query.boost(query.boost());
  }
  return numeric_range_query;
}

auto
to_term_range_query(const protocol::sdk::search::TermRangeQuery& query)
  -> couchbase::term_range_query
{
  couchbase::term_range_query term_range_query;

  if (query.has_min() && query.has_inclusive_min()) {
    term_range_query.min(query.min(), query.inclusive_min());
  } else if (query.has_min() && !query.has_inclusive_min()) {
    term_range_query.min(query.min());
  }

  if (query.has_max() && query.has_inclusive_max()) {
    term_range_query.max(query.max(), query.inclusive_max());
  } else if (query.has_max() && !query.has_inclusive_max()) {
    term_range_query.max(query.max());
  }

  if (query.has_field()) {
    term_range_query.field(query.field());
  }
  if (query.has_boost()) {
    term_range_query.boost(query.boost());
  }
  return term_range_query;
}

auto
to_geo_distance_query(const protocol::sdk::search::GeoDistanceQuery& query)
  -> couchbase::geo_distance_query
{
  couchbase::geo_distance_query geo_distance_query(
    query.location().lat(), query.location().lon(), query.distance());

  if (query.has_field()) {
    geo_distance_query.field(query.field());
  }
  if (query.has_boost()) {
    geo_distance_query.boost(query.boost());
  }
  return geo_distance_query;
}

auto
to_geo_bounding_box_query(const protocol::sdk::search::GeoBoundingBoxQuery& query)
  -> couchbase::geo_bounding_box_query
{
  couchbase::geo_bounding_box_query geo_bounding_box_query(query.top_left().lat(),
                                                           query.top_left().lon(),
                                                           query.bottom_right().lat(),
                                                           query.bottom_right().lon());

  if (query.has_field()) {
    geo_bounding_box_query.field(query.field());
  }
  if (query.has_boost()) {
    geo_bounding_box_query.boost(query.boost());
  }
  return geo_bounding_box_query;
}

auto
to_conjunction_query(const protocol::sdk::search::ConjunctionQuery& query)
  -> couchbase::conjunction_query
{
  couchbase::conjunction_query conjunction_query;
  for (const auto& search_query : query.conjuncts()) {
    auto query_cb = to_search_query(search_query);
    std::visit(common::overloaded{ [&conjunction_query](auto&& arg) -> void {
                 conjunction_query.and_also(arg);
               } },
               query_cb);
  }
  if (query.has_boost()) {
    conjunction_query.boost(query.boost());
  }
  return conjunction_query;
}

auto
to_disjunction_query(const protocol::sdk::search::DisjunctionQuery& query)
  -> couchbase::disjunction_query
{
  couchbase::disjunction_query disjunction_query;
  for (const auto& search_query : query.disjuncts()) {
    auto query_cb = to_search_query(search_query);
    std::visit(common::overloaded{ [&disjunction_query](auto&& arg) -> void {
                 disjunction_query.or_else(arg);
               } },
               query_cb);
  }
  if (query.has_min()) {
    disjunction_query.min(query.min());
  }
  if (query.has_boost()) {
    disjunction_query.boost(query.boost());
  }
  return disjunction_query;
}

auto
to_boolean_query(const protocol::sdk::search::BooleanQuery& query) -> couchbase::boolean_query
{
  couchbase::boolean_query boolean_query;

  for (const auto& must_query : query.must()) {
    auto query_cb = fit_cxx::commands::search::to_search_query(must_query);

    std::visit(common::overloaded{ [&boolean_query](auto&& arg) -> void {
                 auto must = boolean_query.must().and_also(arg);
                 boolean_query.must(must);
               } },
               query_cb);
  }
  for (const auto& should_query : query.should()) {
    auto query_cb = fit_cxx::commands::search::to_search_query(should_query);

    std::visit(common::overloaded{ [&boolean_query](auto&& arg) -> void {
                 auto should = boolean_query.should().or_else(arg);
                 boolean_query.should(should);
               } },
               query_cb);
  }
  for (const auto& must_not_query : query.must_not()) {
    auto query_cb = fit_cxx::commands::search::to_search_query(must_not_query);

    std::visit(common::overloaded{ [&boolean_query](auto&& arg) -> void {
                 auto must_not = boolean_query.must_not().or_else(arg);
                 boolean_query.must_not(must_not);
               } },
               query_cb);
  }

  if (query.has_should_min()) {
    auto should = boolean_query.should().min(query.should_min());
    boolean_query.should(should);
  }

  if (query.has_boost()) {
    boolean_query.boost(query.boost());
  }

  return boolean_query;
}

auto
to_term_query(const protocol::sdk::search::TermQuery& query) -> couchbase::term_query
{
  couchbase::term_query term_query(query.term());

  if (query.has_field()) {
    term_query.field(query.field());
  }
  if (query.has_fuzziness()) {
    term_query.fuzziness(query.fuzziness());
  }
  if (query.has_prefix_length()) {
    term_query.prefix_length(query.prefix_length());
  }
  if (query.has_boost()) {
    term_query.boost(query.boost());
  }
  return term_query;
}

auto
to_prefix_query(const protocol::sdk::search::PrefixQuery& query) -> couchbase::prefix_query
{
  couchbase::prefix_query prefix_query(query.prefix());

  if (query.has_field()) {
    prefix_query.field(query.field());
  }
  if (query.has_boost()) {
    prefix_query.boost(query.boost());
  }
  return prefix_query;
}

auto
to_phrase_query(const protocol::sdk::search::PhraseQuery& query) -> couchbase::phrase_query
{
  couchbase::phrase_query phrase_query({ query.terms().begin(), query.terms().end() });

  if (query.has_field()) {
    phrase_query.field(query.field());
  }
  if (query.has_boost()) {
    phrase_query.boost(query.boost());
  }
  return phrase_query;
}

auto
to_match_all_query(const protocol::sdk::search::MatchAllQuery& /*query*/)
  -> couchbase::match_all_query
{
  return {};
}

auto
to_match_none_query(const protocol::sdk::search::MatchNoneQuery& /*query*/)
  -> couchbase::match_none_query
{
  return {};
}

auto
to_search_query(const protocol::sdk::search::SearchQuery& query)
  -> fit_cxx::commands::search::search_query_types
{
  switch (query.query_case()) {
    case protocol::sdk::search::SearchQuery::QueryCase::kMatch:
      return to_match_query(query.match());
    case protocol::sdk::search::SearchQuery::QueryCase::kMatchPhrase:
      return to_match_phrase_query(query.match_phrase());
    case protocol::sdk::search::SearchQuery::QueryCase::kRegexp:
      return to_regexp_query(query.regexp());
    case protocol::sdk::search::SearchQuery::QueryCase::kQueryString:
      return to_query_string_query(query.query_string());
    case protocol::sdk::search::SearchQuery::QueryCase::kWildcard:
      return to_wildcard_query(query.wildcard());
    case protocol::sdk::search::SearchQuery::QueryCase::kDocId:
      return to_doc_id_query(query.doc_id());
    case protocol::sdk::search::SearchQuery::QueryCase::kSearchBooleanField:
      return to_boolean_field_query(query.search_boolean_field());
    case protocol::sdk::search::SearchQuery::QueryCase::kDateRange:
      return to_date_range_query(query.date_range());
    case protocol::sdk::search::SearchQuery::QueryCase::kNumericRange:
      return to_numeric_range_query(query.numeric_range());
    case protocol::sdk::search::SearchQuery::QueryCase::kTermRange:
      return to_term_range_query(query.term_range());
    case protocol::sdk::search::SearchQuery::QueryCase::kGeoDistance:
      return to_geo_distance_query(query.geo_distance());
    case protocol::sdk::search::SearchQuery::QueryCase::kGeoBoundingBox:
      return to_geo_bounding_box_query(query.geo_bounding_box());
    case protocol::sdk::search::SearchQuery::QueryCase::kConjunction:
      return to_conjunction_query(query.conjunction());
    case protocol::sdk::search::SearchQuery::QueryCase::kDisjunction:
      return to_disjunction_query(query.disjunction());
    case protocol::sdk::search::SearchQuery::QueryCase::kBoolean:
      return to_boolean_query(query.boolean());
    case protocol::sdk::search::SearchQuery::QueryCase::kTerm:
      return to_term_query(query.term());
    case protocol::sdk::search::SearchQuery::QueryCase::kPrefix:
      return to_prefix_query(query.prefix());
    case protocol::sdk::search::SearchQuery::QueryCase::kPhrase:
      return to_phrase_query(query.phrase());
    case protocol::sdk::search::SearchQuery::QueryCase::kMatchAll:
      return to_match_all_query(query.match_all());
    case protocol::sdk::search::SearchQuery::QueryCase::kMatchNone:
      return to_match_none_query(query.match_none());
    default:
      throw performer_exception::unimplemented("Unknown search query type provided");
  }
}

void
from_search_row_location(
  const std::vector<couchbase::search_row_location>& locations,
  google::protobuf::RepeatedPtrField<protocol::sdk::search::SearchRowLocation>* proto_locations)
{
  for (const auto& location : locations) {
    auto* proto_location = proto_locations->Add();
    proto_location->set_field(location.field());
    proto_location->set_term(location.term());
    proto_location->set_position(static_cast<std::uint32_t>(location.position()));
    proto_location->set_start(static_cast<std::uint32_t>(location.start_offset()));
    proto_location->set_end(static_cast<std::uint32_t>(location.end_offset()));
    if (location.array_positions().has_value()) {
      *proto_location->mutable_array_positions() = { location.array_positions().value().begin(),
                                                     location.array_positions().value().end() };
    }
  }
}

void
from_search_row_fragments(
  const std::map<std::string, std::vector<std::string>>& fragments,
  google::protobuf::Map<std::string, protocol::sdk::search::SearchFragments>* proto_fragments)
{
  for (const auto& [key, val] : fragments) {
    protocol::sdk::search::SearchFragments fragment;
    *fragment.mutable_fragments() = { val.begin(), val.end() };
    (*proto_fragments)[key] = fragment;
  }
}

void
from_search_row(const couchbase::search_row& row,
                protocol::sdk::search::SearchRow* proto_row,
                std::optional<protocol::shared::ContentAs> fields_as,
                common::serializer serializer)
{
  proto_row->set_index(row.index());
  proto_row->set_id(row.id());
  proto_row->set_score(row.score());
  auto explanation = std::string{ reinterpret_cast<const char*>(row.explanation().data()),
                                  row.explanation().size() };
  proto_row->set_explanation(explanation);
  if (row.locations().has_value()) {
    from_search_row_location(row.locations().value().get_all(), proto_row->mutable_locations());
  }
  if (!row.fragments().empty()) {
    from_search_row_fragments(row.fragments(), proto_row->mutable_fragments());
  }
  if (fields_as.has_value()) {
    fields_to_content(row, fields_as.value(), serializer, proto_row);
  }
}

void
from_facets(
  const std::map<std::string, std::shared_ptr<couchbase::search_facet_result>>& facet,
  google::protobuf::Map<std::string, protocol::sdk::search::SearchFacetResult>* proto_facet)
{
  for (const auto& [key, val] : facet) {
    protocol::sdk::search::SearchFacetResult result;
    result.set_name(val->name());
    result.set_field(val->field());
    result.set_total(val->total());
    result.set_missing(val->missing());
    result.set_other(val->other());
    (*proto_facet)[key] = result;
  }
}

void
from_metrics(const couchbase::search_metrics& metrics,
             protocol::sdk::search::SearchMetrics* proto_metrics)
{
  auto took_millis = std::chrono::duration_cast<std::chrono::milliseconds>(metrics.took());
  proto_metrics->set_took_msec(took_millis.count());
  proto_metrics->set_total_rows(static_cast<std::int64_t>(metrics.total_rows()));
  proto_metrics->set_max_score(metrics.max_score());
  proto_metrics->set_total_partition_count(
    static_cast<std::int64_t>(metrics.total_partition_count()));
  proto_metrics->set_success_partition_count(
    static_cast<std::int64_t>(metrics.success_partition_count()));
  proto_metrics->set_error_partition_count(
    static_cast<std::int64_t>(metrics.error_partition_count()));
}

void
from_metadata(const couchbase::search_meta_data& metadata,
              protocol::sdk::search::SearchMetaData* proto_metadata)
{
  from_metrics(metadata.metrics(), proto_metadata->mutable_metrics());
  for (const auto& [key, val] : metadata.errors()) {
    (*proto_metadata->mutable_errors())[key] = val;
  }
}

std::variant<protocol::run::Result, fit_cxx::next_function>
from_search_result(couchbase::search_result res,
                   bool return_result,
                   std::string stream_id,
                   std::optional<protocol::shared::ContentAs> fields_as,
                   common::serializer serializer)
{
  if (return_result) {
    auto res_index = std::make_shared<std::int64_t>(0);
    auto facets_sent = std::make_shared<bool>(false);
    auto metadata_sent = std::make_shared<bool>(false);
    auto res_ptr = std::make_shared<couchbase::search_result>(std::move(res));

    return [res_ptr, res_index, facets_sent, metadata_sent, fields_as, stream_id, serializer]()
             -> std::optional<protocol::run::Result> {
      auto proto_res = fit_cxx::commands::common::create_new_result();
      proto_res.mutable_sdk()->mutable_search_streaming_result()->set_stream_id(stream_id);
      if (static_cast<std::size_t>(*res_index) >= res_ptr->rows().size()) {
        proto_res.mutable_sdk()->mutable_search_streaming_result()->set_stream_id(stream_id);
        if (!*facets_sent) {
          from_facets(res_ptr->facets(),
                      proto_res.mutable_sdk()
                        ->mutable_search_streaming_result()
                        ->mutable_facets()
                        ->mutable_facets());
          *facets_sent = true;
          return proto_res;
        }
        if (!*metadata_sent) {
          from_metadata(
            std::move(res_ptr->meta_data()),
            proto_res.mutable_sdk()->mutable_search_streaming_result()->mutable_meta_data());
          *metadata_sent = true;
          return proto_res;
        }
        return std::nullopt;
      }
      from_search_row(std::move(res_ptr->rows().at(static_cast<std::size_t>(*res_index))),
                      proto_res.mutable_sdk()->mutable_search_streaming_result()->mutable_row(),
                      fields_as,
                      serializer);
      *res_index = *res_index += 1;
      return proto_res;
    };
  }
  auto proto_res = fit_cxx::commands::common::create_new_result();
  proto_res.mutable_sdk()->set_success(true);
  return proto_res;
}

couchbase::vector_query_combination
to_vector_query_combination(const protocol::sdk::search::VectorQueryCombination& combination)
{
  switch (combination) {
    case protocol::sdk::search::AND:
      return couchbase::vector_query_combination::combination_and;
    case protocol::sdk::search::OR:
      return couchbase::vector_query_combination::combination_or;
    default:
      throw performer_exception::unimplemented("Vector query combination not recognised");
  }
}

couchbase::vector_search_options
to_vector_search_options(const protocol::sdk::search::VectorSearch& vector_search)
{
  couchbase::vector_search_options opts{};

  if (!vector_search.has_options()) {
    return opts;
  }
  if (vector_search.options().has_vector_query_combination()) {
    auto combination =
      to_vector_query_combination(vector_search.options().vector_query_combination());
    opts.query_combination(combination);
  }
  return opts;
}

std::vector<couchbase::vector_query>
to_vector_queries(
  const google::protobuf::RepeatedPtrField<protocol::sdk::search::VectorQuery>& vector_queries)
{
  std::vector<couchbase::vector_query> queries{};
  for (const auto& vector_query : vector_queries) {
    std::unique_ptr<couchbase::vector_query> query;
    if (vector_query.has_base64_vector_query()) {
      query = std::make_unique<couchbase::vector_query>(vector_query.vector_field_name(),
                                                        vector_query.base64_vector_query());
    } else {
      std::vector<double> vector_fields{ vector_query.vector_query().begin(),
                                         vector_query.vector_query().end() };
      query =
        std::make_unique<couchbase::vector_query>(vector_query.vector_field_name(), vector_fields);
    }
    if (!vector_query.has_options()) {
      queries.push_back(*query);
      continue;
    }
    if (vector_query.options().has_boost()) {
      query->boost(vector_query.options().boost());
    }
    if (vector_query.options().has_num_candidates()) {
      query->num_candidates(static_cast<std::uint32_t>(vector_query.options().num_candidates()));
    }
#ifdef COUCHBASE_CXX_CLIENT_HAS_VECTOR_SEARCH_PREFILTER
    if (vector_query.options().has_prefilter()) {
      auto prefilter_query = to_search_query(vector_query.options().prefilter());
      std::visit(common::overloaded{ [&query](auto&& arg) {
                   query->prefilter(arg);
                 } },
                 prefilter_query);
    }
#endif
    queries.push_back(*query);
  }
  return queries;
}

couchbase::vector_search
to_vector_search(const protocol::sdk::search::VectorSearch& vector_search)
{
  auto options = to_vector_search_options(vector_search);
  auto vector_queries = to_vector_queries(vector_search.vector_query());
  couchbase::vector_search search(vector_queries, options);
  return search;
}

couchbase::search_request
to_search_request(const protocol::sdk::search::SearchRequest& request)
{
  if (request.has_vector_search()) {
    auto vector = to_vector_search(request.vector_search());
    couchbase::search_request search_request(vector);
    if (request.has_search_query()) {
      auto query = to_search_query(request.search_query());
      std::visit(common::overloaded{ [&search_request](auto&& arg) {
                   search_request.search_query(arg);
                 } },
                 query);
    }
    return search_request;
  }
  if (request.has_search_query()) {
    auto query = to_search_query(request.search_query());
    auto search_request = std::visit(common::overloaded{ [](auto&& arg) {
                                       return couchbase::search_request(arg);
                                     } },
                                     query);
    return search_request;
  }
  throw performer_exception::unimplemented(
    "C++ Does not allow having both vector_search and search_query to be unset");
}

auto
execute_streaming_command(const protocol::sdk::search::SearchWrapper& wrapper,
                          fit_cxx::commands::search::command_args& args)
  -> std::variant<protocol::run::Result, fit_cxx::next_function>
{
  const auto& cmd = wrapper.search();

  couchbase::search_options options;
  std::unique_ptr<couchbase::search_request> request;

  try {
    options = to_search_options(cmd, args.spans);
    request = std::make_unique<couchbase::search_request>(to_search_request(cmd.request()));
  } catch (const std::invalid_argument& e) {
    auto proto_res = fit_cxx::commands::common::create_new_result();
    fit_cxx::commands::common::convert_error_code(
      couchbase::errc::make_error_code(couchbase::errc::common::invalid_argument),
      proto_res.mutable_sdk()->mutable_exception());
    return proto_res;
  }
  couchbase::error err;
  couchbase::search_result res;

  if (args.cluster) {
    auto cluster = args.cluster;
    auto cluster_result = cluster->search(cmd.indexname(), *request, options).get();
    err = std::move(cluster_result.first);
    res = std::move(cluster_result.second);
  } else if (args.scope.has_value()) {
    auto scope = args.scope.value();
    auto scope_result = scope.search(cmd.indexname(), *request, options).get();
    err = std::move(scope_result.first);
    res = std::move(scope_result.second);
  } else {
    throw performer_exception::invalid_argument(
      "Neither cluster nor scope specified in search command args");
  }

  if (err.ec()) {
    auto proto_res = fit_cxx::commands::common::create_new_result();
    fit_cxx::commands::common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
    return proto_res;
  }
  std::optional<protocol::shared::ContentAs> fields_as;
  if (wrapper.has_fields_as()) {
    fields_as = wrapper.fields_as();
  }
  auto serializer = to_serializer(cmd);
  return from_search_result(
    std::move(res), args.return_result, wrapper.stream_config().stream_id(), fields_as, serializer);
}
} // namespace fit_cxx::commands::search
