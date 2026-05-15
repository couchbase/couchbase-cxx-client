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

#pragma once

#include "run.top_level.pb.h"
#include "sdk.search.pb.h"

#include "../observability/span_owner.hxx"
#include "../stream.hxx"

#include <couchbase/cluster.hxx>
#include <couchbase/scope.hxx>

#include <couchbase/boolean_field_query.hxx>
#include <couchbase/boolean_query.hxx>
#include <couchbase/conjunction_query.hxx>
#include <couchbase/date_range_query.hxx>
#include <couchbase/disjunction_query.hxx>
#include <couchbase/doc_id_query.hxx>
#include <couchbase/geo_bounding_box_query.hxx>
#include <couchbase/geo_distance_query.hxx>
#include <couchbase/match_all_query.hxx>
#include <couchbase/match_none_query.hxx>
#include <couchbase/match_phrase_query.hxx>
#include <couchbase/match_query.hxx>
#include <couchbase/numeric_range_query.hxx>
#include <couchbase/phrase_query.hxx>
#include <couchbase/prefix_query.hxx>
#include <couchbase/query_string_query.hxx>
#include <couchbase/regexp_query.hxx>
#include <couchbase/term_query.hxx>
#include <couchbase/term_range_query.hxx>
#include <couchbase/wildcard_query.hxx>

namespace fit_cxx::commands::search
{

using search_query_types = std::variant<couchbase::match_query,
                                        couchbase::match_phrase_query,
                                        couchbase::regexp_query,
                                        couchbase::query_string_query,
                                        couchbase::wildcard_query,
                                        couchbase::doc_id_query,
                                        couchbase::boolean_field_query,
                                        couchbase::date_range_query,
                                        couchbase::numeric_range_query,
                                        couchbase::term_range_query,
                                        couchbase::geo_distance_query,
                                        couchbase::geo_bounding_box_query,
                                        couchbase::conjunction_query,
                                        couchbase::disjunction_query,
                                        couchbase::boolean_query,
                                        couchbase::term_query,
                                        couchbase::prefix_query,
                                        couchbase::phrase_query,
                                        couchbase::match_all_query,
                                        couchbase::match_none_query>;

struct command_args {
  std::shared_ptr<couchbase::cluster> cluster{};
  std::optional<couchbase::scope> scope{};
  observability::span_owner* spans;
  bool return_result{ true };
};

search_query_types
to_search_query(const protocol::sdk::search::SearchQuery& query);

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::search::SearchWrapper& cmd, command_args& args);
} // namespace fit_cxx::commands::search
