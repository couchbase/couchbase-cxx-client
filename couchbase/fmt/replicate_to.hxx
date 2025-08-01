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

#include <couchbase/replicate_to.hxx>

/**
 * Helper for fmtlib to format @ref couchbase::replicate_to objects.
 *
 * @since 1.2.0
 * @committed
 */
template<>
struct fmt::formatter<couchbase::replicate_to> {
  template<typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    return ctx.begin();
  }

  template<typename FormatContext>
  auto format(couchbase::replicate_to value, FormatContext& ctx) const
  {
    string_view name = "unknown";
    switch (value) {
      case couchbase::replicate_to::none:
        name = "none";
        break;
      case couchbase::replicate_to::one:
        name = "one";
        break;
      case couchbase::replicate_to::two:
        name = "two";
        break;
      case couchbase::replicate_to::three:
        name = "three";
        break;
    }
    return format_to(ctx.out(), "{}", name);
  }
};
