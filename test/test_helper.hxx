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

#include "utils/binary.hxx"
#include "utils/test_context.hxx"
#include "utils/test_data.hxx"

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_tostring.hpp>
#include <spdlog/fmt/bundled/format.h>

#include <couchbase/fmt/error.hxx>

/**
 * This will make Catch2 show the contents of the error when used in an assertion that fails.
 */
template<>
struct Catch::StringMaker<couchbase::error> {
  static auto convert(const couchbase::error& err) -> std::string
  {
    return fmt::format("couchbase::error{{ ec: {}, msg: {}, ctx:{}, cause:{} }}",
                       err.ec().message(),
                       err.message(),
                       err.ctx().to_json(),
                       err.cause().has_value() ? convert(err) : "<unset>");
  }
};

#define REQUIRE_SUCCESS(ec)                                                                        \
  INFO((ec).message());                                                                            \
  REQUIRE_FALSE(ec)
#define EXPECT_SUCCESS(result)                                                                     \
  if (!(result)) {                                                                                 \
    INFO((result).error().message());                                                              \
  }                                                                                                \
  REQUIRE(result)
#define REQUIRE_NO_ERROR(err)                                                                      \
  if (err) {                                                                                       \
    INFO(fmt::format("Expected no error. Got: {}.", err));                                         \
  }                                                                                                \
  REQUIRE_FALSE((err));
