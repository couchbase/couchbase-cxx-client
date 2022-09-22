/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include <tao/json/value.hpp>

#include <string>

struct AnotherSimpleObject {
    std::string foo;

    bool operator==(const AnotherSimpleObject& other) const
    {
        return foo == other.foo;
    }
};

template<>
struct tao::json::traits<AnotherSimpleObject> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const AnotherSimpleObject& p)
    {
        v = {
            { "foo", p.foo },
        };
    }

    template<template<typename...> class Traits>
    static AnotherSimpleObject as(const tao::json::basic_value<Traits>& v)
    {
        AnotherSimpleObject result;
        const auto& object = v.get_object();
        result.foo = object.at("foo").template as<std::string>();
        return result;
    }
};
