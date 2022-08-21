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

struct SimpleObject {
    std::string name;
    std::uint64_t number;

    bool operator==(const SimpleObject& other) const
    {
        return (name == other.name) && (number == other.number);
    }
};

template<>
struct tao::json::traits<SimpleObject> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const SimpleObject& p)
    {
        v = {
            { "name", p.name },
            { "number", p.number },
        };
    }

    template<template<typename...> class Traits>
    static SimpleObject as(const tao::json::basic_value<Traits>& v)
    {
        SimpleObject result;
        const auto& object = v.get_object();
        result.name = object.at("name").template as<std::string>();
        result.number = object.at("number").template as<std::uint64_t>();
        return result;
    }
};
