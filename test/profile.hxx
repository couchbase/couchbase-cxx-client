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

#pragma once

#include <cinttypes>
#include <string>

#include <tao/json/forward.hpp>

struct profile {
    std::string username{};
    std::string full_name{};
    std::uint32_t birth_year{};

    bool operator==(const profile& other) const
    {
        return username == other.username && full_name == other.full_name && birth_year == other.birth_year;
    }
};

template<>
struct tao::json::traits<profile> {
    template<template<typename...> class Traits>
    static void assign(tao::json::basic_value<Traits>& v, const profile& p)
    {
        v = {
            { "username", p.username },
            { "full_name", p.full_name },
            { "birth_year", p.birth_year },
        };
    }

    template<template<typename...> class Traits>
    static profile as(const tao::json::basic_value<Traits>& v)
    {
        profile result;
        const auto& object = v.get_object();
        result.username = object.at("username").template as<std::string>();
        result.full_name = object.at("full_name").template as<std::string>();
        if (object.count("birth_year") != 0) {
            // expect incomplete JSON here, as we might use projections to fetch reduced document
            // as an alternative we might use std::optional<> here
            result.birth_year = object.at("birth_year").template as<std::uint32_t>();
        }
        return result;
    }
};