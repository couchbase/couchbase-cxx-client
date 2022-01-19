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

#include <couchbase/tracing/request_tracer.hxx>

namespace couchbase::tracing
{

class noop_span : public request_span
{
    void add_tag(const std::string& /* name */, std::uint64_t /* value */) override
    {
        /* do nothing */
    }

    void add_tag(const std::string& /* name */, const std::string& /* value */) override
    {
        /* do nothing */
    }

    void end() override
    {
        /* do nothing */
    }
};

class noop_tracer : public request_tracer
{
  private:
    std::shared_ptr<noop_span> instance_{ std::make_shared<noop_span>() };

  public:
    std::shared_ptr<request_span> start_span(std::string /* name */, std::shared_ptr<request_span> /* parent */) override
    {
        return instance_;
    }
};

} // namespace couchbase::tracing
