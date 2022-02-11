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

#include <couchbase/metrics/meter.hxx>

namespace couchbase::metrics
{

class noop_value_recorder : public value_recorder
{
  public:
    void record_value(std::int64_t /* value */) override
    {
        /* do nothing */
    }
};

class noop_meter : public meter
{
  private:
    std::shared_ptr<noop_value_recorder> instance_{ std::make_shared<noop_value_recorder>() };

  public:
    std::shared_ptr<value_recorder> get_value_recorder(const std::string& /* name */,
                                                       const std::map<std::string, std::string>& /* tags */) override
    {
        return instance_;
    }
};

} // namespace couchbase::metrics
