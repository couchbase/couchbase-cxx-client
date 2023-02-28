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

#include "core/utils/split_string.hxx"

#include "internal/exceptions_internal.hxx"
#include "internal/logging.hxx"

#include <chrono>
#include <list>
#include <optional>
#include <string>
#include <vector>

namespace couchbase::core::transactions
{
enum class forward_compat_stage {
    WWC_READING_ATR,
    WWC_REPLACING,
    WWC_REMOVING,
    WWC_INSERTING,
    WWC_INSERTING_GET,
    GETS,
    GETS_READING_ATR,
    CLEANUP_ENTRY
};

static forward_compat_stage
create_forward_compat_stage(const std::string& str)
{
    if (str == "WW_R") {
        return forward_compat_stage::WWC_READING_ATR;
    } else if (str == "WW_RP") {
        return forward_compat_stage::WWC_REPLACING;
    } else if (str == "WW_RM") {
        return forward_compat_stage::WWC_REMOVING;
    } else if (str == "WW_I") {
        return forward_compat_stage::WWC_INSERTING;
    } else if (str == "WW_IG") {
        return forward_compat_stage::WWC_INSERTING_GET;
    } else if (str == "G") {
        return forward_compat_stage::GETS;
    } else if (str == "G_A") {
        return forward_compat_stage::GETS_READING_ATR;
    } else if (str == "CL_E") {
        return forward_compat_stage::CLEANUP_ENTRY;
    }
    throw std::runtime_error("Unknown forward compatibility stage");
}

enum class forward_compat_behavior { CONTINUE, RETRY_TXN, FAIL_FAST_TXN };

inline forward_compat_behavior
create_forward_compat_behavior(const std::string& str)
{
    if (str == "r") {
        return forward_compat_behavior::RETRY_TXN;
    }
    return forward_compat_behavior::FAIL_FAST_TXN;
}

// used only for logging
inline const char*
forward_compat_behavior_name(forward_compat_behavior b)
{
    switch (b) {
        case forward_compat_behavior::CONTINUE:
            return "CONTINUE";
        case forward_compat_behavior::RETRY_TXN:
            return "RETRY_TXN";
        case forward_compat_behavior::FAIL_FAST_TXN:
            return "FAIL_FAST_TRANSACTION";
    }
    return "unknown behavior";
}

struct forward_compat_behavior_full {
    forward_compat_behavior behavior;
    std::optional<std::chrono::milliseconds> retry_delay;

    forward_compat_behavior_full(forward_compat_behavior b, std::optional<std::chrono::milliseconds> r)
      : behavior(b)
      , retry_delay(r)
    {
    }

    forward_compat_behavior_full(const tao::json::value& j)
    {
        std::string b = j.at("b").get_string();
        behavior = create_forward_compat_behavior(b);
        if (const auto* ra = j.find("ra"); ra != nullptr) {
            retry_delay = std::chrono::milliseconds(ra->get_unsigned());
        }
    }

    template<typename OStream>
    friend OStream& operator<<(OStream& os, const forward_compat_behavior_full& b)
    {
        os << "forward_compat_behavior_full:{";
        os << "behavior: " << forward_compat_behavior_name(b.behavior);
        if (b.retry_delay) {
            os << ", retry_delay: " << b.retry_delay->count() << " ms";
        }
        os << "}";
    }
};

struct forward_compat_supported {
    uint32_t protocol_major = 2;
    uint32_t protocol_minor = 0;
    std::list<std::string> extensions{ "TI", "MO", "BM",     "QU", "SD", "BF3787", "BF3705", "BF3838", "RC",
                                       "UA", "CO", "BF3791", "CM", "SI", "QC",     "IX",     "TS" };
};

struct forward_compat_requirement {
    forward_compat_behavior_full behavior;

    forward_compat_requirement(forward_compat_behavior_full b)
      : behavior(b)
    {
    }

    virtual forward_compat_behavior_full check(forward_compat_supported supported) = 0;
    virtual ~forward_compat_requirement() = default;
};

struct forward_compat_protocol_requirement : public forward_compat_requirement {
    uint32_t min_protocol_major;
    uint32_t min_protocol_minor;

    forward_compat_protocol_requirement(forward_compat_behavior_full b, uint32_t min_major, uint32_t min_minor)
      : forward_compat_requirement(b)
      , min_protocol_major(min_major)
      , min_protocol_minor(min_minor)
    {
    }

    virtual forward_compat_behavior_full check(forward_compat_supported supported)
    {
        if ((min_protocol_major > supported.protocol_major) || (min_protocol_minor > supported.protocol_minor)) {
            return behavior;
        }
        return { forward_compat_behavior::CONTINUE, std::nullopt };
    }
};

struct forward_compat_extension_requirement : public forward_compat_requirement {
    std::string extension_id;

    forward_compat_extension_requirement(forward_compat_behavior_full b, const std::string ext)
      : forward_compat_requirement(b)
      , extension_id(ext)
    {
    }

    virtual forward_compat_behavior_full check(forward_compat_supported supported)
    {
        auto it = std::find(supported.extensions.begin(), supported.extensions.end(), extension_id);
        if (it == supported.extensions.end()) {
            return behavior;
        }
        return { forward_compat_behavior::CONTINUE, std::nullopt };
    }
};

class forward_compat
{
  private:
    std::map<forward_compat_stage, std::list<forward_compat_requirement*>> compat_map_;
    tao::json::value json_;

    std::optional<transaction_operation_failed> check_internal(forward_compat_stage stage, forward_compat_supported supported)
    {
        if (auto it = compat_map_.find(stage); it != compat_map_.end()) {
            transaction_operation_failed ex(FAIL_OTHER, "Forward Compatibililty failure");
            ex.cause(FORWARD_COMPATIBILITY_FAILURE);
            for (const auto& b : it->second) {
                auto behavior = b->check(supported);
                switch (behavior.behavior) {
                    case forward_compat_behavior::FAIL_FAST_TXN:
                        CB_TXN_LOG_TRACE("forward compatiblity FAIL_FAST_TXN");
                        return ex;
                    case forward_compat_behavior::RETRY_TXN:
                        CB_TXN_LOG_TRACE("forward compatibility RETRY_TXN");
                        if (behavior.retry_delay) {
                            CB_TXN_LOG_TRACE("delay {}ms before retrying", behavior.retry_delay->count());
                            std::this_thread::sleep_for(*behavior.retry_delay);
                        }
                        return ex.retry();
                    case forward_compat_behavior::CONTINUE:
                    default:
                        break;
                }
            }
        }
        return std::nullopt;
    }

    forward_compat(tao::json::value& json)
      : json_(json)
    {
        CB_TXN_LOG_TRACE("creating forward_compat from {}", core::utils::json::generate(json_));
        // parse it into the map
        for (const auto& [key, value] : json_.get_object()) {
            auto stage = create_forward_compat_stage(key);
            // the value is an array of forward_compat_behavior_full objects
            for (const auto& item : value.get_array()) {
                forward_compat_behavior_full behavior(item);
                if (const auto* b = item.find("b"); b != nullptr) {
                    if (const auto* e = item.find("e")) {
                        std::string ext = e->get_string();
                        compat_map_[stage].push_back(new forward_compat_extension_requirement(behavior, ext));
                    } else if (const auto* p = item.find("p")) {
                        std::string proto_string = p->get_string();
                        auto proto = core::utils::split_string(proto_string, '.');
                        compat_map_[stage].push_back(new forward_compat_protocol_requirement(
                          behavior, static_cast<std::uint32_t>(std::stoi(proto[0])), static_cast<std::uint32_t>(std::stoi(proto[1]))));
                    }
                }
            }
        }
    }

  public:
    static std::optional<transaction_operation_failed> check(forward_compat_stage stage, std::optional<tao::json::value> json)
    {
        if (json) {
            forward_compat_supported supported;
            forward_compat fc(json.value());
            return fc.check_internal(stage, supported);
        }
        return std::nullopt;
    }
};

} // namespace couchbase::core::transactions
