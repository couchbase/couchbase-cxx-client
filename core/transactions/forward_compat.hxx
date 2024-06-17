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

#include "internal/exceptions_internal.hxx"
#include <cstdint>
#include <list>
#include <optional>
#include <stdexcept>
#include <string>

namespace couchbase::core::transactions
{
// TODO(SA): rename using long and human-readable names
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

inline auto
to_string(forward_compat_stage value) -> const char*
{
  switch (value) {
    case forward_compat_stage::WWC_READING_ATR:
      return "WW_R";
    case forward_compat_stage::WWC_REPLACING:
      return "WW_RP";
    case forward_compat_stage::WWC_REMOVING:
      return "WW_RM";
    case forward_compat_stage::WWC_INSERTING:
      return "WW_I";
    case forward_compat_stage::WWC_INSERTING_GET:
      return "WW_IG";
    case forward_compat_stage::GETS:
      return "G";
    case forward_compat_stage::GETS_READING_ATR:
      return "G_A";
    case forward_compat_stage::CLEANUP_ENTRY:
      return "CL_E";
  };
  throw std::runtime_error("Unknown forward compatibility stage");
}

enum class forward_compat_behavior {
  CONTINUE,
  RETRY_TXN,
  FAIL_FAST_TXN
};

inline auto
create_forward_compat_behavior(const std::string& str) -> forward_compat_behavior
{
  if (str == "r") {
    return forward_compat_behavior::RETRY_TXN;
  }
  return forward_compat_behavior::FAIL_FAST_TXN;
}

// used only for logging
inline auto
forward_compat_behavior_name(forward_compat_behavior b) -> const char*
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

struct forward_compat_supported {
  std::uint32_t protocol_major = 2;
  std::uint32_t protocol_minor = 0;
  std::list<std::string> extensions{
    "TI", "MO",     "BM", "QU", "SD", "BF3787", "BF3705", "BF3838", "RC", "UA",
    "CO", "BF3791", "CM", "SI", "QC", "IX",     "TS",     "PU",     "BS", "RP",
  };
};

class transaction_operation_failed;

std::optional<transaction_operation_failed>
check_forward_compat(forward_compat_stage stage, std::optional<tao::json::value> json);
} // namespace couchbase::core::transactions
