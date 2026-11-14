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

#include "document_get_projected.hxx"

#include "core/impl/subdoc/command_bundle.hxx"
#include "core/utils/json.hxx"

#include <couchbase/error_codes.hxx>
#include <couchbase/lookup_in_specs.hxx>

#include <tao/json/value.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace couchbase::core::operations
{

namespace
{
constexpr std::size_t max_subdoc_path_length = 1024;
constexpr std::size_t max_subdoc_components = 31;

enum class path_step_kind : std::uint8_t {
  field,
  element,
};

struct path_step {
  path_step_kind kind{ path_step_kind::field };
  std::string name{};
  std::size_t index{ 0 };

  auto operator<(const path_step& other) const noexcept -> bool
  {
    return std::tie(kind, index, name) < std::tie(other.kind, other.index, other.name);
  }

  auto operator==(const path_step& other) const noexcept -> bool
  {
    return kind == other.kind && index == other.index && name == other.name;
  }
};

using structural_path = std::vector<path_step>;

struct parsed_step {
  path_step_kind kind{ path_step_kind::field };
  std::string name{};
  std::int64_t array_index{ 0 };
};

using parsed_path = std::vector<parsed_step>;

enum class path_parse_status : std::uint8_t {
  ok,
  invalid,
  too_big,
  too_long,
};

struct array_slot_cache {
  std::map<std::int64_t, std::size_t> slots{};
  std::size_t seeded_size{ 0 };
};

using array_slot_map = std::map<structural_path, array_slot_cache>;

struct resolved_projection {
  parsed_path steps{};
  tao::json::value value{};
};

enum class lookup_status : std::uint8_t {
  found,
  not_found,
  path_mismatch,
  path_invalid,
  path_too_big,
  invalid_argument,
};

struct lookup_result {
  lookup_status status{ lookup_status::not_found };
  resolved_projection projection{};
};

auto
parse_array_index(std::string_view text, std::int64_t& out) -> bool
{
  // Subdoc::Path::add_num_component treats an empty bracket component ([]) as index 0.
  if (text.empty()) {
    out = 0;
    return true;
  }
  if (text.front() == '-') {
    if (text != "-1") {
      return false;
    }
    out = -1;
    return true;
  }
  const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
  return ec == std::errc{} && ptr == text.data() + text.size() && out >= 0;
}

auto
parse_u32_field(std::string_view text, std::uint32_t& out) -> bool
{
  if (text.empty()) {
    return false;
  }
  const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
  return ec == std::errc{} && ptr == text.data() + text.size();
}

auto
unescape_json_char(char ch, char& out) -> bool
{
  switch (ch) {
    case '"':
      out = '"';
      return true;
    case '/':
      out = '/';
      return true;
    case '\\':
      out = '\\';
      return true;
    case 'b':
      out = '\b';
      return true;
    case 'f':
      out = '\f';
      return true;
    case 'n':
      out = '\n';
      return true;
    case 'r':
      out = '\r';
      return true;
    case 't':
      out = '\t';
      return true;
    default:
      return false;
  }
}

auto
add_string_step(std::string_view raw,
                std::size_t n_backticks,
                bool has_json_escape,
                parsed_path& steps) -> path_parse_status
{
  if (raw.size() > 1 && raw.front() == '`' && raw.back() == '`') {
    raw.remove_prefix(1);
    raw.remove_suffix(1);
    n_backticks -= 2;
  }
  if (steps.size() == max_subdoc_components) {
    return path_parse_status::too_big;
  }
  if (raw.empty()) {
    return path_parse_status::invalid;
  }
  if (n_backticks == 0 && !has_json_escape) {
    steps.push_back(parsed_step{ path_step_kind::field, std::string{ raw }, 0 });
    return path_parse_status::ok;
  }
  std::string decoded;
  decoded.reserve(raw.size());
  for (std::size_t i = 0; i < raw.size(); ++i) {
    if (raw[i] == '\\') {
      char unescaped = '\0';
      if (i + 1 >= raw.size() || !unescape_json_char(raw[i + 1], unescaped)) {
        return path_parse_status::invalid;
      }
      decoded.push_back(unescaped);
      ++i;
    } else if (raw[i] == '`') {
      if (i + 1 < raw.size() && raw[i + 1] == '`') {
        decoded.push_back('`');
        ++i;
      }
    } else {
      decoded.push_back(raw[i]);
    }
  }
  steps.push_back(parsed_step{ path_step_kind::field, std::move(decoded), 0 });
  return path_parse_status::ok;
}

auto
parse_string_component(std::string_view input, std::size_t& consumed, parsed_path& steps)
  -> path_parse_status
{
  if (input.empty()) {
    return path_parse_status::invalid;
  }
  bool in_backticks = false;
  bool in_json_escape = false;
  bool has_json_escape = false;
  std::size_t n_backticks = 0;

  for (std::size_t i = 0; i < input.size(); ++i) {
    const auto ch = input[i];
    const auto byte = static_cast<unsigned char>(ch);
    if (in_json_escape) {
      char unused = '\0';
      if (!unescape_json_char(ch, unused)) {
        return path_parse_status::invalid;
      }
      in_json_escape = false;
    } else if (ch == '\\') {
      in_json_escape = true;
      has_json_escape = true;
    } else if (ch == '"' || byte < 0x1FU) {
      return path_parse_status::invalid;
    }

    if (ch == '`') {
      ++n_backticks;
      in_backticks = !in_backticks;
    }
    if (in_backticks) {
      continue;
    }

    if (ch == ']') {
      return path_parse_status::invalid;
    }
    if (ch == '[' || ch == '.') {
      if (in_json_escape) {
        return path_parse_status::invalid;
      }
      consumed = (ch == '.') ? (i + 1) : i;
      return add_string_step(input.substr(0, i), n_backticks, has_json_escape, steps);
    }
  }

  if (in_backticks || in_json_escape) {
    return path_parse_status::invalid;
  }
  consumed = input.size();
  return add_string_step(input, n_backticks, has_json_escape, steps);
}

auto
parse_projection_path(std::string_view path, parsed_path& steps) -> path_parse_status
{
  steps.clear();
  // kv_engine's subdoc_multi_validator rejects paths > SUBDOC_PATH_MAX_LENGTH (1024) with
  // Status::Einval (errc::common::invalid_argument), whereas > 31 path components fail in
  // Subdoc::Path::parse with Status::SubdocPathE2big (errc::key_value::path_too_big).
  if (path.size() > max_subdoc_path_length) {
    return path_parse_status::too_long;
  }
  std::size_t pos = 0;
  while (pos < path.size()) {
    if (path[pos] == '[') {
      const auto close = path.find(']', pos + 1);
      if (close == std::string_view::npos) {
        return path_parse_status::invalid;
      }
      std::int64_t array_index = 0;
      if (!parse_array_index(path.substr(pos + 1, close - (pos + 1)), array_index)) {
        return path_parse_status::invalid;
      }
      if (steps.size() == max_subdoc_components) {
        return path_parse_status::too_big;
      }
      steps.push_back(parsed_step{ path_step_kind::element, {}, array_index });
      pos = close + 1;
      if (pos == path.size() || path[pos] == '[') {
        continue;
      }
      if (path[pos] == '.') {
        ++pos;
        continue;
      }
      return path_parse_status::invalid;
    }
    std::size_t consumed = 0;
    const auto status = parse_string_component(path.substr(pos), consumed, steps);
    if (status != path_parse_status::ok) {
      return status;
    }
    pos += consumed;
  }
  return path_parse_status::ok;
}

auto
can_resolve_projection_locally(std::string_view path, parsed_path& steps) -> bool
{
  return parse_projection_path(path, steps) == path_parse_status::ok;
}

auto
has_mixed_sign_array_indexes(const std::vector<std::string>& projections) -> bool
{
  std::vector<parsed_path> all_parsed;
  all_parsed.reserve(projections.size());
  for (const auto& path : projections) {
    parsed_path steps;
    if (!can_resolve_projection_locally(path, steps)) {
      return false;
    }
    all_parsed.push_back(std::move(steps));
  }

  std::map<structural_path, unsigned int> seen_signs_by_array;
  for (const auto& steps : all_parsed) {
    structural_path prefix;
    prefix.reserve(steps.size());
    for (const auto& step : steps) {
      if (step.kind == path_step_kind::field) {
        prefix.push_back(path_step{ path_step_kind::field, step.name, 0 });
      } else {
        auto& mask = seen_signs_by_array[prefix];
        mask |= (step.array_index < 0) ? 2U : 1U;
        if (mask == 3U) {
          return true;
        }
        prefix.push_back(path_step{
          path_step_kind::element,
          step.array_index < 0 ? "-" : "",
          step.array_index < 0 ? std::size_t{ 0 } : static_cast<std::size_t>(step.array_index),
        });
      }
    }
  }
  return false;
}

auto
has_path_prefix(const structural_path& path, const structural_path& prefix) -> bool
{
  return path.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), path.begin());
}

void
erase_array_slots_subtree(array_slot_map& array_slots, const structural_path& prefix)
{
  if (array_slots.empty()) {
    return;
  }
  for (auto it = array_slots.lower_bound(prefix);
       it != array_slots.end() && has_path_prefix(it->first, prefix);) {
    it = array_slots.erase(it);
  }
}

void
relocate_array_slots_subtree(array_slot_map& array_slots,
                             const structural_path& old_prefix,
                             const structural_path& new_prefix)
{
  if (array_slots.empty() || old_prefix == new_prefix) {
    return;
  }
  array_slot_map relocated;
  for (auto it = array_slots.lower_bound(old_prefix);
       it != array_slots.end() && has_path_prefix(it->first, old_prefix);) {
    structural_path updated_key = new_prefix;
    updated_key.insert(updated_key.end(),
                       it->first.begin() + static_cast<std::ptrdiff_t>(old_prefix.size()),
                       it->first.end());
    relocated.emplace(std::move(updated_key), std::move(it->second));
    it = array_slots.erase(it);
  }
  for (auto& [key, val] : relocated) {
    array_slots[key] = std::move(val);
  }
}

auto
subdoc_lookup(tao::json::value& root, std::string_view path) -> lookup_result
{
  parsed_path steps;
  switch (parse_projection_path(path, steps)) {
    case path_parse_status::ok:
      break;
    case path_parse_status::invalid:
      return { lookup_status::path_invalid, {} };
    case path_parse_status::too_big:
      return { lookup_status::path_too_big, {} };
    case path_parse_status::too_long:
      return { lookup_status::invalid_argument, {} };
  }
  if (steps.empty()) {
    return { lookup_status::not_found, {} };
  }

  tao::json::value* cur = &root;
  for (auto& step : steps) {
    if (step.kind == path_step_kind::field) {
      if (!cur->is_object()) {
        return { lookup_status::path_mismatch, {} };
      }
      auto* val = cur->find(step.name);
      if (val == nullptr) {
        return { lookup_status::not_found, {} };
      }
      cur = val;
    } else {
      if (!cur->is_array()) {
        return { lookup_status::path_mismatch, {} };
      }
      const auto& arr = cur->get_array();
      if (arr.empty()) {
        return { lookup_status::not_found, {} };
      }
      std::size_t resolved_index = 0;
      if (step.array_index == -1) {
        resolved_index = arr.size() - 1;
      } else if (step.array_index >= 0 && static_cast<std::size_t>(step.array_index) < arr.size()) {
        resolved_index = static_cast<std::size_t>(step.array_index);
      } else {
        return { lookup_status::not_found, {} };
      }
      step.array_index = static_cast<std::int64_t>(resolved_index);
      cur = &cur->get_array()[resolved_index];
    }
  }

  return { lookup_status::found, { std::move(steps), *cur } };
}

void
subdoc_apply_projection(tao::json::value& root,
                        const parsed_path& steps,
                        const tao::json::value& value,
                        bool preserve_array_indexes,
                        array_slot_map& array_slots)
{
  if (steps.empty()) {
    return;
  }
  tao::json::value* cur = &root;
  structural_path current_path;
  current_path.reserve(steps.size());

  for (std::size_t i = 0; i < steps.size(); ++i) {
    const auto& step = steps[i];
    const bool is_last = (i + 1 == steps.size());

    if (step.kind == path_step_kind::field) {
      if (!cur->is_object()) {
        return;
      }
      if (is_last) {
        if (const auto* existing = cur->find(step.name);
            existing != nullptr && (existing->is_array() || existing->is_object())) {
          current_path.push_back(path_step{ path_step_kind::field, step.name, 0 });
          erase_array_slots_subtree(array_slots, current_path);
        }
        cur->operator[](step.name) = value;
        return;
      }
      tao::json::value* child = cur->find(step.name);
      if (child == nullptr) {
        cur->operator[](step.name) = (steps[i + 1].kind == path_step_kind::element)
                                       ? tao::json::value(tao::json::empty_array)
                                       : tao::json::value(tao::json::empty_object);
        child = cur->find(step.name);
      }
      current_path.push_back(path_step{ path_step_kind::field, step.name, 0 });
      cur = child;
      continue;
    }

    if (i == 0 && cur->is_object() && cur->get_object().empty()) {
      *cur = tao::json::empty_array;
    }
    if (!cur->is_array()) {
      return;
    }
    const auto array_index = step.array_index;
    auto [cache_it, inserted] = array_slots.try_emplace(current_path);
    auto& cache = cache_it->second;
    if (inserted) {
      cache.seeded_size = cur->get_array().size();
    }
    auto& slots = cache.slots;
    if (array_index == -1 && cache.seeded_size > 0 && slots.find(-1) == slots.end()) {
      slots[-1] = cache.seeded_size - 1;
    } else if (array_index >= 0 && static_cast<std::size_t>(array_index) < cache.seeded_size &&
               slots.find(array_index) == slots.end()) {
      slots[array_index] = static_cast<std::size_t>(array_index);
    }

    std::size_t pos = 0;
    if (!preserve_array_indexes) {
      if (auto it = slots.find(array_index);
          it != slots.end() && it->second < cur->get_array().size()) {
        pos = it->second;
      } else {
        pos = cur->get_array().size();
        cur->get_array().emplace_back(tao::json::uninitialized);
        slots[array_index] = pos;
      }
    } else if (array_index >= 0) {
      pos = static_cast<std::size_t>(array_index);
      if (auto neg_it = slots.find(-1);
          neg_it != slots.end() && neg_it->second <= pos &&
          neg_it->second < cur->get_array().size() && neg_it->second >= cache.seeded_size &&
          slots.find(static_cast<std::int64_t>(neg_it->second)) == slots.end()) {
        const auto old_pos = neg_it->second;
        auto neg_val = std::move(cur->at(old_pos));
        cur->at(old_pos) = tao::json::uninitialized;
        if (pos >= cur->get_array().size()) {
          cur->get_array().resize(pos + 1, tao::json::uninitialized);
        }
        const auto new_pos = cur->get_array().size();
        neg_it->second = new_pos;
        cur->get_array().push_back(std::move(neg_val));
        structural_path old_elem_path = current_path;
        old_elem_path.push_back(path_step{ path_step_kind::element, {}, old_pos });
        structural_path new_elem_path = current_path;
        new_elem_path.push_back(path_step{ path_step_kind::element, {}, new_pos });
        relocate_array_slots_subtree(array_slots, old_elem_path, new_elem_path);
      } else if (pos >= cur->get_array().size()) {
        cur->get_array().resize(pos + 1, tao::json::uninitialized);
      }
      slots[array_index] = pos;
    } else {
      if (auto it = slots.find(-1); it != slots.end() && it->second < cur->get_array().size()) {
        pos = it->second;
      } else {
        pos = cur->get_array().size();
        cur->get_array().emplace_back(tao::json::uninitialized);
        slots[-1] = pos;
      }
    }

    current_path.push_back(path_step{ path_step_kind::element, {}, pos });
    if (is_last) {
      if (cur->at(pos).is_array() || cur->at(pos).is_object()) {
        erase_array_slots_subtree(array_slots, current_path);
      }
      cur->at(pos) = value;
      return;
    }
    if (cur->at(pos).is_uninitialized()) {
      cur->at(pos) = (steps[i + 1].kind == path_step_kind::element)
                       ? tao::json::value(tao::json::empty_array)
                       : tao::json::value(tao::json::empty_object);
    }
    cur = &cur->at(pos);
  }
}

void
subdoc_apply_projection(tao::json::value& root,
                        std::string_view path,
                        const tao::json::value& value,
                        bool preserve_array_indexes,
                        array_slot_map& array_slots)
{
  parsed_path steps;
  if (parse_projection_path(path, steps) != path_parse_status::ok) {
    return;
  }
  subdoc_apply_projection(root, steps, value, preserve_array_indexes, array_slots);
}

void
finalize_projected_arrays(tao::json::value& node)
{
  if (node.is_object()) {
    for (auto& [_, child] : node.get_object()) {
      finalize_projected_arrays(child);
    }
    return;
  }
  if (node.is_array()) {
    for (auto& elem : node.get_array()) {
      if (elem.is_uninitialized()) {
        elem = tao::json::null;
      } else {
        finalize_projected_arrays(elem);
      }
    }
  }
}

auto
initial_projected_document(const std::vector<std::string>& projections) -> tao::json::value
{
  for (const auto& path : projections) {
    parsed_path steps;
    if (parse_projection_path(path, steps) == path_parse_status::ok && !steps.empty()) {
      return (steps.front().kind == path_step_kind::element)
               ? tao::json::value(tao::json::empty_array)
               : tao::json::value(tao::json::empty_object);
    }
  }
  return tao::json::empty_object;
}
} // namespace

auto
get_projected_request::encode_to(get_projected_request::encoded_request_type& encoded,
                                 mcbp_context&& /* context */) -> std::error_code
{
  encoded.opaque(opaque);
  encoded.partition(partition);
  encoded.body().id(id);

  for (const auto& path : projections) {
    if (path.size() > max_subdoc_path_length) {
      return errc::common::invalid_argument;
    }
  }

  effective_projections = projections;
  std::size_t num_projections = effective_projections.size();
  num_projections++; // flags
  if (with_expiry) {
    num_projections++;
  }
  if (num_projections > 16 || has_mixed_sign_array_indexes(effective_projections)) {
    // too many subdoc operations, or mixed positive and negative indexes on the same array
    // require the source array length to resolve aliases; fetch full document
    effective_projections.clear();
  }

  couchbase::lookup_in_specs specs{
    couchbase::lookup_in_specs::get(subdoc::lookup_in_macro::flags).xattr(),
  };
  if (with_expiry) {
    specs.push_back(couchbase::lookup_in_specs::get(subdoc::lookup_in_macro::expiry_time).xattr());
  }
  if (effective_projections.empty()) {
    specs.push_back(couchbase::lookup_in_specs::get(""));
  } else {
    for (const auto& path : effective_projections) {
      specs.push_back(couchbase::lookup_in_specs::get(path));
    }
  }
  encoded.body().specs(specs.specs());
  return {};
}

auto
get_projected_request::make_response(key_value_error_context&& ctx,
                                     const encoded_response_type& encoded) const
  -> get_projected_response
{
  get_projected_response response{ std::move(ctx) };
  if (!response.ctx.ec()) {
    const std::size_t min_fields =
      (with_expiry ? 2U : 1U) + (effective_projections.empty() ? 1U : projections.size());
    if (encoded.body().fields().size() < min_fields) {
      response.ctx.override_ec(errc::common::parsing_failure);
      return response;
    }
    response.cas = encoded.cas();
    if (!parse_u32_field(encoded.body().fields()[0].value, response.flags)) {
      response.ctx.override_ec(errc::common::parsing_failure);
      return response;
    }
    if (with_expiry && !encoded.body().fields()[1].value.empty()) {
      std::uint32_t expiry = 0;
      if (!parse_u32_field(encoded.body().fields()[1].value, expiry)) {
        response.ctx.override_ec(errc::common::parsing_failure);
        return response;
      }
      response.expiry = expiry;
    }
    if (effective_projections.empty()) {
      // from full document
      const auto& full_field = encoded.body().fields()[with_expiry ? 2 : 1];
      if (projections.empty()) {
        const bool empty_body_status =
          full_field.status == key_value_status_code::subdoc_path_not_found &&
          full_field.value.empty();
        if (full_field.status != key_value_status_code::success && !empty_body_status) {
          response.ctx.override_ec(
            protocol::map_status_code(protocol::client_opcode::subdoc_multi_lookup,
                                      static_cast<std::uint16_t>(full_field.status)));
          return response;
        }
        // special case when user only wanted full+expiration
        const auto& full_body = full_field.value;
        response.value.resize(full_body.size());
        std::transform(full_body.begin(), full_body.end(), response.value.begin(), [](auto ch) {
          return static_cast<std::byte>(ch);
        });
      } else {
        if (full_field.status != key_value_status_code::success) {
          if (full_field.status == key_value_status_code::subdoc_path_not_found) {
            response.ctx.override_ec(errc::key_value::document_not_json);
          } else {
            response.ctx.override_ec(
              protocol::map_status_code(protocol::client_opcode::subdoc_multi_lookup,
                                        static_cast<std::uint16_t>(full_field.status)));
          }
          return response;
        }
        tao::json::value full_doc{};
        try {
          full_doc = utils::json::parse(full_field.value);
        } catch (const tao::pegtl::parse_error&) {
          response.ctx.override_ec(errc::key_value::document_not_json);
          return response;
        }
        tao::json::value new_doc = initial_projected_document(projections);
        array_slot_map array_slots{};
        for (const auto& projection : projections) {
          const auto lookup = subdoc_lookup(full_doc, projection);
          switch (lookup.status) {
            case lookup_status::found:
              subdoc_apply_projection(new_doc,
                                      lookup.projection.steps,
                                      lookup.projection.value,
                                      preserve_array_indexes,
                                      array_slots);
              break;
            case lookup_status::not_found:
              // Ignore paths that were not found, similar to how we ignore them in the subdoc
              // multi-lookup below.
              break;
            case lookup_status::path_mismatch:
              response.ctx.override_ec(errc::key_value::path_mismatch);
              return response;
            case lookup_status::path_invalid:
              response.ctx.override_ec(errc::key_value::path_invalid);
              return response;
            case lookup_status::path_too_big:
              response.ctx.override_ec(errc::key_value::path_too_big);
              return response;
            case lookup_status::invalid_argument:
              response.ctx.override_ec(errc::common::invalid_argument);
              return response;
          }
        }
        if (preserve_array_indexes) {
          finalize_projected_arrays(new_doc);
        }
        response.value = utils::json::generate_binary(new_doc);
      }
    } else {
      tao::json::value new_doc = initial_projected_document(projections);
      array_slot_map array_slots{};
      std::size_t offset = with_expiry ? 2 : 1;
      for (const auto& projection : projections) {
        const auto& field = encoded.body().fields()[offset];
        ++offset;
        if (field.status == key_value_status_code::success && !field.value.empty()) {
          tao::json::value value_to_apply{};
          try {
            value_to_apply = utils::json::parse(field.value);
          } catch (const tao::pegtl::parse_error&) {
            response.ctx.override_ec(errc::common::parsing_failure);
            return response;
          }
          subdoc_apply_projection(
            new_doc, projection, value_to_apply, preserve_array_indexes, array_slots);
        } else if (field.status != key_value_status_code::subdoc_path_not_found) {
          response.ctx.override_ec(
            protocol::map_status_code(protocol::client_opcode::subdoc_multi_lookup,
                                      static_cast<std::uint16_t>(field.status)));
          return response;
        }
      }
      if (preserve_array_indexes) {
        finalize_projected_arrays(new_doc);
      }
      response.value = utils::json::generate_binary(new_doc);
    }
  }
  return response;
}
} // namespace couchbase::core::operations
