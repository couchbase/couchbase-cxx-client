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

#include <cstddef>
#include <cstdint>

namespace couchbase::core::protocol
{
enum class datatype : std::uint8_t {
  raw = 0x00,
  json = 0x01,
  snappy = 0x02,
  xattr = 0x04,
};

/*
 * Free-standing bitwise operators for the datatype flag set.
 *
 * Combining two datatypes stays inside the enum domain and yields a datatype.
 * Combining a datatype with a raw protocol byte yields std::byte (in either
 * operand order), because the result may carry bits outside the named values
 * and therefore should not pretend to be a well-formed datatype.
 */

constexpr auto
operator|(datatype lhs, datatype rhs) noexcept -> datatype
{
  return static_cast<datatype>(static_cast<std::uint8_t>(lhs) | static_cast<std::uint8_t>(rhs));
}

constexpr auto
operator&(datatype lhs, datatype rhs) noexcept -> datatype
{
  return static_cast<datatype>(static_cast<std::uint8_t>(lhs) & static_cast<std::uint8_t>(rhs));
}

constexpr auto
operator^(datatype lhs, datatype rhs) noexcept -> datatype
{
  return static_cast<datatype>(static_cast<std::uint8_t>(lhs) ^ static_cast<std::uint8_t>(rhs));
}

constexpr auto
operator|(datatype lhs, std::byte rhs) noexcept -> std::byte
{
  return static_cast<std::byte>(lhs) | rhs;
}

constexpr auto
operator|(std::byte lhs, datatype rhs) noexcept -> std::byte
{
  return lhs | static_cast<std::byte>(rhs);
}

constexpr auto
operator&(datatype lhs, std::byte rhs) noexcept -> std::byte
{
  return static_cast<std::byte>(lhs) & rhs;
}

constexpr auto
operator&(std::byte lhs, datatype rhs) noexcept -> std::byte
{
  return lhs & static_cast<std::byte>(rhs);
}

constexpr auto
operator^(datatype lhs, std::byte rhs) noexcept -> std::byte
{
  return static_cast<std::byte>(lhs) ^ rhs;
}

constexpr auto
operator^(std::byte lhs, datatype rhs) noexcept -> std::byte
{
  return lhs ^ static_cast<std::byte>(rhs);
}

/*
 * Type-safe flag presence test. Returns true when every bit of flag is set in
 * value; using the composite flag itself as the right-hand side keeps the check
 * correct even when flag carries more than one bit.
 */

constexpr auto
has_flag(datatype value, datatype flag) noexcept -> bool
{
  const auto masked = value & flag;
  return masked == flag;
}

constexpr auto
has_flag(std::byte value, datatype flag) noexcept -> bool
{
  const auto masked = value & flag;
  return masked == static_cast<std::byte>(flag);
}

/*
 * In-place mutators that set or clear flag bits. The flag is always a named
 * datatype, while the value being modified may live in the typed enum domain or
 * as a raw protocol byte.
 */

constexpr void
set_flag(datatype& value, datatype flag) noexcept
{
  value = value | flag;
}

constexpr void
set_flag(std::byte& value, datatype flag) noexcept
{
  value = value | flag;
}

constexpr void
clear_flag(datatype& value, datatype flag) noexcept
{
  // Complement the flag in the integer domain: the bitwise negation of a single
  // flag is not itself a valid datatype, so it must never be cast back to the enum.
  value =
    static_cast<datatype>(static_cast<std::uint8_t>(value) & ~static_cast<std::uint8_t>(flag));
}

constexpr void
clear_flag(std::byte& value, datatype flag) noexcept
{
  value &= ~static_cast<std::byte>(flag);
}

constexpr auto
is_valid_datatype(std::uint8_t code) -> bool
{
  switch (static_cast<datatype>(code)) {
    case datatype::raw:
    case datatype::json:
    case datatype::snappy:
    case datatype::xattr:
      return true;
  }
  return false;
}

constexpr auto
has_json_datatype(std::uint8_t code) -> bool
{
  return has_flag(static_cast<std::byte>(code), datatype::json);
}
} // namespace couchbase::core::protocol
