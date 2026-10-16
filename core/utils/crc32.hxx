/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* The crc32 functions and data was originally written by Spencer
 * Garrett <srg@quick.com> and was gleaned from the PostgreSQL source
 * tree via the files contrib/ltree/crc32.[ch] and from FreeBSD at
 * src/usr.bin/cksum/crc32.c.
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace couchbase::core::utils
{
namespace detail
{
constexpr auto
generate_crc32_slice_tables() noexcept -> std::array<std::array<std::uint32_t, 256>, 8>
{
  std::array<std::array<std::uint32_t, 256>, 8> tables{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    std::uint32_t crc = i;
    for (int j = 0; j < 8; ++j) {
      crc = (crc >> 1U) ^ ((crc & 1U) != 0U ? 0xedb88320U : 0U);
    }
    tables[0][i] = crc;
  }
  for (std::size_t slice = 1; slice < 8; ++slice) {
    for (std::size_t i = 0; i < 256; ++i) {
      const std::uint32_t prev = tables[slice - 1][i];
      tables[slice][i] = (prev >> 8U) ^ tables[0][prev & 0xffU];
    }
  }
  return tables;
}

inline constexpr auto crc32_slice_tables = generate_crc32_slice_tables();
} // namespace detail

static inline auto
hash_crc32_software(const char* key, std::size_t key_length) noexcept -> std::uint32_t
{
  std::uint32_t crc = UINT32_MAX;
  const auto* ptr = reinterpret_cast<const std::uint8_t*>(key);

#if !defined(__BYTE_ORDER__) || (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
  while (key_length >= 8) {
    std::uint32_t w0 = 0;
    std::uint32_t w1 = 0;
    std::memcpy(&w0, ptr, sizeof(std::uint32_t));
    std::memcpy(&w1, ptr + sizeof(std::uint32_t), sizeof(std::uint32_t));
    crc ^= w0;
    crc = detail::crc32_slice_tables[7][(crc >> 0U) & 0xffU] ^
          detail::crc32_slice_tables[6][(crc >> 8U) & 0xffU] ^
          detail::crc32_slice_tables[5][(crc >> 16U) & 0xffU] ^
          detail::crc32_slice_tables[4][(crc >> 24U) & 0xffU] ^
          detail::crc32_slice_tables[3][(w1 >> 0U) & 0xffU] ^
          detail::crc32_slice_tables[2][(w1 >> 8U) & 0xffU] ^
          detail::crc32_slice_tables[1][(w1 >> 16U) & 0xffU] ^
          detail::crc32_slice_tables[0][(w1 >> 24U) & 0xffU];
    ptr += 8;
    key_length -= 8;
  }
#endif

  while (key_length > 0) {
    crc = (crc >> 8U) ^ detail::crc32_slice_tables[0][(crc ^ *ptr++) & 0xffU];
    --key_length;
  }

  return ((~crc) >> 16U) & 0x7fffU;
}

static inline auto
hash_crc32(const char* key, std::size_t key_length) noexcept -> std::uint32_t
{
#if defined(__ARM_FEATURE_CRC32) &&                                                                \
  (!defined(__BYTE_ORDER__) || (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__))
  std::uint32_t crc = UINT32_MAX;
  const auto* ptr = reinterpret_cast<const std::uint8_t*>(key);

  while (key_length >= 8) {
    std::uint64_t val = 0;
    std::memcpy(&val, ptr, sizeof(std::uint64_t));
    crc = __crc32d(crc, val);
    ptr += 8;
    key_length -= 8;
  }
  if (key_length >= 4) {
    std::uint32_t val = 0;
    std::memcpy(&val, ptr, sizeof(std::uint32_t));
    crc = __crc32w(crc, val);
    ptr += 4;
    key_length -= 4;
  }
  if (key_length >= 2) {
    std::uint16_t val = 0;
    std::memcpy(&val, ptr, sizeof(std::uint16_t));
    crc = __crc32h(crc, val);
    ptr += 2;
    key_length -= 2;
  }
  if (key_length >= 1) {
    crc = __crc32b(crc, *ptr);
  }

  return ((~crc) >> 16U) & 0x7fffU;
#else
  return hash_crc32_software(key, key_length);
#endif
}

static inline auto
hash_crc32(const std::byte* key, std::size_t key_length) noexcept -> std::uint32_t
{
  return hash_crc32(reinterpret_cast<const char*>(key), key_length);
}
} // namespace couchbase::core::utils
