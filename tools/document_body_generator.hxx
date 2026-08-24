/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include "core/utils/binary.hxx"
#include "core/utils/json.hxx"

#include <tao/json.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace cbc
{
enum class body_fill : std::uint8_t {
  constant, // every document repeats one filler character
  random,   // a seeded Mersenne Twister
};

enum class body_format : std::uint8_t {
  json,   // a JSON object, so the body is restricted to a JSON-safe alphabet
  binary, // opaque bytes of exactly the requested size
};

inline auto
parse_body_fill(const std::string& name) -> body_fill
{
  if (name == "random") {
    return body_fill::random;
  }
  return body_fill::constant;
}

inline auto
parse_body_format(const std::string& name) -> body_format
{
  if (name == "binary") {
    return body_format::binary;
  }
  return body_format::json;
}

/**
 * Source of document bodies for one worker thread.
 *
 * A random fill has to give every document distinct bytes. The storage engine
 * compresses a whole data block at once, so a body reused across documents is
 * stored once and back-referenced for the rest of the block: a bucket loaded
 * that way reports several times the compression its documents earn
 * individually, and its disk footprint stops following from the number of
 * documents written.
 */
class document_body_generator
{
public:
  /**
   * @param predefined used for every document when @p document_size is zero
   */
  document_body_generator(body_fill fill,
                          body_format format,
                          std::size_t document_size,
                          std::size_t pool_size,
                          std::uint64_t seed,
                          std::vector<std::byte> predefined = {})
    : fill_{ fill }
    , format_{ format }
    , document_size_{ document_size }
    , generator_{ seed }
  {
    if (document_size_ == 0) {
      pool_.push_back(std::move(predefined));
      return;
    }
    if (fill_ == body_fill::constant) {
      pool_.push_back(render(std::string(document_size_, constant_filler)));
      return;
    }
    for (std::size_t generated = 0; generated + document_size_ <= pool_size;
         generated += document_size_) {
      pool_.push_back(generate());
    }
  }

  /**
   * The next body. It stays valid until the following call.
   */
  [[nodiscard]] auto next() -> const std::vector<std::byte>&
  {
    if (pool_.empty()) {
      current_ = generate();
      return current_;
    }
    const auto& body = pool_[cursor_];
    cursor_ = (cursor_ + 1) % pool_.size();
    return body;
  }

  /**
   * How many distinct bodies this generator cycles through, or zero when it
   * generates a fresh one for every document.
   */
  [[nodiscard]] auto pooled_documents() const -> std::size_t
  {
    return pool_.size();
  }

  [[nodiscard]] auto binary() const -> bool
  {
    return format_ == body_format::binary;
  }

private:
  static constexpr char constant_filler{ 'x' };
  static constexpr std::size_t entropy_buffer_size{ 64 * 1024 };
  static constexpr std::string_view alphabet{
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  };

  [[nodiscard]] auto generate() -> std::vector<std::byte>
  {
    if (format_ == body_format::binary) {
      std::vector<std::byte> body(document_size_);
      fill(body.data(), body.size());
      return body;
    }
    return render(random_text());
  }

  [[nodiscard]] auto render(std::string text) const -> std::vector<std::byte>
  {
    if (format_ == body_format::binary) {
      return couchbase::core::utils::to_binary(text);
    }
    return couchbase::core::utils::json::generate_binary({
      { "size", document_size_ },
      { "text", std::move(text) },
    });
  }

  [[nodiscard]] auto random_text() -> std::string
  {
    // The largest multiple of the alphabet size that fits in a byte. Folding the
    // whole byte range onto the alphabet would over-represent its first eight
    // symbols.
    static constexpr std::uint8_t unbiased_limit{ 248 };
    static_assert(unbiased_limit % alphabet.size() == 0);

    std::string text(document_size_, constant_filler);
    for (auto& symbol : text) {
      std::uint8_t byte{ 0 };
      do {
        byte = next_byte();
      } while (byte >= unbiased_limit);
      symbol = alphabet[byte % alphabet.size()];
    }
    return text;
  }

  [[nodiscard]] auto next_byte() -> std::uint8_t
  {
    if (entropy_cursor_ == entropy_.size()) {
      refill();
    }
    return static_cast<std::uint8_t>(entropy_[entropy_cursor_++]);
  }

  /**
   * Copy @p size random bytes out of the buffer, refilling it as it runs dry, so
   * that the generator is stepped in bulk rather than once per byte consumed.
   */
  void fill(std::byte* out, std::size_t size)
  {
    while (size > 0) {
      if (entropy_cursor_ == entropy_.size()) {
        refill();
      }
      const auto chunk = std::min(size, entropy_.size() - entropy_cursor_);
      std::memcpy(out, entropy_.data() + entropy_cursor_, chunk);
      entropy_cursor_ += chunk;
      out += chunk;
      size -= chunk;
    }
  }

  void refill()
  {
    std::size_t offset{ 0 };
    for (; offset + sizeof(std::uint64_t) <= entropy_.size(); offset += sizeof(std::uint64_t)) {
      const auto word = generator_();
      std::memcpy(entropy_.data() + offset, &word, sizeof(word));
    }
    if (offset < entropy_.size()) {
      const auto word = generator_();
      std::memcpy(entropy_.data() + offset, &word, entropy_.size() - offset);
    }
    entropy_cursor_ = 0;
  }

  body_fill fill_;
  body_format format_;
  std::size_t document_size_;
  std::mt19937_64 generator_;

  std::vector<std::vector<std::byte>> pool_{};
  std::size_t cursor_{ 0 };
  std::vector<std::byte> current_{};

  std::vector<std::byte> entropy_ = std::vector<std::byte>(entropy_buffer_size);
  std::size_t entropy_cursor_{ entropy_buffer_size };
};
} // namespace cbc
