/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <array>
#include <gsl/util>
#include <optional>
#include <type_traits>
#include <stdexcept>

namespace couchbase::utils
{

/**
 * Helper code for encode and decode of LEB128 values.
 * - mcbp encodes collection-ID as an unsigned LEB128
 * - see https://en.wikipedia.org/wiki/LEB128
 */

struct Leb128NoThrow {
};

/**
 * decode_unsigned_leb128 returns the decoded T and a const_byte_buffer
 * initialised with the data following the leb128 data. This form of the decode
 * does not throw for invalid input and the caller should always check
 * second.data() for success or error (see returns info).
 *
 * @param buf buffer containing a leb128 encoded value (of size T)
 * @returns On error a std::pair where first is set to 0 and second is nullptr/0
 *          const_byte_buffer. On success a std::pair where first is the decoded
 *          value and second is a buffer initialised with the data following the
 *          leb128 data.
 */
template<class T>
typename std::enable_if_t<std::is_unsigned_v<T>, std::pair<T, std::string_view>>
decode_unsigned_leb128(std::string_view buf, struct Leb128NoThrow /* unused */)
{
    T rv = static_cast<uint8_t>(buf[0]) & 0x7fULL;
    size_t end = 0;
    if ((static_cast<uint8_t>(buf[0]) & 0x80ULL) == 0x80ULL) {
        T shift = 7;
        // shift in the remaining data
        for (end = 1; end < buf.size(); end++) {
            rv |= (static_cast<uint8_t>(buf[end]) & 0x7fULL) << shift;
            if ((static_cast<uint8_t>(buf[end]) & 0x80ULL) == 0) {
                break; // no more
            }
            shift += 7;
        }

        // We should of stopped for a stop byte, not the end of the buffer
        if (end == buf.size()) {
            return { 0, std::string_view{} };
        }
    }
    // Return the decoded value and a buffer for any remaining data
    return { rv, std::string_view{ buf.data() + end + 1, buf.size() - (end + 1) } };
}

/**
 * decode_unsigned_leb128 returns the decoded T and a const_byte_buffer
 * initialised with the data following the leb128 data. This form of the decode
 * throws for invalid input.
 *
 * @param buf buffer containing a leb128 encoded value (of size T)
 * @returns std::pair first is the decoded value and second a buffer for the
 *          remaining data (size will be 0 for no more data)
 * @throws std::invalid_argument if buf[0] does not encode a leb128 value with
 *         a stop byte.
 */
template<class T>
typename std::enable_if_t<std::is_unsigned_v<T>, std::pair<T, std::string_view>>
decode_unsigned_leb128(std::string_view buf)
{
    if (buf.size() > 0) {
        auto rv = decode_unsigned_leb128<T>(buf, Leb128NoThrow());
        if (rv.second.data()) {
            return rv;
        }
    }
    throw std::invalid_argument("decode_unsigned_leb128: invalid buf size:" + std::to_string(buf.size()));
}

/**
 * @return a buffer to the data after the leb128 prefix
 */
template<class T>
typename std::enable_if_t<std::is_unsigned_v<T>, std::string_view>
skip_unsigned_leb128(std::string_view buf)
{
    return decode_unsigned_leb128<T>(buf).second;
}

// Empty, non specialised version of the decoder class
template<class T, class Enable = void>
class unsigned_leb128
{
};

/**
 * For encoding a unsigned T leb128, class constructs from a T value and
 * provides a const_byte_buffer for access to the encoded
 */
template<class T>
class unsigned_leb128<T, typename std::enable_if_t<std::is_unsigned_v<T>>>
{
  public:
    explicit unsigned_leb128(T in)
    {
        while (in > 0) {
            auto byte = gsl::narrow_cast<uint8_t>(in & 0x7fULL);
            in >>= 7;

            // In has more data?
            if (in > 0) {
                byte |= 0x80;
                encodedData[encodedSize - 1U] = byte;
                // Increase the size
                encodedSize++;
            } else {
                encodedData[encodedSize - 1U] = byte;
            }
        }
    }

    [[nodiscard]] std::string get() const
    {
        return { begin(), end() };
    }

    [[nodiscard]] const uint8_t* begin() const
    {
        return encodedData.data();
    }

    [[nodiscard]] const uint8_t* end() const
    {
        return encodedData.data() + encodedSize;
    }

    [[nodiscard]] const uint8_t* data() const
    {
        return encodedData.data();
    }

    [[nodiscard]] size_t size() const
    {
        return encodedSize;
    }

    constexpr static size_t getMaxSize()
    {
        return maxSize;
    }

  private:
    // Larger T may need a larger array
    static_assert(sizeof(T) <= 8, "Class is only valid for uint 8/16/64");

    // value is large enough to store ~0 as leb128
    static constexpr size_t maxSize = sizeof(T) + (((sizeof(T) + 1) / 8) + 1);
    std::array<uint8_t, maxSize> encodedData{};
    uint8_t encodedSize{ 1 };
};

} // namespace couchbase::utils
