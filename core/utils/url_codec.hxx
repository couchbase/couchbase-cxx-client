#pragma once

#include <map>
#include <sstream>
#include <string>

namespace couchbase::core::utils::string_codec
{
std::string
url_decode(const std::string& src);

std::string
url_encode(const std::string& src);

std::string
form_encode(const std::string& src);

namespace v2
{
enum class encoding {
    encode_path,
    encode_path_segment,
    encode_host,
    encode_zone,
    encode_user_password,
    encode_query_component,
    encode_fragment,
};

std::string
escape(const std::string& s, encoding mode);

/**
 * Escapes the string so it can be safely placed inside a URL query.
 *
 * @param s
 * @return
 */
inline std::string
query_escape(const std::string& s)
{
    return escape(s, encoding::encode_query_component);
}

/**
 * Escapes the string so it can be safely placed inside a URL path segment, replacing special characters (including /) with %XX sequences as
 * needed.
 */
inline std::string
path_escape(const std::string& s)
{
    return escape(s, encoding::encode_path_segment);
}

inline std::string
form_encode(const std::map<std::string, std::string>& values)
{
    std::stringstream ss;
    bool first{ true };
    for (const auto& [key, value] : values) {
        if (first) {
            first = false;
        } else {
            ss << '&';
        }
        ss << query_escape(key) << '=' << query_escape(value);
    }
    return ss.str();
}
} // namespace v2

} // namespace couchbase::core::utils::string_codec
