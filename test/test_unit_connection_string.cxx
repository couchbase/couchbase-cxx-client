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

#include "test_helper.hxx"

#include "core/utils/connection_string.hxx"

TEST_CASE("unit: connection string", "[unit]")
{
    SECTION("full example")
    {
        auto spec =
          couchbase::core::utils::parse_connection_string("couchbase://localhost:8091=http;127.0.0.1=mcd/default?enable_tracing=false");
        CHECK(spec.scheme == "couchbase");
        CHECK(spec.default_port == 11210);
        CHECK(spec.default_mode == couchbase::core::utils::connection_string::bootstrap_mode::gcccp);
        CHECK(spec.tls == false);
        CHECK(spec.params == std::map<std::string, std::string>{
                               { "enable_tracing", "false" },
                             });
        CHECK(spec.bootstrap_nodes == std::vector<couchbase::core::utils::connection_string::node>{
                                        { "localhost",
                                          8091,
                                          couchbase::core::utils::connection_string::address_type::dns,
                                          couchbase::core::utils::connection_string::bootstrap_mode::http },
                                        { "127.0.0.1",
                                          0,
                                          couchbase::core::utils::connection_string::address_type::ipv4,
                                          couchbase::core::utils::connection_string::bootstrap_mode::gcccp },
                                      });
        CHECK(spec.options.enable_tracing == false);
        CHECK(spec.default_bucket_name == "default");
    }

    SECTION("scheme")
    {
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1").scheme == "couchbase");
        CHECK(couchbase::core::utils::parse_connection_string("http://127.0.0.1").scheme == "http");
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://").scheme == "couchbase");
        CHECK(couchbase::core::utils::parse_connection_string("my+scheme://").scheme == "my+scheme");
        CHECK(couchbase::core::utils::parse_connection_string("127.0.0.1").scheme == "couchbase");
        CHECK(couchbase::core::utils::parse_connection_string("127.0.0.1:8091").scheme == "couchbase");

        SECTION("default bootstrap mode")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://").default_mode ==
                  couchbase::core::utils::connection_string::bootstrap_mode::gcccp);
            CHECK(couchbase::core::utils::parse_connection_string("https://").default_mode ==
                  couchbase::core::utils::connection_string::bootstrap_mode::http);
            CHECK(couchbase::core::utils::parse_connection_string("my+scheme://").default_mode ==
                  couchbase::core::utils::connection_string::bootstrap_mode::unspecified);
        }

        SECTION("default port")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://").default_port == 11210);
            CHECK(couchbase::core::utils::parse_connection_string("couchbases://").default_port == 11207);
            CHECK(couchbase::core::utils::parse_connection_string("http://").default_port == 8091);
            CHECK(couchbase::core::utils::parse_connection_string("https://").default_port == 18091);
            CHECK(couchbase::core::utils::parse_connection_string("my+scheme://").default_port == 0);
        }

        SECTION("tls")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://").tls == false);
            CHECK(couchbase::core::utils::parse_connection_string("http://").tls == false);
            CHECK(couchbase::core::utils::parse_connection_string("couchbases://").tls == true);
            CHECK(couchbase::core::utils::parse_connection_string("https://").tls == true);
        }
    }

    SECTION("bootstrap nodes")
    {
        SECTION("single node")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://231.1.1.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "231.1.1.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://255.1.1.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "255.1.1.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://256.1.1.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "256.1.1.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[::ffff:13.15.49.232]").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "::ffff:13.15.49.232",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[::]").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "::",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[::1]").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "::1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:db8::1]").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "2001:db8::1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:db8:85a3:8d3:1319:8a2e:370:7348]").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "2001:db8:85a3:8d3:1319:8a2e:370:7348",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://example.com").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "example.com",
                      0,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("1.2.3.4").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
        }

        SECTION("multiple nodes")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4,4.3.2.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4;4.3.2.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:db8::1];123.123.12.4").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "2001:db8::1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "123.123.12.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://example.com,[::1];127.0.0.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "example.com",
                      0,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "::1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "127.0.0.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("1.2.3.4,4.3.2.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
        }

        SECTION("custom ports")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4,4.3.2.1:11210").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      11210,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4:8091,4.3.2.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      8091,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:db8::1]:18091;123.123.12.4").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "2001:db8::1",
                      18091,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "123.123.12.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://example.com:123,[::1]:456;127.0.0.1:789").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "example.com",
                      123,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "::1",
                      456,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "127.0.0.1",
                      789,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("example.com:123,[::1]:456;127.0.0.1:789").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "example.com",
                      123,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "::1",
                      456,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "127.0.0.1",
                      789,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
        }

        SECTION("custom bootstrap mode")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4,4.3.2.1=MCD").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::gcccp },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4:8091=http;4.3.2.1").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      8091,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::http },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://1.2.3.4:8091=http;4.3.2.1=gcccp").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "1.2.3.4",
                      8091,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::http },
                    { "4.3.2.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::gcccp },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:db8::1]:18091=mcd;123.123.12.4").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "2001:db8::1",
                      18091,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::gcccp },
                    { "123.123.12.4",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                  });
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://example.com=CcCp,[::1];127.0.0.1=Http").bootstrap_nodes ==
                  std::vector<couchbase::core::utils::connection_string::node>{
                    { "example.com",
                      0,
                      couchbase::core::utils::connection_string::address_type::dns,
                      couchbase::core::utils::connection_string::bootstrap_mode::gcccp },
                    { "::1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv6,
                      couchbase::core::utils::connection_string::bootstrap_mode::unspecified },
                    { "127.0.0.1",
                      0,
                      couchbase::core::utils::connection_string::address_type::ipv4,
                      couchbase::core::utils::connection_string::bootstrap_mode::http },
                  });
        }

        SECTION("default bucket name")
        {
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1/bucket").default_bucket_name == "bucket");
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1/bUcKeT").default_bucket_name == "bUcKeT");
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1/bU%1F-K__big__.mp3").default_bucket_name ==
                  "bU%1F-K__big__.mp3");
            CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1").default_bucket_name.has_value() == false);
        }
    }

    SECTION("options")
    {
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1").options.trust_certificate.empty());
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?trust_certificate=/etc/tls/example.cert")
                .options.trust_certificate == "/etc/tls/example.cert");
        auto spec = couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?key_value_timeout=42&query_timeout=123");
        CHECK(spec.options.key_value_timeout == std::chrono::milliseconds(42));
        CHECK(spec.options.query_timeout == std::chrono::milliseconds(123));

        SECTION("parameters")
        {
            CHECK(spec.params == std::map<std::string, std::string>{
                                   { "key_value_timeout", "42" },
                                   { "query_timeout", "123" },
                                 });

            spec = couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?kv_timeout=42&foo=bar");
            CHECK(spec.params == std::map<std::string, std::string>{
                                   { "kv_timeout", "42" },
                                   { "foo", "bar" },
                                 });
            CHECK(spec.options.key_value_timeout == std::chrono::milliseconds(42));

            spec = couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?kv_timeout=4s2ms");
            CHECK(spec.params == std::map<std::string, std::string>{
                                   { "kv_timeout", "4s2ms" },
                                 });
            CHECK(spec.options.key_value_timeout == std::chrono::milliseconds(4002));

            spec = couchbase::core::utils::parse_connection_string(
              "couchbase://127.0.0.1?user_agent_extra=couchnode%2F4.1.1%20(node%2F12.11.1%3B%20v8%2F7.7.299.11-node.12%3B%20ssl%2F1.1.1c)");
            CHECK(spec.options.user_agent_extra == "couchnode/4.1.1 (node/12.11.1; v8/7.7.299.11-node.12; ssl/1.1.1c)");
        }
    }

    SECTION("parsing errors")
    {
        CHECK(couchbase::core::utils::parse_connection_string("").error == "failed to parse connection string: empty input");
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1/bucket/foo").error ==
              R"(failed to parse connection string (column: 29, trailer: "/foo"))");
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://[:13.15.49.232]").error ==
              R"(failed to parse connection string (column: 14, trailer: ":13.15.49.232]"))");
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://[2001:1:db8:85a3:8d3:1319:8a2e:370:7348]").error ==
              R"(failed to parse connection string (column: 47, trailer: ":7348]"))");
        CHECK(couchbase::core::utils::parse_connection_string("couchbase://2001:db8:85a3:8d3:1319:8a2e:370:7348").error.value() ==
              R"(failed to parse connection string (column: 18, trailer: "db8:85a3:8d3:1319:8a2e:370:7348"))");
    }

    SECTION("parsing warnings")
    {
        auto spec = couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?kv_timeout=42&foo=bar");
        CHECK(spec.warnings == std::vector<std::string>{
                                 R"(unknown parameter "foo" in connection string (value "bar"))",
                               });

        spec = couchbase::core::utils::parse_connection_string("couchbase://127.0.0.1?enable_dns_srv=maybe&ip_protocol=yes");
        CHECK(spec.warnings ==
              std::vector<std::string>{
                R"(unable to parse "enable_dns_srv" parameter in connection string (value "maybe" cannot be interpreted as a boolean))",
                R"(unable to parse "ip_protocol" parameter in connection string (value "yes" is not a valid IP protocol preference))",
              });

        spec = couchbase::core::utils::parse_connection_string("couchbase://localhost:8091=http;127.0.0.1=mcd/default?enable_dns_srv=true");
        CHECK(
          spec.warnings ==
          std::vector<std::string>{
            R"(parameter "enable_dns_srv" requires single entry in bootstrap nodes list of the connection string, ignoring (value "true"))",
          });

        spec = couchbase::core::utils::parse_connection_string(
          "couchbase://localhost?query_timeout=10000&kv_timeout=true&management_timeout=11000");
        std::string warning_prefix = R"(unable to parse "kv_timeout" parameter in connection string (value "true" is not a number))";
        CHECK(spec.warnings.at(0).substr(0, warning_prefix.size()) == warning_prefix);
        CHECK(spec.options.query_timeout == std::chrono::milliseconds(10000));
        CHECK(spec.options.management_timeout == std::chrono::milliseconds(11000));
    }
}
