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

#include "configuration.hxx"

#include <tao/json/forward.hpp>

#include <charconv>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace tao::json
{
template<>
struct traits<couchbase::core::topology::configuration> {
  static auto parse_port_component(std::string_view port_str) -> std::uint16_t
  {
    if (port_str.empty()) {
      throw std::invalid_argument("empty port in cluster configuration");
    }
    std::uint32_t port = 0;
    const auto [ptr, ec] =
      std::from_chars(port_str.data(), port_str.data() + port_str.size(), port);
    if (ec != std::errc{} || ptr != port_str.data() + port_str.size() || port == 0 ||
        port > std::numeric_limits<std::uint16_t>::max()) {
      throw std::invalid_argument("invalid port in cluster configuration");
    }
    return static_cast<std::uint16_t>(port);
  }

  template<template<typename...> class Traits>
  static auto optional_port(const tao::json::basic_value<Traits>& v, const std::string& key)
    -> std::optional<std::uint16_t>
  {
    const auto* field = v.find(key);
    if (field == nullptr || field->is_null()) {
      return std::nullopt;
    }
    if (!field->is_integer()) {
      throw std::invalid_argument("non-integer port in cluster configuration");
    }
    const auto port = field->template as<std::int64_t>();
    if (port <= 0 || port > std::numeric_limits<std::uint16_t>::max()) {
      throw std::invalid_argument("invalid port in cluster configuration");
    }
    return static_cast<std::uint16_t>(port);
  }

  template<template<typename...> class Traits>
  static void read_service_ports(const tao::json::basic_value<Traits>& s,
                                 couchbase::core::topology::configuration::port_map& services_plain,
                                 couchbase::core::topology::configuration::port_map& services_tls)
  {
    static_cast<void>(s.get_object());
    if (const auto p = optional_port(s, "kv")) {
      services_plain.key_value = p;
    }
    if (const auto p = optional_port(s, "mgmt")) {
      services_plain.management = p;
    }
    if (const auto p = optional_port(s, "fts")) {
      services_plain.search = p;
    }
    if (const auto p = optional_port(s, "cbas")) {
      services_plain.analytics = p;
    }
    if (const auto p = optional_port(s, "n1ql")) {
      services_plain.query = p;
    }
    if (const auto p = optional_port(s, "capi")) {
      services_plain.views = p;
    }
    if (const auto p = optional_port(s, "eventingAdminPort")) {
      services_plain.eventing = p;
    }
    if (const auto p = optional_port(s, "kvSSL")) {
      services_tls.key_value = p;
    }
    if (const auto p = optional_port(s, "mgmtSSL")) {
      services_tls.management = p;
    }
    if (const auto p = optional_port(s, "ftsSSL")) {
      services_tls.search = p;
    }
    if (const auto p = optional_port(s, "cbasSSL")) {
      services_tls.analytics = p;
    }
    if (const auto p = optional_port(s, "n1qlSSL")) {
      services_tls.query = p;
    }
    if (const auto p = optional_port(s, "capiSSL")) {
      services_tls.views = p;
    }
    if (const auto p = optional_port(s, "eventingSSL")) {
      services_tls.eventing = p;
    }
  }

  template<template<typename...> class Traits>
  static auto as(const tao::json::basic_value<Traits>& v)
    -> couchbase::core::topology::configuration
  {
    static_cast<void>(v.get_object());
    couchbase::core::topology::configuration result;
    result.id = couchbase::core::uuid::random();
    result.epoch = v.template optional<std::int64_t>("revEpoch");
    result.rev = v.template optional<std::int64_t>("rev");

    if (auto* node_locator = v.find("nodeLocator");
        node_locator != nullptr && node_locator->is_string()) {
      if (node_locator->get_string() == "ketama") {
        result.node_locator = couchbase::core::topology::configuration::node_locator_type::ketama;
      } else {
        result.node_locator = couchbase::core::topology::configuration::node_locator_type::vbucket;
      }
    }

    if (auto* nodes_ext = v.find("nodesExt"); nodes_ext != nullptr && !nodes_ext->is_null()) {
      size_t index = 0;
      for (const auto& j : nodes_ext->get_array()) {
        couchbase::core::topology::configuration::node n;
        n.index = index++;
        const auto& o = j.get_object();
        if (const auto& this_node = o.find("thisNode"); this_node != o.end() &&
                                                        !this_node->second.is_null() &&
                                                        this_node->second.get_boolean()) {
          n.this_node = true;
        }
        if (const auto& hostname = o.find("hostname");
            hostname != o.end() && !hostname->second.is_null()) {
          n.hostname = hostname->second.get_string();
        }
        if (const auto& group = o.find("serverGroup");
            group != o.end() && !group->second.is_null()) {
          n.server_group = group->second.get_string();
        }
        if (const auto& path = o.find("appTelemetryPath");
            path != o.end() && !path->second.is_null()) {
          n.app_telemetry_path = path->second.get_string();
        }
        if (const auto& uuid = o.find("nodeUUID"); uuid != o.end() && !uuid->second.is_null()) {
          n.node_uuid = uuid->second.get_string();
        }
        if (const auto s_it = o.find("services"); s_it != o.end() && !s_it->second.is_null()) {
          read_service_ports(s_it->second, n.services_plain, n.services_tls);
        }
        {
          const auto& alt = o.find("alternateAddresses");
          if (alt != o.end() && !alt->second.is_null()) {
            for (const auto& entry : alt->second.get_object()) {
              couchbase::core::topology::configuration::alternate_address addr;
              addr.name = entry.first;
              addr.hostname = entry.second.at("hostname").get_string();
              addr.services_plain = n.services_plain;
              addr.services_tls = n.services_tls;
              if (const auto* ports = entry.second.find("ports");
                  ports != nullptr && !ports->is_null()) {
                read_service_ports(*ports, addr.services_plain, addr.services_tls);
              }
              n.alt.emplace(entry.first, addr);
            }
          }
        }
        result.nodes.emplace_back(n);
      }
    } else {
      if (result.node_locator ==
          couchbase::core::topology::configuration::node_locator_type::vbucket) {
        const auto* m = v.find("vBucketServerMap");
        const auto& nodes = v.at("nodes").get_array();
        if (m != nullptr && !m->is_null()) {
          if (const auto* s = m->find("serverList"); s != nullptr && s->is_array()) {
            std::size_t index = 0;
            for (const auto& j : s->get_array()) {
              couchbase::core::topology::configuration::node n;
              n.index = index++;
              const auto& address = j.get_string();
              const auto addr_colon = address.rfind(':');
              if (addr_colon == std::string::npos) {
                throw std::invalid_argument("missing port in serverList entry");
              }
              n.hostname = address.substr(0, addr_colon);
              n.services_plain.key_value =
                parse_port_component(std::string_view{ address }.substr(addr_colon + 1));
              if (n.index >= nodes.size()) {
                continue;
              }
              if (const auto& np = nodes[n.index]; np.is_object()) {
                const auto& o = np.get_object();

                if (const auto& this_node = o.find("thisNode"); this_node != o.end() &&
                                                                !this_node->second.is_null() &&
                                                                this_node->second.get_boolean()) {
                  n.this_node = true;
                }
                if (const auto p_it = o.find("ports"); p_it != o.end() && !p_it->second.is_null()) {
                  const auto& p = p_it->second;
                  static_cast<void>(p.get_object());
                  if (auto https_views = p.template optional<std::int64_t>("httpsCAPI");
                      https_views && https_views.value() > 0 &&
                      https_views.value() <= std::numeric_limits<std::uint16_t>::max()) {
                    n.services_tls.views = static_cast<std::uint16_t>(https_views.value());
                  }
                  if (auto https_mgmt = p.template optional<std::int64_t>("httpsMgmt");
                      https_mgmt && https_mgmt.value() > 0 &&
                      https_mgmt.value() <= std::numeric_limits<std::uint16_t>::max()) {
                    n.services_tls.management = static_cast<std::uint16_t>(https_mgmt.value());
                  }
                }
                if (const auto h_it = o.find("hostname");
                    h_it != o.end() && !h_it->second.is_null()) {
                  const auto& h = h_it->second.get_string();
                  if (const auto h_colon = h.rfind(':'); h_colon != std::string::npos) {
                    n.services_plain.management =
                      parse_port_component(std::string_view{ h }.substr(h_colon + 1));
                  }
                }
                if (const auto capi_it = o.find("couchApiBase");
                    capi_it != o.end() && !capi_it->second.is_null()) {
                  const auto& capi = capi_it->second.get_string();
                  const auto slash = capi.rfind('/');
                  const auto colon = capi.rfind(':', slash);
                  if (slash != std::string::npos && colon != std::string::npos &&
                      colon + 1 < slash) {
                    n.services_plain.views = parse_port_component(
                      std::string_view{ capi }.substr(colon + 1, slash - (colon + 1)));
                  }
                }
              }
              result.nodes.emplace_back(n);
            }
          }
        }
      } else {
        std::size_t index = 0;
        for (const auto& node : v.at("nodes").get_array()) {
          couchbase::core::topology::configuration::node n;
          n.index = index++;
          const auto& o = node.get_object();

          if (const auto& this_node = o.find("thisNode"); this_node != o.end() &&
                                                          !this_node->second.is_null() &&
                                                          this_node->second.get_boolean()) {
            n.this_node = true;
          }
          if (const auto p_it = o.find("ports"); p_it != o.end() && !p_it->second.is_null()) {
            const auto& p = p_it->second;
            static_cast<void>(p.get_object());
            if (auto direct = p.template optional<std::int64_t>("direct");
                direct && direct.value() > 0 &&
                direct.value() <= std::numeric_limits<std::uint16_t>::max()) {
              n.services_plain.key_value = static_cast<std::uint16_t>(direct.value());
            }
            if (auto https_views = p.template optional<std::int64_t>("httpsCAPI");
                https_views && https_views.value() > 0 &&
                https_views.value() <= std::numeric_limits<std::uint16_t>::max()) {
              n.services_tls.views = static_cast<std::uint16_t>(https_views.value());
            }
            if (auto https_mgmt = p.template optional<std::int64_t>("httpsMgmt");
                https_mgmt && https_mgmt.value() > 0 &&
                https_mgmt.value() <= std::numeric_limits<std::uint16_t>::max()) {
              n.services_tls.management = static_cast<std::uint16_t>(https_mgmt.value());
            }
          }
          const auto& h = o.at("hostname").get_string();
          const auto colon = h.rfind(':');
          if (colon == std::string::npos) {
            n.hostname = h;
          } else {
            n.hostname = h.substr(0, colon);
            n.services_plain.management =
              parse_port_component(std::string_view{ h }.substr(colon + 1));
          }

          if (const auto capi_it = o.find("couchApiBase");
              capi_it != o.end() && !capi_it->second.is_null()) {
            const auto& capi = capi_it->second.get_string();
            const auto slash = capi.rfind('/');
            const auto capi_colon = capi.rfind(':', slash);
            if (slash != std::string::npos && capi_colon != std::string::npos &&
                capi_colon + 1 < slash) {
              n.services_plain.views = parse_port_component(
                std::string_view{ capi }.substr(capi_colon + 1, slash - (capi_colon + 1)));
            }
          }

          result.nodes.emplace_back(n);
        }
      }
    }
    if (const auto m = v.find("uuid"); m != nullptr && !m->is_null()) {
      result.uuid = m->get_string();
    }
    if (const auto m = v.find("collectionsManifestUid"); m != nullptr && !m->is_null()) {
      const auto& uid_str = m->get_string();
      std::uint64_t uid = 0;
      const auto [ptr, ec] =
        std::from_chars(uid_str.data(), uid_str.data() + uid_str.size(), uid, 16);
      if (ec != std::errc{} || ptr != uid_str.data() + uid_str.size()) {
        throw std::invalid_argument("invalid collectionsManifestUid in cluster configuration");
      }
      result.collections_manifest_uid = uid;
    }
    if (const auto m = v.find("name"); m != nullptr && !m->is_null()) {
      result.bucket = m->get_string();
    }

    if (const auto m = v.find("vBucketServerMap"); m != nullptr && !m->is_null()) {
      const auto& o = m->get_object();
      if (const auto f = o.find("numReplicas"); f != o.end() && !f->second.is_null()) {
        result.num_replicas = f->second.template as<std::uint32_t>();
      }
      if (const auto f = o.find("vBucketMap"); f != o.end() && !f->second.is_null()) {
        const auto& vb = f->second.get_array();
        couchbase::core::topology::configuration::vbucket_map vbmap;
        vbmap.resize(vb.size());
        for (size_t i = 0; i < vb.size(); i++) {
          const auto& p = vb[i].get_array();
          vbmap[i].resize(p.size());
          for (size_t n = 0; n < p.size(); n++) {
            vbmap[i][n] = p[n].template as<std::int16_t>();
          }
        }
        result.vbmap = vbmap;
      }
    }
    if (const auto m = v.find("bucketCapabilities"); m != nullptr && m->is_array()) {
      for (const auto& entry : m->get_array()) {
        const auto& name = entry.get_string();
        if (name == "couchapi") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::couchapi);
        } else if (name == "collections") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::collections);
        } else if (name == "durableWrite") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::durable_write);
        } else if (name == "tombstonedUserXAttrs") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::tombstoned_user_xattrs);
        } else if (name == "dcp") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::dcp);
        } else if (name == "cbhello") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::cbhello);
        } else if (name == "touch") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::touch);
        } else if (name == "cccp") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::cccp);
        } else if (name == "xdcrCheckpointing") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::xdcr_checkpointing);
        } else if (name == "nodesExt") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::nodes_ext);
        } else if (name == "xattr") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::xattr);
        } else if (name == "rangeScan") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::range_scan);
        } else if (name == "subdoc.ReplicaRead") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::subdoc_replica_read);
        } else if (name == "subdoc.AccessDeleted") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::subdoc_access_deleted);
        } else if (name == "nonDedupedHistory") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::non_deduped_history);
        } else if (name == "subdoc.ReplaceBodyWithXattr") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::subdoc_replace_body_with_xattr);
        } else if (name == "subdoc.DocumentMacroSupport") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::subdoc_document_macro_support);
        } else if (name == "subdoc.ReviveDocument") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::subdoc_revive_document);
        } else if (name == "dcp.IgnorePurgedTombstones") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::dcp_ignore_purged_tombstones);
        } else if (name == "preserve_expiry") {
          result.capabilities.bucket.insert(couchbase::core::bucket_capability::preserve_expiry);
        } else if (name == "querySystemCollection") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::query_system_collection);
        } else if (name == "mobileSystemCollection") {
          result.capabilities.bucket.insert(
            couchbase::core::bucket_capability::mobile_system_collection);
        }
      }
    }
    if (const auto m = v.find("clusterCapabilities"); m != nullptr && m->is_object()) {
      if (const auto nc = m->find("n1ql"); nc != nullptr && nc->is_array()) {
        for (const auto& entry : nc->get_array()) {
          if (const auto& name = entry.get_string(); name == "costBasedOptimizer") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_cost_based_optimizer);
          } else if (name == "indexAdvisor") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_index_advisor);
          } else if (name == "javaScriptFunctions") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_javascript_functions);
          } else if (name == "inlineFunctions") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_inline_functions);
          } else if (name == "enhancedPreparedStatements") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_enhanced_prepared_statements);
          } else if (name == "readFromReplica") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::n1ql_read_from_replica);
          }
        }
      }
      if (const auto sc = m->find("search"); sc != nullptr && sc->is_array()) {
        for (const auto& entry : sc->get_array()) {
          if (const auto& name = entry.get_string(); name == "vectorSearch") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::search_vector_search);
          } else if (name == "scopedSearchIndex") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::search_scoped_search_index);
          } else if (name == "scoreFusion") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::search_score_fusion);
          } else if (name == "udfQuery") {
            result.capabilities.cluster.insert(
              couchbase::core::cluster_capability::search_udf_query);
          }
        }
      }
    }

    if (const auto n = v.find("clusterName"); n != nullptr && !n->is_null()) {
      result.cluster_name = n->get_string();
    }
    if (const auto u = v.find("clusterUUID"); u != nullptr && !u->is_null()) {
      result.cluster_uuid = u->get_string();
    }
    if (const auto n = v.find("prod"); n != nullptr && !n->is_null()) {
      result.prod = n->get_string();
      result.capabilities.prod = result.prod;
    }

    return result;
  }
};
} // namespace tao::json
