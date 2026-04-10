/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

#include "key_value.hxx"

#include "common.hxx"

#include <core/utils/binary.hxx>

#include <spdlog/spdlog.h>

namespace fit_cxx::commands::key_value
{
couchbase::read_preference
to_read_preference(const protocol::shared::ReadPreference& preference)
{
  switch (preference) {
    case protocol::shared::NO_PREFERENCE:
      return couchbase::read_preference::no_preference;
    case protocol::shared::SELECTED_SERVER_GROUP:
      return couchbase::read_preference::selected_server_group;
    case protocol::shared::SELECTED_SERVER_GROUP_OR_ALL_AVAILABLE:
      return couchbase::read_preference::selected_server_group_or_all_available;
    default:
      throw performer_exception::unimplemented("Invalid read preference specified");
  }
}

couchbase::insert_options
to_insert_options(const protocol::sdk::kv::Insert& cmd, observability::span_owner* spans)
{
  couchbase::insert_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().has_expiry()) {
      if (cmd.options().expiry().has_relativesecs()) {
        opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
      } else if (cmd.options().expiry().has_absoluteepochsecs()) {
        auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
        opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
      } else {
        spdlog::error("unknown expiry type {}", cmd.options().expiry().DebugString());
      }
    }
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_options
to_get_options(const protocol::sdk::kv::Get& cmd, observability::span_owner* spans)
{
  couchbase::get_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }

  if (cmd.options().has_with_expiry()) {
    opts.with_expiry(cmd.options().with_expiry());
  }

  if (!cmd.options().projection().empty()) {
    std::vector<std::string> projection(cmd.options().projection().begin(),
                                        cmd.options().projection().end());
    opts.project(projection);
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::remove_options
to_remove_options(const protocol::sdk::kv::Remove& cmd, observability::span_owner* spans)
{
  couchbase::remove_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_cas()) {
    couchbase::cas cas(static_cast<std::uint64_t>(cmd.options().cas()));
    opts.cas(cas);
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::replace_options
to_replace_options(const protocol::sdk::kv::Replace& cmd, observability::span_owner* spans)
{
  couchbase::replace_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_cas()) {
    couchbase::cas cas(static_cast<std::uint64_t>(cmd.options().cas()));
    opts.cas(cas);
  }
  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().has_expiry()) {
      if (cmd.options().expiry().has_relativesecs()) {
        opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
      } else if (cmd.options().expiry().has_absoluteepochsecs()) {
        auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
        opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
      } else {
        spdlog::error("unknown expiry type {}", cmd.options().expiry().DebugString());
      }
    }
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
  if (cmd.options().has_preserve_expiry()) {
    opts.preserve_expiry(cmd.options().preserve_expiry());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::upsert_options
to_upsert_options(const protocol::sdk::kv::Upsert& cmd, observability::span_owner* spans)
{
  couchbase::upsert_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().has_expiry()) {
      if (cmd.options().expiry().has_relativesecs()) {
        opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
      } else if (cmd.options().expiry().has_absoluteepochsecs()) {
        auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
        opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
      } else {
        spdlog::error("unknown expiry type {}", cmd.options().expiry().DebugString());
      }
    }
  }
  if (cmd.options().has_preserve_expiry()) {
    opts.preserve_expiry(cmd.options().preserve_expiry());
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::lookup_in_options
to_lookup_in_options(const protocol::sdk::kv::lookup_in::LookupIn& cmd,
                     observability::span_owner* spans)
{
  couchbase::lookup_in_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_millis()) {
    opts.timeout(std::chrono::milliseconds{ cmd.options().timeout_millis() });
  }
  if (cmd.options().has_access_deleted()) {
    opts.access_deleted(cmd.options().access_deleted());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_and_lock_options
to_get_and_lock_options(const protocol::sdk::kv::GetAndLock& cmd, observability::span_owner* spans)
{
  couchbase::get_and_lock_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::unlock_options
to_unlock_options(const protocol::sdk::kv::Unlock& cmd, observability::span_owner* spans)
{
  couchbase::unlock_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_and_touch_options
to_get_and_touch_options(const protocol::sdk::kv::GetAndTouch& cmd,
                         observability::span_owner* spans)
{
  couchbase::get_and_touch_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::exists_options
to_exists_options(const protocol::sdk::kv::Exists& cmd, observability::span_owner* spans)
{
  couchbase::exists_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::touch_options
to_touch_options(const protocol::sdk::kv::Touch& cmd, observability::span_owner* spans)
{
  couchbase::touch_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::get_any_replica_options
to_get_any_replica_options(const protocol::sdk::kv::GetAnyReplica& cmd,
                           observability::span_owner* spans)
{
  couchbase::get_any_replica_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_read_preference()) {
    opts.read_preference(to_read_preference(cmd.options().read_preference()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::get_all_replicas_options
to_get_all_replicas_options(const protocol::sdk::kv::GetAllReplicas& cmd,
                            observability::span_owner* spans)
{
  couchbase::get_all_replicas_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_read_preference()) {
    opts.read_preference(to_read_preference(cmd.options().read_preference()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::increment_options
to_increment_options(const protocol::sdk::kv::Increment& cmd, observability::span_owner* spans)
{
  couchbase::increment_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().expiry().has_relativesecs()) {
      opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
    } else {
      auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
      opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
    }
  }
  if (cmd.options().has_delta()) {
    opts.delta(static_cast<std::uint64_t>(cmd.options().delta()));
  }
  if (cmd.options().has_initial()) {
    opts.initial(static_cast<std::uint64_t>(cmd.options().initial()));
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::decrement_options
to_decrement_options(const protocol::sdk::kv::Decrement& cmd, observability::span_owner* spans)
{
  couchbase::decrement_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().expiry().has_relativesecs()) {
      opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
    } else {
      auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
      opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
    }
  }
  if (cmd.options().has_delta()) {
    opts.delta(static_cast<std::uint64_t>(cmd.options().delta()));
  }
  if (cmd.options().has_initial()) {
    opts.initial(static_cast<std::uint64_t>(cmd.options().initial()));
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::append_options
to_append_options(const protocol::sdk::kv::Append& cmd, observability::span_owner* spans)
{
  couchbase::append_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_cas()) {
    opts.cas(couchbase::cas(static_cast<std::uint64_t>(cmd.options().cas())));
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::prepend_options
to_prepend_options(const protocol::sdk::kv::Prepend& cmd, observability::span_owner* spans)
{
  couchbase::prepend_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_cas()) {
    opts.cas(couchbase::cas(static_cast<std::uint64_t>(cmd.options().cas())));
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::mutate_in_options
to_mutate_in_options(const protocol::sdk::kv::mutate_in::MutateIn& cmd,
                     observability::span_owner* spans)
{
  couchbase::mutate_in_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_millis()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_millis()));
  }
  if (cmd.options().has_expiry()) {
    if (cmd.options().expiry().has_relativesecs()) {
      opts.expiry(std::chrono::seconds{ cmd.options().expiry().relativesecs() });
    } else {
      auto time_since_epoch = std::chrono::seconds{ cmd.options().expiry().absoluteepochsecs() };
      opts.expiry(std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch });
    }
  }
  if (cmd.options().has_cas()) {
    opts.cas(couchbase::cas(static_cast<std::uint64_t>(cmd.options().cas())));
  }
  if (cmd.options().has_durability()) {
    if (cmd.options().durability().has_durabilitylevel()) {
      opts.durability(common::to_durability_level(cmd.options().durability().durabilitylevel()));
    } else if (cmd.options().durability().has_observe()) {
      opts.durability(common::to_persist_to(cmd.options().durability().observe()),
                      common::to_replicate_to(cmd.options().durability().observe()));
    }
  }
  if (cmd.options().has_store_semantics()) {
    switch (cmd.options().store_semantics()) {
      case protocol::sdk::kv::mutate_in::StoreSemantics::INSERT:
        opts.store_semantics(couchbase::store_semantics::insert);
        break;
      case protocol::sdk::kv::mutate_in::StoreSemantics::REPLACE:
        opts.store_semantics(couchbase::store_semantics::upsert);
        break;
      case protocol::sdk::kv::mutate_in::StoreSemantics::UPSERT:
        opts.store_semantics(couchbase::store_semantics::upsert);
        break;
      default:
        throw performer_exception::unimplemented("Unsupported value for mutate-in store semantics");
    }
  }
  if (cmd.options().has_access_deleted()) {
    opts.access_deleted(cmd.options().access_deleted());
  }
  if (cmd.options().has_preserve_expiry()) {
    opts.preserve_expiry(cmd.options().preserve_expiry());
  }
  if (cmd.options().has_create_as_deleted()) {
    opts.create_as_deleted(cmd.options().create_as_deleted());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif
  return opts;
}

couchbase::subdoc::mutate_in_macro
to_mutate_in_macro(const protocol::sdk::kv::mutate_in::MutateInMacro& proto_macro)
{
  switch (proto_macro) {

    case protocol::sdk::kv::mutate_in::CAS:
      return couchbase::subdoc::mutate_in_macro::cas;
    case protocol::sdk::kv::mutate_in::SEQ_NO:
      return couchbase::subdoc::mutate_in_macro::sequence_number;
    case protocol::sdk::kv::mutate_in::VALUE_CRC_32C:
      return couchbase::subdoc::mutate_in_macro::value_crc32c;
    default:
      throw performer_exception::unimplemented("MutateIn macro not recognised");
  }
}

template<typename Cmd>
couchbase::lookup_in_specs
to_lookup_in_specs(const Cmd& cmd)
{
  couchbase::lookup_in_specs specs{};

  for (protocol::sdk::kv::lookup_in::LookupInSpec s : cmd.spec()) {
    if (s.has_exists()) {
      auto spec = couchbase::lookup_in_specs::exists(s.exists().path());
      if (s.exists().has_xattr()) {
        spec.xattr(s.exists().xattr());
      }
      specs.push_back(spec);
    } else if (s.has_get()) {
      auto spec = couchbase::lookup_in_specs::get(s.get().path());
      if (s.get().has_xattr()) {
        spec.xattr(s.get().xattr());
      }
      specs.push_back(spec);
    } else if (s.has_count()) {
      auto spec = couchbase::lookup_in_specs::count(s.count().path());
      if (s.count().has_xattr()) {
        spec.xattr(s.count().xattr());
      }
      specs.push_back(spec);
    } else {
      throw performer_exception::unimplemented("lookup_in spec type not recognised");
    }
  }

  return specs;
}

std::vector<std::byte>
encode_subdoc_array(
  const google::protobuf::RepeatedPtrField<protocol::sdk::kv::mutate_in::ContentOrMacro>& contents)
{
  constexpr std::byte delimiter{ ',' };
  std::vector<std::byte> encoded_items{};
  for (auto content_or_macro : contents) {
    std::vector<std::byte> encoded;
    if (content_or_macro.has_macro()) {
      encoded = couchbase::subdoc::to_binary(to_mutate_in_macro(content_or_macro.macro()));
    } else {
      auto content = common::to_content(content_or_macro.content());
      std::visit(
        common::overloaded{
          [&](auto c) {
            encoded = couchbase::codec::tao_json_serializer::serialize(c);
          },
        },
        content);
      for (std::byte b : encoded) {
        encoded_items.push_back(b);
      }
      encoded_items.push_back(delimiter);
    }
  }
  if (!encoded_items.empty()) {
    // Remove the trailing delimiter
    encoded_items.pop_back();
  }
  return encoded_items;
}

couchbase::mutate_in_specs
to_mutate_in_specs(const protocol::sdk::kv::mutate_in::MutateIn& cmd)
{
  couchbase::mutate_in_specs specs;
  for (auto proto_spec : cmd.spec()) {
    if (proto_spec.has_upsert()) {
      auto s = proto_spec.upsert();
      if (s.content().has_macro()) {
        auto spec =
          couchbase::mutate_in_specs::upsert(s.path(), to_mutate_in_macro(s.content().macro()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        auto content = common::to_content(s.content().content());
        std::visit(
          common::overloaded{
            [&](auto c) {
              auto spec = couchbase::mutate_in_specs::upsert(s.path(), c);
              if (s.has_xattr()) {
                spec.xattr(s.xattr());
              }
              if (s.has_create_path()) {
                spec.create_path(s.create_path());
              }
              specs.push_back(spec);
            },
          },
          content);
      }
    } else if (proto_spec.has_insert()) {
      auto s = proto_spec.insert();
      if (s.content().has_macro()) {
        auto spec =
          couchbase::mutate_in_specs::insert(s.path(), to_mutate_in_macro(s.content().macro()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        auto content = common::to_content(s.content().content());
        std::visit(
          common::overloaded{
            [&](auto c) {
              auto spec = couchbase::mutate_in_specs::insert(s.path(), c);
              if (s.has_xattr()) {
                spec.xattr(s.xattr());
              }
              if (s.has_create_path()) {
                spec.create_path(s.create_path());
              }
              specs.push_back(spec);
            },
          },
          content);
      }
    } else if (proto_spec.has_replace()) {
      auto s = proto_spec.replace();
      if (s.content().has_macro()) {
        auto spec =
          couchbase::mutate_in_specs::replace(s.path(), to_mutate_in_macro(s.content().macro()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        specs.push_back(spec);
      } else {
        auto content = common::to_content(s.content().content());
        std::visit(
          common::overloaded{
            [&](auto c) {
              auto spec = couchbase::mutate_in_specs::replace(s.path(), c);
              if (s.has_xattr()) {
                spec.xattr(s.xattr());
              }
              specs.push_back(spec);
            },
          },
          content);
      }
    } else if (proto_spec.has_remove()) {
      auto s = proto_spec.remove();
      auto spec = couchbase::mutate_in_specs::remove(s.path());
      if (s.has_xattr()) {
        spec.xattr(s.xattr());
      }
      specs.push_back(spec);
    } else if (proto_spec.has_array_append()) {
      auto s = proto_spec.array_append();
      if (s.content_size() != 1) {
        auto spec =
          couchbase::mutate_in_specs::array_append_raw(s.path(), encode_subdoc_array(s.content()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        if (s.content(0).has_macro()) {
          // TODO: Should we add something to the SDK?
          throw performer_exception::unimplemented("cannot use macro with array append");
        } else {
          auto content = common::to_content(s.content(0).content());
          std::visit(
            common::overloaded{
              [&](auto c) {
                auto spec = couchbase::mutate_in_specs::array_append(s.path(), c);
                if (s.has_xattr()) {
                  spec.xattr(s.xattr());
                }
                if (s.has_create_path()) {
                  spec.create_path(s.create_path());
                }
                specs.push_back(spec);
              },
            },
            content);
        }
      }
    } else if (proto_spec.has_array_prepend()) {
      auto s = proto_spec.array_prepend();
      if (s.content_size() != 1) {
        auto spec =
          couchbase::mutate_in_specs::array_prepend_raw(s.path(), encode_subdoc_array(s.content()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        if (s.content(0).has_macro()) {
          // TODO: Should we add something to the SDK?
          throw performer_exception::unimplemented("cannot use macro with array append");
        } else {
          auto content = common::to_content(s.content(0).content());
          std::visit(
            common::overloaded{
              [&](auto c) {
                auto spec = couchbase::mutate_in_specs::array_prepend(s.path(), c);
                if (s.has_xattr()) {
                  spec.xattr(s.xattr());
                }
                if (s.has_create_path()) {
                  spec.create_path(s.create_path());
                }
                specs.push_back(spec);
              },
            },
            content);
        }
      }
    } else if (proto_spec.has_array_insert()) {
      auto s = proto_spec.array_insert();
      if (s.content_size() != 1) {
        auto spec =
          couchbase::mutate_in_specs::array_insert_raw(s.path(), encode_subdoc_array(s.content()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        if (s.content(0).has_macro()) {
          // TODO: Should we add something to the SDK?
          throw performer_exception::unimplemented("cannot use macro with array append");
        } else {
          auto content = common::to_content(s.content(0).content());
          std::visit(
            common::overloaded{
              [&](auto c) {
                auto spec = couchbase::mutate_in_specs::array_insert(s.path(), c);
                if (s.has_xattr()) {
                  spec.xattr(s.xattr());
                }
                if (s.has_create_path()) {
                  spec.create_path(s.create_path());
                }
                specs.push_back(spec);
              },
            },
            content);
        }
      }
    } else if (proto_spec.has_array_add_unique()) {
      auto s = proto_spec.array_add_unique();
      if (s.content().has_macro()) {
        auto spec = couchbase::mutate_in_specs::array_add_unique(
          s.path(), to_mutate_in_macro(s.content().macro()));
        if (s.has_xattr()) {
          spec.xattr(s.xattr());
        }
        if (s.has_create_path()) {
          spec.create_path(s.create_path());
        }
        specs.push_back(spec);
      } else {
        auto content = common::to_content(s.content().content());
        std::visit(
          common::overloaded{
            [&](auto c) {
              auto spec = couchbase::mutate_in_specs::array_add_unique(s.path(), c);
              if (s.has_xattr()) {
                spec.xattr(s.xattr());
              }
              if (s.has_create_path()) {
                spec.create_path(s.create_path());
              }
              specs.push_back(spec);
            },
          },
          content);
      }
    } else if (proto_spec.has_increment()) {
      auto s = proto_spec.increment();
      auto spec = couchbase::mutate_in_specs::increment(s.path(), s.delta());
      if (s.has_xattr()) {
        spec.xattr(s.xattr());
      }
      if (s.has_create_path()) {
        spec.create_path(s.create_path());
      }
      specs.push_back(spec);
    } else if (proto_spec.has_decrement()) {
      auto s = proto_spec.decrement();
      auto spec = couchbase::mutate_in_specs::decrement(s.path(), s.delta());
      if (s.has_xattr()) {
        spec.xattr(s.xattr());
      }
      if (s.has_create_path()) {
        spec.create_path(s.create_path());
      }
      specs.push_back(spec);
    } else {
      throw performer_exception::unimplemented(
        fmt::format("MutateIn operation not supported: {}", proto_spec.DebugString()));
    }
  }
  return specs;
}

void
from_get_result(const couchbase::get_result& res,
                const common::transcoder& transcoder,
                const protocol::shared::ContentAs& content_as,
                protocol::sdk::kv::GetResult* proto_res)
{
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  common::result_to_content(res, transcoder, content_as, proto_res->mutable_content());
  if (res.expiry_time().has_value()) {
    auto expiry =
      std::chrono::duration_cast<std::chrono::seconds>(res.expiry_time()->time_since_epoch());
    proto_res->set_expiry_time(expiry.count());
  }
}

void
from_exists_result(const couchbase::exists_result& res, protocol::sdk::kv::ExistsResult* proto_res)
{
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  proto_res->set_exists(res.exists());
}

void
from_mutation_result(const couchbase::mutation_result& res,
                     protocol::sdk::kv::MutationResult* proto_res)
{
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  if (res.mutation_token().has_value()) {
    proto_res->mutable_mutation_token()->set_bucket_name(
      res.mutation_token().value().bucket_name());
    proto_res->mutable_mutation_token()->set_partition_id(
      res.mutation_token().value().partition_id());
    proto_res->mutable_mutation_token()->set_partition_uuid(
      static_cast<std::int64_t>(res.mutation_token().value().partition_uuid()));
    proto_res->mutable_mutation_token()->set_sequence_number(
      static_cast<std::int64_t>(res.mutation_token().value().sequence_number()));
  }
}

void
from_counter_result(const couchbase::counter_result& res,
                    protocol::sdk::kv::CounterResult* proto_res)
{
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  proto_res->set_content(static_cast<std::int64_t>(res.content()));
  if (res.mutation_token().has_value()) {
    proto_res->mutable_mutation_token()->set_bucket_name(
      res.mutation_token().value().bucket_name());
    proto_res->mutable_mutation_token()->set_partition_id(
      res.mutation_token().value().partition_id());
    proto_res->mutable_mutation_token()->set_partition_uuid(
      static_cast<std::int64_t>(res.mutation_token().value().partition_uuid()));
    proto_res->mutable_mutation_token()->set_sequence_number(
      static_cast<std::int64_t>(res.mutation_token().value().sequence_number()));
  }
}

void
from_get_replica_result(const couchbase::get_replica_result& res,
                        const common::transcoder& transcoder,
                        const protocol::shared::ContentAs& content_as,
                        protocol::sdk::kv::GetReplicaResult* proto_res)
{
  proto_res->set_is_replica(res.is_replica());
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  common::result_to_content(res, transcoder, content_as, proto_res->mutable_content());
}

void
from_lookup_in_result(const couchbase::lookup_in_result& res,
                      const protocol::sdk::kv::lookup_in::LookupIn& cmd,
                      protocol::sdk::kv::lookup_in::LookupInResult* proto_result)
{
  for (std::size_t idx = 0; idx < static_cast<std::size_t>(cmd.spec().size()); idx++) {
    auto proto_spec = cmd.spec().at(static_cast<int>(idx));
    auto proto_spec_result = proto_result->add_results();
    try {
      proto_spec_result->mutable_exists_result()->set_value(res.exists(idx));
    } catch (const std::system_error& e) {
      fit_cxx::commands::common::convert_error_code(
        e.code(), proto_spec_result->mutable_exists_result()->mutable_exception());
    }
    try {
      fit_cxx::commands::common::subdoc_result_to_content(
        res,
        idx,
        proto_spec.content_as(),
        proto_spec_result->mutable_content_as_result()->mutable_content());
    } catch (const std::system_error& e) {
      fit_cxx::commands::common::convert_error_code(
        e.code(), proto_spec_result->mutable_content_as_result()->mutable_exception());
    }
  }
}

void
from_mutate_in_result(const couchbase::mutate_in_result& res,
                      const protocol::sdk::kv::mutate_in::MutateIn& cmd,
                      protocol::sdk::kv::mutate_in::MutateInResult* proto_res)
{
  proto_res->set_cas(static_cast<std::int64_t>(res.cas().value()));
  if (res.mutation_token().has_value()) {
    proto_res->mutable_mutation_token()->set_bucket_name(
      res.mutation_token().value().bucket_name());
    proto_res->mutable_mutation_token()->set_partition_id(
      res.mutation_token().value().partition_id());
    proto_res->mutable_mutation_token()->set_partition_uuid(
      static_cast<std::int64_t>(res.mutation_token().value().partition_uuid()));
    proto_res->mutable_mutation_token()->set_sequence_number(
      static_cast<std::int64_t>(res.mutation_token().value().sequence_number()));
  }
  for (std::size_t idx = 0; idx < static_cast<std::size_t>(cmd.spec().size()); idx++) {
    auto proto_spec = cmd.spec().at(static_cast<int>(idx));
    auto proto_spec_result = proto_res->add_results();
    try {
      fit_cxx::commands::common::subdoc_result_to_content(
        res,
        idx,
        proto_spec.content_as(),
        proto_spec_result->mutable_content_as_result()->mutable_content());
    } catch (const std::system_error& e) {
      fit_cxx::commands::common::convert_error_code(
        e.code(), proto_spec_result->mutable_content_as_result()->mutable_exception());
    }
  }
}

void
from_scan_result_item(const std::string& stream_id,
                      const couchbase::scan_result_item& item,
                      const common::transcoder& transcoder,
                      const std::optional<protocol::shared::ContentAs>& content_as,
                      protocol::sdk::kv::rangescan::ScanResult* proto_res)
{
  proto_res->set_stream_id(stream_id);
  proto_res->set_id(item.id());
  proto_res->set_id_only(item.id_only());

  if (item.id_only()) {
    return;
  }

  if (item.expiry_time().has_value()) {
    auto expiry =
      std::chrono::duration_cast<std::chrono::seconds>(item.expiry_time()->time_since_epoch());
    proto_res->set_expiry_time(expiry.count());
  }
  if (content_as.has_value()) {
    common::result_to_content(item, transcoder, content_as.value(), proto_res->mutable_content());
  }
  proto_res->set_cas(static_cast<std::int64_t>(item.cas().value()));
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Insert& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto content = common::to_content(cmd.content());
  auto options = to_insert_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  std::chrono::time_point<std::chrono::high_resolution_clock> start{};
  std::chrono::time_point<std::chrono::high_resolution_clock> end{};
  std::pair<couchbase::error, couchbase::mutation_result> result;
  std::visit(common::overloaded{
               [&](auto c, couchbase::codec::default_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_binary_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_string_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto c, std::monostate) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.insert(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto /* c */, auto /* t */) {
                 throw performer_exception::unimplemented(
                   "The SDK does not support the given content/transcoder combination");
               },
             },
             content,
             transcoder);

  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  auto err = result.first;
  auto res = result.second;

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Upsert& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto content = common::to_content(cmd.content());
  auto options = to_upsert_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  std::chrono::time_point<std::chrono::high_resolution_clock> start{};
  std::chrono::time_point<std::chrono::high_resolution_clock> end{};
  std::pair<couchbase::error, couchbase::mutation_result> result;

  std::visit(common::overloaded{
               [&](auto c, couchbase::codec::default_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_binary_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_string_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto c, std::monostate) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.upsert(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto /* c */, auto /* t */) {
                 throw performer_exception::unimplemented(
                   "The SDK does not support the given content/transcoder combination");
               },
             },
             content,
             transcoder);

  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  auto err = result.first;
  auto res = result.second;

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Replace& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto content = common::to_content(cmd.content());
  auto options = to_replace_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  std::chrono::time_point<std::chrono::high_resolution_clock> start{};
  std::chrono::time_point<std::chrono::high_resolution_clock> end{};
  std::pair<couchbase::error, couchbase::mutation_result> result;
  std::visit(common::overloaded{
               [&](auto c, couchbase::codec::default_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_binary_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::vector<std::byte> c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_json_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](std::string c, couchbase::codec::raw_string_transcoder t) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace<decltype(t)>(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto c, std::monostate) {
                 start = std::chrono::high_resolution_clock::now();
                 result = collection.replace(key, c, options).get();
                 end = std::chrono::high_resolution_clock::now();
               },
               [&](auto /* c */, auto /* t */) {
                 throw performer_exception::unimplemented(
                   "The SDK does not support the given content/transcoder combination");
               },
             },
             content,
             transcoder);

  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  auto err = result.first;
  auto res = result.second;

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Remove& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_remove_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.remove(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Get& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_get_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.get(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_get_result(
        res, transcoder, cmd.content_as(), proto_res.mutable_sdk()->mutable_get_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::lookup_in::LookupIn& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto specs = to_lookup_in_specs(cmd);
  auto options = to_lookup_in_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.lookup_in(key, specs, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_lookup_in_result(res, cmd, proto_res.mutable_sdk()->mutable_lookup_in_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAndLock& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto lock_duration = std::chrono::seconds{ cmd.duration().seconds() };
  auto options = to_get_and_lock_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.get_and_lock(key, lock_duration, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_get_result(
        res, transcoder, cmd.content_as(), proto_res.mutable_sdk()->mutable_get_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Unlock& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto cas = couchbase::cas{ static_cast<std::uint64_t>(cmd.cas()) };
  auto options = to_unlock_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto err = collection.unlock(key, cas, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    proto_res.mutable_sdk()->set_success(true);
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAndTouch& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_get_and_touch_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  auto start = std::chrono::high_resolution_clock::now();
  std::pair<couchbase::error, couchbase::get_result> resp;
  if (cmd.expiry().has_relativesecs()) {
    auto expiry = std::chrono::seconds{ cmd.expiry().relativesecs() };
    resp = collection.get_and_touch(key, expiry, options).get();
  } else {
    auto time_since_epoch = std::chrono::seconds{ cmd.expiry().absoluteepochsecs() };
    auto expiry = std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch };
    resp = collection.get_and_touch(key, expiry, options).get();
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto [err, res] = resp;
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_get_result(
        res, transcoder, cmd.content_as(), proto_res.mutable_sdk()->mutable_get_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Exists& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_exists_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.exists(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_exists_result(res, proto_res.mutable_sdk()->mutable_exists_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Touch& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_touch_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();

  std::pair<couchbase::error, couchbase::result> resp;
  if (cmd.expiry().has_relativesecs()) {
    auto expiry = std::chrono::seconds{ cmd.expiry().relativesecs() };
    resp = collection.touch(key, expiry, options).get();
  } else {
    auto time_since_epoch = std::chrono::seconds{ cmd.expiry().absoluteepochsecs() };
    auto expiry = std::chrono::time_point<std::chrono::system_clock>{ time_since_epoch };
    resp = collection.touch(key, expiry, options).get();
  }
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());
  auto [err, res] = resp;

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      proto_res.mutable_sdk()->mutable_mutation_result()->set_cas(
        static_cast<std::int64_t>(res.cas().value()));
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::mutate_in::MutateIn& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto specs = to_mutate_in_specs(cmd);
  auto options = to_mutate_in_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.mutate_in(key, specs, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutate_in_result(res, cmd, proto_res.mutable_sdk()->mutable_mutate_in_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Append& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto binary_collection = args.collection.binary();
  auto key = args.key;
  auto data = couchbase::core::utils::to_binary(cmd.content());
  auto options = to_append_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = binary_collection.append(key, data, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Prepend& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto binary_collection = args.collection.binary();
  auto key = args.key;
  auto data = couchbase::core::utils::to_binary(cmd.content());
  auto options = to_prepend_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = binary_collection.prepend(key, data, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_mutation_result(res, proto_res.mutable_sdk()->mutable_mutation_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Increment& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto binary_collection = args.collection.binary();
  auto key = args.key;
  auto options = to_increment_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = binary_collection.increment(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_counter_result(res, proto_res.mutable_sdk()->mutable_counter_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::Decrement& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto binary_collection = args.collection.binary();
  auto key = args.key;
  auto options = to_decrement_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = binary_collection.decrement(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_counter_result(res, proto_res.mutable_sdk()->mutable_counter_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

protocol::run::Result
execute_command(const protocol::sdk::kv::GetAnyReplica& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto options = to_get_any_replica_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.get_any_replica(key, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_get_replica_result(
        res, transcoder, cmd.content_as(), proto_res.mutable_sdk()->mutable_get_replica_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::GetAllReplicas& cmd, const command_args& args)
{
  auto collection = args.collection;
  auto key = args.key;
  auto options = to_get_all_replicas_options(cmd, args.spans);
  auto transcoder = common::to_transcoder(cmd);

  auto [err, res] = collection.get_all_replicas(key, options).get();

  if (err.ec()) {
    auto proto_res = common::create_new_result();
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
    return proto_res;
  } else {
    if (args.return_result) {
      auto res_index = std::make_shared<std::int64_t>(0);
      return [res_index,
              stream_id = cmd.stream_config().stream_id(),
              res = std::move(res),
              transcoder = std::move(transcoder),
              content_as = cmd.content_as()]() -> std::optional<protocol::run::Result> {
        if (static_cast<std::size_t>(*res_index) >= res.size()) {
          spdlog::trace("next_fn for get_all_replicas has no more entries");
          return std::nullopt;
        }
        auto replica_res = res.at(static_cast<std::size_t>(*res_index));
        auto proto_res = common::create_new_result();
        from_get_replica_result(replica_res,
                                transcoder,
                                content_as,
                                proto_res.mutable_sdk()->mutable_get_replica_result());
        proto_res.mutable_sdk()->mutable_get_replica_result()->set_stream_id(stream_id);
        *res_index = *res_index + 1;
        return proto_res;
      };
    } else {
      auto proto_res = common::create_new_result();
      proto_res.mutable_sdk()->set_success(!res.empty());
      return proto_res;
    }
  }
}

couchbase::lookup_in_any_replica_options
to_lookup_in_any_replica_options(const protocol::sdk::kv::lookup_in::LookupInAnyReplica& cmd,
                                 observability::span_owner* spans)
{
  couchbase::lookup_in_any_replica_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_millis()) {
    opts.timeout(std::chrono::milliseconds{ cmd.options().timeout_millis() });
  }
  if (cmd.options().has_read_preference()) {
    opts.read_preference(to_read_preference(cmd.options().read_preference()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::lookup_in_all_replicas_options
to_lookup_in_all_replicas_options(const protocol::sdk::kv::lookup_in::LookupInAllReplicas& cmd,
                                  observability::span_owner* spans)
{
  couchbase::lookup_in_all_replicas_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_timeout_millis()) {
    opts.timeout(std::chrono::milliseconds{ cmd.options().timeout_millis() });
  }
  if (cmd.options().has_read_preference()) {
    opts.read_preference(to_read_preference(cmd.options().read_preference()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

void
from_lookup_in_replica_result(
  const couchbase::lookup_in_replica_result& result,
  const google::protobuf::RepeatedPtrField<protocol::sdk::kv::lookup_in::LookupInSpec>& proto_specs,
  protocol::sdk::kv::lookup_in::LookupInReplicaResult* proto_result)
{
  for (std::size_t idx = 0; idx < static_cast<std::size_t>(proto_specs.size()); idx++) {
    auto proto_spec = proto_specs.at(static_cast<int>(idx));
    auto proto_spec_result = proto_result->mutable_results()->Add();
    try {
      proto_spec_result->mutable_exists_result()->set_value(result.exists(idx));
    } catch (const std::system_error& e) {
      fit_cxx::commands::common::convert_error_code(
        e.code(), proto_spec_result->mutable_exists_result()->mutable_exception());
    }
    try {
      fit_cxx::commands::common::subdoc_result_to_content(
        result,
        idx,
        proto_spec.content_as(),
        proto_spec_result->mutable_content_as_result()->mutable_content());
    } catch (const std::system_error& e) {
      fit_cxx::commands::common::convert_error_code(
        e.code(), proto_spec_result->mutable_content_as_result()->mutable_exception());
    }
  }
  proto_result->set_is_replica(result.is_replica());
}

protocol::run::Result
execute_command(const protocol::sdk::kv::lookup_in::LookupInAnyReplica& cmd,
                const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto key = args.key;
  auto specs = to_lookup_in_specs(cmd);
  auto options = to_lookup_in_any_replica_options(cmd, args.spans);

  auto start = std::chrono::high_resolution_clock::now();
  auto [err, res] = collection.lookup_in_any_replica(key, specs, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err.ec()) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    if (args.return_result) {
      from_lookup_in_replica_result(
        res, cmd.spec(), proto_res.mutable_sdk()->mutable_lookup_in_any_replica_result());
    } else {
      proto_res.mutable_sdk()->set_success(!res.cas().empty());
    }
  }
  return proto_res;
}

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::lookup_in::LookupInAllReplicas& cmd,
                          const command_args& args)
{
  auto collection = args.collection;
  auto key = args.key;
  auto specs = to_lookup_in_specs(cmd);
  auto options = to_lookup_in_all_replicas_options(cmd, args.spans);

  auto [err, res] = collection.lookup_in_all_replicas(key, specs, options).get();

  if (err.ec()) {
    auto proto_res = common::create_new_result();
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
    return proto_res;
  } else {
    if (args.return_result) {
      auto res_index = std::make_shared<std::int64_t>(0);
      return [res_index,
              stream_id = cmd.stream_config().stream_id(),
              res = std::move(res),
              cmd = cmd]() -> std::optional<protocol::run::Result> {
        if (static_cast<std::size_t>(*res_index) >= res.size()) {
          spdlog::trace("next_fn for get_all_replicas has no more entries");
          return std::nullopt;
        }
        auto replica_res = res.at(static_cast<std::size_t>(*res_index));
        auto proto_res = common::create_new_result();
        from_lookup_in_replica_result(res.at(static_cast<std::size_t>(*res_index)),
                                      cmd.spec(),
                                      proto_res.mutable_sdk()
                                        ->mutable_lookup_in_all_replicas_result()
                                        ->mutable_lookup_in_replica_result());
        proto_res.mutable_sdk()->mutable_lookup_in_all_replicas_result()->set_stream_id(stream_id);
        *res_index = *res_index += 1;
        return proto_res;
      };
    } else {
      auto proto_res = common::create_new_result();
      proto_res.mutable_sdk()->set_success(!res.empty());
      return proto_res;
    }
  }
}

std::optional<couchbase::scan_term>
to_scan_term(const protocol::sdk::kv::rangescan::ScanTermChoice& cmd)
{
  if (cmd.has_term()) {
    auto scan_term = couchbase::scan_term(cmd.term().as_string());
    if (cmd.term().has_exclusive()) {
      scan_term.exclusive(cmd.term().exclusive());
    }
    return scan_term;
  } else {
    return {};
  }
}

auto
to_scan_type(const protocol::sdk::kv::rangescan::Scan& cmd) -> std::unique_ptr<couchbase::scan_type>
{
  auto proto_scan_type = cmd.scan_type();
  switch (proto_scan_type.type_case()) {
    case protocol::sdk::kv::rangescan::ScanType::TypeCase::kRange:
      switch (proto_scan_type.range().range_case()) {
        case protocol::sdk::kv::rangescan::RangeScan::RangeCase::kDocIdPrefix: {
          return std::make_unique<couchbase::prefix_scan>(proto_scan_type.range().doc_id_prefix());
        }
        case protocol::sdk::kv::rangescan::RangeScan::RangeCase::kFromTo: {
          std::optional<couchbase::scan_term> from{};
          std::optional<couchbase::scan_term> to{};
          auto proto_range_scan = cmd.scan_type().range().from_to();
          if (proto_range_scan.has_from()) {
            from = to_scan_term(proto_range_scan.from());
          }
          if (proto_range_scan.has_to()) {
            to = to_scan_term(proto_range_scan.to());
          }
          return std::make_unique<couchbase::range_scan>(from, to);
        }
        default:
          throw performer_exception::unimplemented(
            fmt::format("unexpected range case in {}", proto_scan_type.range().DebugString()));
      }
    case protocol::sdk::kv::rangescan::ScanType::TypeCase::kSampling: {
      if (proto_scan_type.sampling().has_seed()) {
        return std::make_unique<couchbase::sampling_scan>(proto_scan_type.sampling().limit(),
                                                          proto_scan_type.sampling().seed());
      } else {
        return std::make_unique<couchbase::sampling_scan>(proto_scan_type.sampling().limit());
      }
    }
    default:
      throw performer_exception::unimplemented(
        fmt::format("unknown scan type: {}", cmd.DebugString()));
  }
}

couchbase::scan_options
to_scan_options(const protocol::sdk::kv::rangescan::Scan& cmd)
{
  couchbase::scan_options opts{};

  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_ids_only()) {
    opts.ids_only(cmd.options().ids_only());
  }
  if (cmd.options().has_batch_byte_limit()) {
    opts.batch_byte_limit(cmd.options().batch_byte_limit());
  }
  if (cmd.options().has_batch_item_limit()) {
    opts.batch_item_limit(cmd.options().batch_item_limit());
  }
  if (cmd.options().has_concurrency()) {
    opts.concurrency(static_cast<std::uint16_t>(cmd.options().concurrency()));
  }
  if (cmd.options().has_timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
  if (cmd.options().has_consistent_with()) {
    couchbase::mutation_state state{};
    for (auto& t : cmd.options().consistent_with().tokens()) {
      // Create a dummy mutation result to add the token to the state
      couchbase::mutation_token token{ static_cast<std::uint64_t>(t.partition_uuid()),
                                       static_cast<std::uint64_t>(t.sequence_number()),
                                       static_cast<std::uint16_t>(t.partition_id()),
                                       t.bucket_name() };
      couchbase::mutation_result mutation_result{ couchbase::cas{ 0 }, token };
      state.add(mutation_result);
    }
    opts.consistent_with(state);
  }

  return opts;
}

std::variant<protocol::run::Result, fit_cxx::next_function>
execute_streaming_command(const protocol::sdk::kv::rangescan::Scan& cmd, const command_args& args)
{
  auto proto_res = common::create_new_result();

  auto collection = args.collection;
  auto scan_type = to_scan_type(cmd);
  auto options = to_scan_options(cmd);
  auto transcoder = common::to_transcoder(cmd);

  auto start = std::chrono::high_resolution_clock::now();
  auto resp = collection.scan(*scan_type, options).get();
  auto end = std::chrono::high_resolution_clock::now();
  auto err = resp.first;
  auto res = resp.second;
  proto_res.set_elapsednanos(std::chrono::nanoseconds(end - start).count());

  if (err) {
    common::convert_error(err, proto_res.mutable_sdk()->mutable_exception());
  } else {
    std::optional<protocol::shared::ContentAs> content_as{};
    if (cmd.has_content_as()) {
      content_as = cmd.content_as();
    }
    return [transcoder,
            content_as,
            stream_id = cmd.stream_config().stream_id(),
            res = std::move(res)]() -> std::optional<protocol::run::Result> {
      auto [next_err, item] = res.next().get();
      auto proto_res = common::create_new_result();
      if (next_err) {
        spdlog::trace("next_fn got err calling scan_result.next() : {}", next_err.message());
        fit_cxx::commands::common::convert_error(next_err,
                                                 proto_res.mutable_sdk()->mutable_exception());
        return proto_res;
      }
      if (!item.has_value()) {
        spdlog::trace("next_fn has no more scan entries, returning std::nullopt");
        return std::nullopt;
      }
      from_scan_result_item(stream_id,
                            item.value(),
                            transcoder,
                            content_as,
                            proto_res.mutable_sdk()->mutable_range_scan_result());
      return proto_res;
    };
  }
  return proto_res;
}
} // namespace fit_cxx::commands::key_value
