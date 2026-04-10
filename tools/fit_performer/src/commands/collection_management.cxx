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

#include "collection_management.hxx"
#include "common.hxx"

#include "core/meta/features.hxx"

#include <spdlog/spdlog.h>

namespace fit_cxx::commands::collection_management
{
couchbase::get_all_scopes_options
to_get_all_scopes_options(const protocol::sdk::bucket::collection_manager::GetAllScopesRequest& cmd,
                          observability::span_owner* spans)
{
  couchbase::get_all_scopes_options opts{};
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

couchbase::create_collection_options
to_create_collection_options(
  const protocol::sdk::bucket::collection_manager::CreateCollectionRequest& cmd,
  observability::span_owner* spans)
{
  couchbase::create_collection_options opts{};
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

couchbase::update_collection_options
to_update_collection_options(
  const protocol::sdk::bucket::collection_manager::UpdateCollectionRequest& cmd,
  observability::span_owner* spans)
{
  couchbase::update_collection_options opts{};
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

couchbase::drop_collection_options
to_drop_collection_options(
  const protocol::sdk::bucket::collection_manager::DropCollectionRequest& cmd,
  observability::span_owner* spans)
{
  couchbase::drop_collection_options opts{};
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

couchbase::create_scope_options
to_create_scope_options(const protocol::sdk::bucket::collection_manager::CreateScopeRequest& cmd,
                        observability::span_owner* spans)
{
  couchbase::create_scope_options opts{};
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

couchbase::drop_scope_options
to_drop_scope_options(const protocol::sdk::bucket::collection_manager::DropScopeRequest& cmd,
                      observability::span_owner* spans)
{
  couchbase::drop_scope_options opts{};
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

couchbase::create_collection_settings
to_create_collection_settings(
  const protocol::sdk::bucket::collection_manager::CreateCollectionRequest& cmd)
{
  couchbase::create_collection_settings settings{};
  if (cmd.has_settings()) {
    if (cmd.settings().has_expiry_secs()) {
      settings.max_expiry = cmd.settings().expiry_secs();
    }
    if (cmd.settings().has_history()) {
      settings.history = cmd.settings().history();
    }
  }
  return settings;
}

couchbase::update_collection_settings
to_update_collection_settings(
  const protocol::sdk::bucket::collection_manager::UpdateCollectionRequest& cmd)
{
  couchbase::update_collection_settings settings{};
  if (cmd.has_settings()) {
    if (cmd.settings().has_expiry_secs()) {
      settings.max_expiry = cmd.settings().expiry_secs();
    }
    if (cmd.settings().has_history()) {
      settings.history = cmd.settings().history();
    }
  }
  return settings;
}

void
from_get_all_scopes_result(const std::vector<couchbase::management::bucket::scope_spec>& result,
                           protocol::sdk::bucket::collection_manager::GetAllScopesResult* res)
{
  for (const auto& scope_spec : result) {
    auto* scope = res->add_result();
    scope->set_name(scope_spec.name);
    for (const auto& collection_spec : scope_spec.collections) {
      auto* collection = scope->add_collections();
      collection->set_name(collection_spec.name);
      collection->set_scope_name(collection_spec.scope_name);
      collection->set_expiry_secs(collection_spec.max_expiry);
      if (collection_spec.history.has_value()) {
        collection->set_history(collection_spec.history.value());
      }
    }
  }
}

protocol::run::Result
execute_command(const protocol::sdk::bucket::collection_manager::Command& cmd,
                const command_args& args)
{
  protocol::run::Result res;
  auto manager = args.bucket.collections();
  if (cmd.has_create_collection()) {
    const auto& create_collection = cmd.create_collection();
    const auto opts = to_create_collection_options(create_collection, args.spans);
    const auto settings = to_create_collection_settings(create_collection);
    auto err =
      manager
        .create_collection(create_collection.scope_name(), create_collection.name(), settings, opts)
        .get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_create_scope()) {
    const auto& create_scope = cmd.create_scope();
    const auto opts = to_create_scope_options(create_scope, args.spans);
    auto err = manager.create_scope(create_scope.name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_update_collection()) {
    const auto& update_collection = cmd.update_collection();
    const auto opts = to_update_collection_options(update_collection, args.spans);
    const auto settings = to_update_collection_settings(update_collection);
    auto err =
      manager
        .update_collection(update_collection.scope_name(), update_collection.name(), settings, opts)
        .get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_drop_collection()) {
    const auto& drop_collection = cmd.drop_collection();
    const auto opts = to_drop_collection_options(drop_collection, args.spans);
    auto err =
      manager.drop_collection(drop_collection.scope_name(), drop_collection.name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_drop_scope()) {
    const auto& drop_scope = cmd.drop_scope();
    const auto opts = to_drop_scope_options(drop_scope, args.spans);
    auto err = manager.drop_scope(drop_scope.name(), opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      res.mutable_sdk()->set_success(true);
    }
  } else if (cmd.has_get_all_scopes()) {
    const auto& get_all_scopes = cmd.get_all_scopes();
    const auto opts = to_get_all_scopes_options(get_all_scopes, args.spans);
    auto [err, result] = manager.get_all_scopes(opts).get();
    if (err.ec()) {
      common::convert_error(err, res.mutable_sdk()->mutable_exception());
    } else {
      if (args.return_result) {
        from_get_all_scopes_result(
          result,
          res.mutable_sdk()->mutable_collection_manager_result()->mutable_get_all_scopes_result());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  }
  return res;
}
} // namespace fit_cxx::commands::collection_management
