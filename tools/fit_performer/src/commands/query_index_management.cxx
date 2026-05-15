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

#include "query_index_management.hxx"

#include "../exceptions.hxx"
#include "common.hxx"
#include "run.top_level.pb.h"
#include "sdk.cluster.query.index_manager.pb.h"

#include "core/meta/features.hxx"

namespace fit_cxx::commands::query_index_management
{
couchbase::create_primary_query_index_options
to_create_primary_index_options(const protocol::sdk::query::index_manager::CreatePrimaryIndex& cmd,
                                observability::span_owner* spans)
{
  couchbase::create_primary_query_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_ignore_if_exists()) {
    opts.ignore_if_exists(cmd.options().ignore_if_exists());
  }
  if (cmd.options().has_num_replicas()) {
    opts.num_replicas(static_cast<std::uint8_t>(cmd.options().num_replicas()));
  }
  if (cmd.options().has_deferred()) {
    opts.build_deferred(cmd.options().deferred());
  }
  if (cmd.options().has_index_name()) {
    opts.index_name(cmd.options().index_name());
  }
  if (cmd.options().timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::create_query_index_options
to_create_index_options(const protocol::sdk::query::index_manager::CreateIndex& cmd,
                        observability::span_owner* spans)
{
  couchbase::create_query_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_ignore_if_exists()) {
    opts.ignore_if_exists(cmd.options().ignore_if_exists());
  }
  if (cmd.options().has_num_replicas()) {
    opts.num_replicas(static_cast<std::uint8_t>(cmd.options().num_replicas()));
  }
  if (cmd.options().has_deferred()) {
    opts.build_deferred(cmd.options().deferred());
  }
  if (cmd.options().timeout_msecs()) {
    opts.timeout(std::chrono::milliseconds(cmd.options().timeout_msecs()));
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::drop_primary_query_index_options
to_drop_primary_index_options(const protocol::sdk::query::index_manager::DropPrimaryIndex& cmd,
                              observability::span_owner* spans)
{
  couchbase::drop_primary_query_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_ignore_if_not_exists()) {
    opts.ignore_if_not_exists(cmd.options().ignore_if_not_exists());
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

couchbase::drop_query_index_options
to_drop_index_options(const protocol::sdk::query::index_manager::DropIndex& cmd,
                      observability::span_owner* spans)
{
  couchbase::drop_query_index_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_ignore_if_not_exists()) {
    opts.ignore_if_not_exists(cmd.options().ignore_if_not_exists());
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

couchbase::watch_query_indexes_options
to_watch_indexes_options(const protocol::sdk::query::index_manager::WatchIndexes& cmd,
                         observability::span_owner* spans)
{
  couchbase::watch_query_indexes_options opts{};
  if (!cmd.has_options()) {
    return opts;
  }

  if (cmd.options().has_watch_primary()) {
    opts.watch_primary(cmd.options().watch_primary());
  }
#ifdef COUCHBASE_CXX_CLIENT_PUBLIC_API_PARENT_SPAN
  if (cmd.options().has_parent_span_id()) {
    opts.parent_span(spans->get_span(cmd.options().parent_span_id()));
  }
#endif

  return opts;
}

couchbase::build_query_index_options
to_build_deferred_indexes_options(
  const protocol::sdk::query::index_manager::BuildDeferredIndexes& cmd,
  observability::span_owner* spans)
{
  couchbase::build_query_index_options opts{};
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

couchbase::get_all_query_indexes_options
to_get_all_indexes_options(const protocol::sdk::query::index_manager::GetAllIndexes& cmd,
                           observability::span_owner* spans)
{
  couchbase::get_all_query_indexes_options opts{};
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

protocol::sdk::query::index_manager::QueryIndexType
from_query_index_type(const std::string& type)
{
  if (type == "gsi") {
    return protocol::sdk::query::index_manager::QueryIndexType::GSI;
  }
  if (type == "view") {
    return protocol::sdk::query::index_manager::QueryIndexType::VIEW;
  }
  throw performer_exception::internal("unrecognized query index type received from server");
}

void
from_query_index_result(const std::vector<couchbase::management::query_index>& result,
                        protocol::sdk::query::index_manager::QueryIndexes* res)
{
  for (const auto& index : result) {
    auto* idx = res->add_indexes();
    idx->set_name(index.name);
    idx->set_is_primary(index.is_primary);
    idx->set_type(from_query_index_type(index.type));
    idx->set_state(index.state);
    idx->set_bucket_name(index.bucket_name);
    idx->mutable_index_key()->Add(index.index_key.begin(), index.index_key.end());
    if (index.scope_name.has_value()) {
      idx->set_scope_name(index.scope_name.value());
    }
    if (index.collection_name.has_value()) {
      idx->set_collection_name(index.collection_name.value());
    }
    if (index.condition.has_value()) {
      idx->set_condition(index.condition.value());
    }
    if (index.partition.has_value()) {
      idx->set_partition(index.partition.value());
    }
  }
}

protocol::run::Result
execute_shared_command(const protocol::sdk::query::index_manager::Command& cmd, command_args& args)
{
  protocol::run::Result res;
  if (args.cluster) {
    auto manager = args.cluster->query_indexes();
    if (cmd.has_create_primary_index()) {
      const auto opts = to_create_primary_index_options(cmd.create_primary_index(), args.spans);
      auto err = manager.create_primary_index(args.bucket_name.value(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_create_index()) {
      const auto opts = to_create_index_options(cmd.create_index(), args.spans);
      std::vector<std::string> fields{ cmd.create_index().fields().begin(),
                                       cmd.create_index().fields().end() };
      auto err =
        manager
          .create_index(args.bucket_name.value(), cmd.create_index().index_name(), fields, opts)
          .get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_get_all_indexes()) {
      const auto opts = to_get_all_indexes_options(cmd.get_all_indexes(), args.spans);
      auto [err, result] = manager.get_all_indexes(args.bucket_name.value(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        if (args.return_result) {
          from_query_index_result(result, res.mutable_sdk()->mutable_query_indexes());
        } else {
          res.mutable_sdk()->set_success(true);
        }
      }
    } else if (cmd.has_drop_primary_index()) {
      const auto opts = to_drop_primary_index_options(cmd.drop_primary_index(), args.spans);
      auto err = manager.drop_primary_index(args.bucket_name.value(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_drop_index()) {
      const auto opts = to_drop_index_options(cmd.drop_index(), args.spans);
      auto err =
        manager.drop_index(args.bucket_name.value(), cmd.drop_index().index_name(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_watch_indexes()) {
      const auto opts = to_watch_indexes_options(cmd.watch_indexes(), args.spans);
      std::vector<std::string> index_names{ cmd.watch_indexes().index_names().begin(),
                                            cmd.watch_indexes().index_names().end() };
      auto err = manager.watch_indexes(args.bucket_name.value(), index_names, opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_build_deferred_indexes()) {
      const auto opts = to_build_deferred_indexes_options(cmd.build_deferred_indexes(), args.spans);
      auto err = manager.build_deferred_indexes(args.bucket_name.value(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  } else {
    auto manager = args.collection->query_indexes();
    if (cmd.has_create_primary_index()) {
      const auto opts = to_create_primary_index_options(cmd.create_primary_index(), args.spans);
      auto err = manager.create_primary_index(opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_create_index()) {
      const auto opts = to_create_index_options(cmd.create_index(), args.spans);
      std::vector<std::string> fields{ cmd.create_index().fields().begin(),
                                       cmd.create_index().fields().end() };
      auto err = manager.create_index(cmd.create_index().index_name(), fields, opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_get_all_indexes()) {
      const auto opts = to_get_all_indexes_options(cmd.get_all_indexes(), args.spans);
      auto [err, result] = manager.get_all_indexes(opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        if (args.return_result) {
          from_query_index_result(result, res.mutable_sdk()->mutable_query_indexes());
        } else {
          res.mutable_sdk()->set_success(true);
        }
      }
    } else if (cmd.has_drop_primary_index()) {
      const auto opts = to_drop_primary_index_options(cmd.drop_primary_index(), args.spans);
      auto err = manager.drop_primary_index(opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_drop_index()) {
      const auto opts = to_drop_index_options(cmd.drop_index(), args.spans);
      auto err = manager.drop_index(cmd.drop_index().index_name(), opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_watch_indexes()) {
      const auto opts = to_watch_indexes_options(cmd.watch_indexes(), args.spans);
      std::vector<std::string> index_names{ cmd.watch_indexes().index_names().begin(),
                                            cmd.watch_indexes().index_names().end() };
      auto err = manager.watch_indexes(index_names, opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    } else if (cmd.has_build_deferred_indexes()) {
      const auto opts = to_build_deferred_indexes_options(cmd.build_deferred_indexes(), args.spans);
      auto err = manager.build_deferred_indexes(opts).get();
      if (err.ec()) {
        common::convert_error(err, res.mutable_sdk()->mutable_exception());
      } else {
        res.mutable_sdk()->set_success(true);
      }
    }
  }
  return res;
}

protocol::run::Result
execute_cluster_command(const protocol::sdk::cluster::query::index_manager::Command& cmd,
                        command_args& args)
{
  args.bucket_name = cmd.bucket_name();
  return execute_shared_command(cmd.shared(), args);
}

protocol::run::Result
execute_collection_command(const protocol::sdk::collection::query::index_manager::Command& cmd,
                           command_args& args)
{
  return execute_shared_command(cmd.shared(), args);
}
} // namespace fit_cxx::commands::query_index_management
