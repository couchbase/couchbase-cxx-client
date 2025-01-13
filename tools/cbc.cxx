/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2023-Present Couchbase, Inc.
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

#include "analytics.hxx"
#include "beam.hxx"
#include "get.hxx"
#include "pillowfight.hxx"
#include "query.hxx"
#include "version.hxx"

#include "core/meta/version.hxx"

#include <spdlog/fmt/bundled/core.h>

int
main(int argc, const char** argv)
{
  CLI::App app{ "Talk to Couchbase Server.", "cbc" };
  app.set_version_flag("--version", fmt::format("cbc {}", couchbase::core::meta::sdk_semver()));
  app.require_subcommand(1);
  app.allow_windows_style_options(true);

  app.add_subcommand(cbc::make_version_command());
  app.add_subcommand(cbc::make_get_command());
  app.add_subcommand(cbc::make_query_command());
  app.add_subcommand(cbc::make_analytics_command());
  app.add_subcommand(cbc::make_pillowfight_command());
  app.add_subcommand(cbc::make_beam_command());

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  }

  for (const auto& item : app.get_subcommands()) {
    if (item->get_name() == "version") {
      return cbc::execute_version_command(item);
    }
    if (item->get_name() == "get") {
      return cbc::execute_get_command(item);
    }
    if (item->get_name() == "query") {
      return cbc::execute_query_command(item);
    }
    if (item->get_name() == "analytics") {
      return cbc::execute_analytics_command(item);
    }
    if (item->get_name() == "pillowfight") {
      return cbc::execute_pillowfight_command(item);
    }
    if (item->get_name() == "beam") {
      return cbc::execute_beam_command(item);
    }
  }

  return 0;
}
