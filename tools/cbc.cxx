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

#include "command_registry.hxx"
#include "utils.hxx"

static constexpr auto* USAGE =
  R"(Talk to Couchbase Server.

Usage:
  cbc <command> [<argument>...]
  cbc (-h|--help)
  cbc --version

Available commands:

  version      Display version information.
  get          Retrieve document from the server.
  query        Perform N1QL query.
  analytics    Perform Analytics query.
  pillowfight  Run workload generator.

Options:
  -h --help  Show this screen.
  --version  Show version.
)";

int
main(int argc, const char** argv)
{
    cbc::command_registry commands;
    try {
        std::vector<std::string> args{ argv + 1, argv + argc };
        auto options = cbc::parse_options(USAGE, args, true);

        if (options["--help"].asBool()) {
            fmt::print(stdout, USAGE);
            return 0;
        }

        auto command_name = options["<command>"].asString();
        if (auto command = commands.get(command_name); command) {
            try {
                command->execute(args);
            } catch (const std::exception& e) {
                fmt::print(stderr, "Error: {}\n", e.what());
                return 1;
            }
            return 0;
        }
        fmt::print(stderr, "Error: unrecognized command 'cbc {}'\n", command_name);
        fmt::print(stderr, "Try 'cbc --help' for more information\n", command_name);
        return 1;
    } catch (const docopt::DocoptArgumentError&) {
        fmt::print(stderr, "Error: missing command 'cbc <command>'\n");
        return 1;
    }
}
