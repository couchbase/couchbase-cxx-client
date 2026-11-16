# Couchbase C++ client

[![license](https://img.shields.io/github/license/couchbase/couchbase-cxx-client?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)
[![linters](https://img.shields.io/github/actions/workflow/status/couchbase/couchbase-cxx-client/linters.yml?branch=main&label=linters)](https://github.com/couchbase/couchbase-cxx-client/actions?query=workflow%3Alinters+branch%3Amain)
[![sanitizers](https://img.shields.io/github/actions/workflow/status/couchbase/couchbase-cxx-client/sanitizers.yml?branch=main&label=sanitizers)](https://github.com/couchbase/couchbase-cxx-client/actions?query=workflow%3Asanitizers+branch%3Amain)
[![tests](https://img.shields.io/github/actions/workflow/status/couchbase/couchbase-cxx-client/tests.yml?branch=main&label=tests)](https://github.com/couchbase/couchbase-cxx-client/actions?query=workflow%3Atests+branch%3Amain)

* Documentation and User Guides: https://docs.couchbase.com/cxx-sdk/current/hello-world/start-using-sdk.html
* API Reference: https://docs.couchbase.com/sdk-api/couchbase-cxx-client
* Issue Tracker: https://issues.couchbase.com/projects/CXXCBC/issues
* Server Documentation: https://docs.couchbase.com/home/server.html

## Using with CPM.cmake

[CMake Package Manager (CPM.cmake)](https://github.com/cpm-cmake/CPM.cmake)
makes it easy to include the library into your project. The CMake Package
Manager (CPM) simplifies dependency management. Use the following snippet to
update your `CMakeLists.txt`:

```cmake
CPMAddPackage(
  NAME
  couchbase_cxx_client
  GIT_TAG
  1.5.1
  VERSION
  1.5.1
  GITHUB_REPOSITORY
  "couchbase/couchbase-cxx-client"
  OPTIONS
  "COUCHBASE_CXX_CLIENT_STATIC_BORINGSSL ON")
```

If you install the library in the system using the `install` target or a package
management system, you can use `FindPackage`:

```cmake
cmake_minimum_required(VERSION 3.19)

project(minimal)

find_package(couchbase_cxx_client REQUIRED)

add_executable(minimal minimal.cxx)
target_link_libraries(minimal PRIVATE couchbase_cxx_client::couchbase_cxx_client)
```

## Building the project

Building requires `CMake` 3.19+ and a C++17 compiler:

```shell
git clone https://github.com/couchbase/couchbase-cxx-client.git
cd couchbase-cxx-client
cmake -S . -B build -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON
cmake --build build --parallel
```

## Running tests

Unit tests run standalone; integration tests connect to a running Couchbase
Server instance configured through environment variables:

```shell
export TEST_CONNECTION_STRING=couchbase://127.0.0.1
export TEST_USERNAME=Administrator
export TEST_PASSWORD=password
export TEST_BUCKET=default

ctest --test-dir build --output-on-failure
```

With [cbdinocluster](https://github.com/couchbaselabs/cbdinocluster), read the
connection string from the running cluster:

```shell
export TEST_CONNECTION_STRING=$(cbdinocluster connstr \
  $(cbdinocluster ps --json 2>/dev/null | jq -r '.[0].id') 2>/dev/null)
```

See [test/README.md](test/README.md) for selecting test labels (`-L unit`,
`-L integration`), running single cases, and writing new tests, and
[test/cng/README.md](test/cng/README.md) for `couchbase2://` (Cloud Native
Gateway) tests.

## Contributing

New C++ headers and sources use `.hxx` and `.cxx`; existing `.h`/`.c` and
third-party files keep their extensions. Formatting and lint rules live in
`.clang-format` and `.clang-tidy` (`snake_case` identifiers, `PascalCase`
template parameters).

`couchbase/` holds only public API headers and never includes `core/` headers
or exposes internal dependencies outside the public API surface. Core I/O is
non-blocking (Asio) and reports failures through `std::error_code` rather than
throwing across the public API. Sensitive arguments passed to `CB_LOG_*` are
wrapped at the log site with the helpers in
[core/logger/redaction.hxx](core/logger/redaction.hxx); passwords, tokens, and
private keys are never logged.

Format edited files with `clang-format -i` (checked on commits by
`./bin/check-clang-format`), run `./bin/check-log-annotations` after touching
`CB_LOG_*` statements, and run `CB_TIDY_DIFF_BASE=origin/main ./bin/check-clang-tidy`
when changing library C++ sources.
