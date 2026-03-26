# Couchbase C++ Client - Developer Guide

This document provides foundational mandates and technical guidance for the `couchbase-cxx-client` project.

## Project Overview
The Couchbase C++ Client is a high-performance, modern C++ library (C++17) for interacting with Couchbase Server. It follows a decoupled architecture separating the public API from internal implementation details.

## Technical Stack
- **Language**: C++17
- **Build System**: CMake 3.19+
- **Key Dependencies**: Asio, GSL, nlohmann/json, OpenSSL/BoringSSL (managed via `cmake/ThirdPartyDependencies.cmake`).

## Project Structure
- `couchbase/`: **Public API**. Contains only header files (`.hxx`). This is what users of the library include.
- `core/`: **Internal Implementation**. Contains both headers (`.hxx`) and source files (`.cxx`). Organized by component (io, mcbp, topology, etc.).
- `test/`: Comprehensive test suite including unit, integration, and functional tests.
- `examples/`: Code samples demonstrating library usage.
- `third_party/`: Bundled or externally fetched dependencies.
- `bin/`: Scripts for environment setup and automated testing.

## Coding Standards & Style
- **File Extensions**:
  - Preferred for new Couchbase C++ client code:
    - Headers: `.hxx`
    - Source: `.cxx`
  - Exceptions:
    - Existing C components and some internal/platform code may use `.h`/`.c`.
    - Third-party code retains its original upstream layout and extensions.
- **Formatting**: Strictly enforced via `.clang-format`.
  - `IndentWidth: 2`
  - `ColumnLimit: 100`
  - `Standard: c++17`
- **Linting**: Uses `clang-tidy` and `cpplint`. See `.clang-tidy` and `CPPLINT.cfg` for specific rules.
- **Naming**: Follows standard C++ conventions (snake_case for functions and variables).

## Development Workflow

### Building
The standard build process uses CMake:
```bash
cmake -S . -B build -DCOUCHBASE_CXX_CLIENT_STATIC_BORINGSSL=ON
cmake --build build -j$(nproc)
```

### Testing
Tests use `ctest` or direct execution of test binaries. Many tests require a running Couchbase cluster.

#### Environment Setup
To automatically fetch the connection string from a running `cbdinocluster`:
```bash
export TEST_CONNECTION_STRING=$(cbdinocluster connstr $(cbdinocluster ps --json 2>/dev/null | jq -r .[0].id) 2>/dev/null)
export TEST_USERNAME=Administrator
export TEST_PASSWORD=password
export TEST_BUCKET=default
```

#### Running Tests
- **Run full test suite**:
  ```bash
  (cd build; for i in $(find test -type f -name 'test_*' -perm -u+x); do echo "running single test suite: $i"; cmake --build . --target "$i"; ./"$i"; done)
  ```
- **List all test cases in a suite**:
  ```bash
  ./build/test/test_integration_crud --list-tests
  ```
- **Run a single test case**:
  ```bash
  (cd build; cmake --build . --target test/test_integration_crud; ./test/test_integration_crud "integration: touch")
  ```
- **Run via ctest**:
  ```bash
  cd build
  ctest --output-on-failure
  ```

### Verification
Always run linting and formatting checks before submitting changes:
- `bin/check-clang-format`
- `bin/check-clang-tidy`

## Architecture Mandates
1.  **Public/Private Separation**: Never expose internal `core/` headers in the `couchbase/` public API.
2.  **Asynchronous First**: The core is built on Asio and utilizes asynchronous patterns extensively.
3.  **Error Handling**: Uses `std::error_code` and custom error categories defined in `core/error.hxx`.
