# Couchbase C++ Client

[![license](https://img.shields.io/github/license/couchbaselabs/couchbase-cxx-client?color=brightgreen)](https://opensource.org/licenses/Apache-2.0)
[![linters](https://img.shields.io/github/actions/workflow/status/couchbaselabs/couchbase-cxx-client/linters.yml?branch=main&label=linters)](https://github.com/couchbaselabs/couchbase-cxx-client/actions?query=workflow%3Alinters+branch%3Amain)
[![sanitizers](https://img.shields.io/github/actions/workflow/status/couchbaselabs/couchbase-cxx-client/sanitizers.yml?branch=main&label=sanitizers)](https://github.com/couchbaselabs/couchbase-cxx-client/actions?query=workflow%3Asanitizers+branch%3Amain)
[![tests](https://img.shields.io/github/actions/workflow/status/couchbaselabs/couchbase-cxx-client/tests.yml?branch=main&label=tests)](https://github.com/couchbaselabs/couchbase-cxx-client/actions?query=workflow%3Atests+branch%3Amain)

This repo is under active development and is not yet ready for release as a public SDK.

Release notes and API reference: https://github.com/couchbaselabs/couchbase-cxx-client/releases.

## Getting the Source Code

This repo uses several git submodules. If you are fetching the repo for the first time by command line, the
`--recurse-submodules` option will init the submodules recursively as well:
```shell
git clone --recurse-submodules https://github.com/couchbaselabs/couchbase-cxx-client.git
```

However, if you fetched using a simple clone command (or another IDE or tool) **you must also perform** the following
command to recursively update and initialize the submodules:
```shell
git submodule update --init --recursive
```


## Building with CMake

This repo is being built with `CMake` so everything should build easily once the basic dev dependencies are satisfied.

### Dev Dependencies

The following dependencies must be installed before the project can be built. We recommend using OS specific utilities
such as `brew`, `apt-get`, and similar package management utilities (depending on your environment).
- **cmake >= 3.20.0+** (e.g., `brew install cmake`)
- **c++ compiler >= std_17** (e.g., `xcode-select --install`)
- **openssl >= 1.1+** (e.g., `brew install openssl`)

**IMPORTANT:** On macOS, the **OpenSSL** `brew` install command mentioned above is not sufficient to be able to build.
The easiest way to fix this is to add the `OPENSSL_ROOT_DIR` env variable to your exports (e.g., `.zshenv`). If this is
not sufficient, see the other tips mentioned when you run `brew info openssl`.
```shell
export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/ 
```

### Building (command-line)
```shell
cd couchbase-cxx-client
mkdir build; cd build
cmake ..
cmake --build .
```


## Running Tests

The tests are located in the `/test` directory. More tests will be added and this directory will be organized more in
the near future to differentiate between the common tests types that might be used for different types of testing
(e.g., `unit tests`, `integration tests`, `system tests`).

### Testing (command-line)
```shell
cd build
export TEST_CONNECTION_STRING=couchbase://127.0.0.1
export TEST_USERNAME=Administrator
export TEST_PASSWORD=password
export TEST_BUCKET=default
ctest
```
