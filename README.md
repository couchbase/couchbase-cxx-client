## Build

```text
git clone --recurse-submodules git@github.com:couchbaselabs/couchbase-cxx-client.git
cd couchbase-cxx-client
mkdir build
cd build
cmake ..
make -j8
```

## Test

```
cd build
export TEST_CONNECTION_STRING=couchbase://127.0.0.1
export TEST_USERNAME=Administrator
export TEST_PASSWORD=password
export TEST_BUCKET=default
ctest
```
