#include "test_helper.hxx"

#include "core/error_context/http.hxx"
#include "core/io/http_message.hxx"
#include "core/operations/management/collection_create.hxx"
#include "core/operations/management/collection_drop.hxx"
#include "core/operations/management/collection_update.hxx"
#include "core/operations/management/scope_create.hxx"
#include "core/operations/management/scope_drop.hxx"

#include <couchbase/error_codes.hxx>

namespace
{
auto
make_encoded(std::uint32_t status, std::string body) -> couchbase::core::io::http_response
{
  couchbase::core::io::http_response encoded{};
  encoded.status_code = status;
  encoded.body.append(body);
  return encoded;
}
} // namespace

TEST_CASE("unit: collection_create::make_response error mapping", "[unit]")
{
  using couchbase::core::operations::management::collection_create_request;
  collection_create_request req{ "default", "_default", "c1" };

  SECTION("404 'Scope with name X is not found' -> scope_not_found")
  {
    auto encoded = make_encoded(404, "Scope with name `myscope` is not found in bucket `default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::scope_not_found);
  }
  SECTION("404 'Bucket ... is not found' -> bucket_not_found (NOT scope_not_found)")
  {
    auto encoded = make_encoded(404, "Bucket with name `nope` is not found");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::bucket_not_found);
  }
  SECTION("400 'Collection with name X already exists' -> collection_exists")
  {
    auto encoded =
      make_encoded(400, "Collection with name `c1` already exists in scope `_default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::management::collection_exists);
  }
  SECTION("400 unrelated body -> invalid_argument")
  {
    auto encoded = make_encoded(400, "name - max length (251) exceeded");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::invalid_argument);
  }
}

TEST_CASE("unit: scope_drop::make_response error mapping", "[unit]")
{
  using couchbase::core::operations::management::scope_drop_request;
  scope_drop_request req{ "default", "myscope" };

  SECTION("404 'Scope with name X is not found' -> scope_not_found")
  {
    auto encoded = make_encoded(404, "Scope with name `myscope` is not found in bucket `default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::scope_not_found);
  }
  SECTION("404 'Bucket ... is not found' -> bucket_not_found (NOT scope_not_found)")
  {
    auto encoded = make_encoded(404, "Bucket with name `nope` is not found");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::bucket_not_found);
  }
}

TEST_CASE("unit: scope_create::make_response error mapping", "[unit]")
{
  using couchbase::core::operations::management::scope_create_request;
  scope_create_request req{ "default", "myscope" };

  SECTION("400 'Scope with name X already exists' -> scope_exists")
  {
    auto encoded =
      make_encoded(400, "Scope with name `myscope` already exists in bucket `default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::management::scope_exists);
  }
  SECTION("400 'Not allowed on this version of cluster' -> feature_not_available")
  {
    auto encoded = make_encoded(400, "Not allowed on this version of cluster");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::feature_not_available);
  }
  SECTION("400 unrelated body -> invalid_argument")
  {
    auto encoded = make_encoded(400, "name - max length (251) exceeded");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::invalid_argument);
  }
}

TEST_CASE("unit: collection_drop::make_response error mapping", "[unit]")
{
  using couchbase::core::operations::management::collection_drop_request;
  collection_drop_request req{ "default", "_default", "c1" };

  SECTION("404 'Collection with name' -> collection_not_found")
  {
    auto encoded = make_encoded(404, "Collection with name `c1` is not found in scope `_default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::collection_not_found);
  }
  SECTION("404 'Scope with name' -> scope_not_found")
  {
    auto encoded = make_encoded(404, "Scope with name `nope` is not found in bucket `default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::scope_not_found);
  }
  SECTION("404 'Bucket ...' -> bucket_not_found")
  {
    auto encoded = make_encoded(404, "Bucket with name `default` is not found");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::bucket_not_found);
  }
}

TEST_CASE("unit: collection_update::make_response error mapping", "[unit]")
{
  using couchbase::core::operations::management::collection_update_request;
  collection_update_request req{ "default", "_default", "c1" };

  SECTION("404 'Collection with name' -> collection_not_found")
  {
    auto encoded = make_encoded(404, "Collection with name `c1` is not found in scope `_default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::collection_not_found);
  }
  SECTION("404 'Scope with name' -> scope_not_found")
  {
    auto encoded = make_encoded(404, "Scope with name `nope` is not found in bucket `default`");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::scope_not_found);
  }
  SECTION("404 'Bucket ...' -> bucket_not_found")
  {
    auto encoded = make_encoded(404, "Bucket with name `default` is not found");
    CHECK(req.make_response({}, encoded).ctx.ec == couchbase::errc::common::bucket_not_found);
  }
}