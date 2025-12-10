#include "tls_context_provider.hxx"

#include <asio/ssl/context.hpp>

namespace couchbase::core
{

tls_context_provider::tls_context_provider()
  : ctx_(std::make_shared<asio::ssl::context>(asio::ssl::context::tls_client))
{
}

tls_context_provider::tls_context_provider(std::shared_ptr<asio::ssl::context> ctx)
  : ctx_(std::move(ctx))
{
}

auto
tls_context_provider::get_ctx() const -> std::shared_ptr<asio::ssl::context>
{
  return std::atomic_load(&ctx_);
}

void
tls_context_provider::set_ctx(std::shared_ptr<asio::ssl::context> new_ctx)
{
  std::atomic_store(&ctx_, std::move(new_ctx));
}
} // namespace couchbase::core
