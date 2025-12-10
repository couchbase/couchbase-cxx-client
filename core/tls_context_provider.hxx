#pragma once

#include <memory>

namespace asio
{
namespace ssl
{
class context;
} // namespace ssl
} // namespace asio

namespace couchbase::core
{

class tls_context_provider
{
public:
  tls_context_provider();

  explicit tls_context_provider(std::shared_ptr<asio::ssl::context> ctx);

  void set_ctx(std::shared_ptr<asio::ssl::context> new_ctx);
  [[nodiscard]] auto get_ctx() const -> std::shared_ptr<asio::ssl::context>;

private:
  std::shared_ptr<asio::ssl::context> ctx_;
};
} // namespace couchbase::core
