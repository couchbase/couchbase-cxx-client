

#include <core/transactions/transaction_get_result.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>

namespace couchbase::transactions
{

transaction_get_result::transaction_get_result()
  : base_(std::make_shared<couchbase::core::transactions::transaction_get_result>())
{
}

auto
transaction_get_result::content() const -> const std::vector<std::byte>&
{
  return base_->content();
}
void
transaction_get_result::content(std::vector<std::byte> content)
{
  return base_->content(content);
}
void
transaction_get_result::content(std::vector<std::byte>&& content)
{
  return base_->content(content);
}
auto
transaction_get_result::key() const -> const std::string
{
  return base_->key();
}
auto
transaction_get_result::bucket() const -> const std::string
{
  return base_->bucket();
}
auto
transaction_get_result::scope() const -> const std::string
{
  return base_->scope();
}
auto
transaction_get_result::collection() const -> const std::string
{
  return base_->collection();
}
auto
transaction_get_result::cas() const -> const couchbase::cas
{
  return base_->cas();
}
} // namespace couchbase::transactions