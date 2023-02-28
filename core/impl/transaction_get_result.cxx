

#include <core/transactions/transaction_get_result.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>

namespace couchbase::transactions
{

transaction_get_result::transaction_get_result()
  : base_(std::make_shared<couchbase::core::transactions::transaction_get_result>())
{
}

const std::vector<std::byte>&
transaction_get_result::content() const
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
const std::string
transaction_get_result::key() const
{
    return base_->key();
}
const std::string
transaction_get_result::bucket() const
{
    return base_->bucket();
}
const std::string
transaction_get_result::scope() const
{
    return base_->scope();
}
const std::string
transaction_get_result::collection() const
{
    return base_->collection();
}
const couchbase::cas
transaction_get_result::cas() const
{
    return base_->cas();
}
} // namespace couchbase::transactions