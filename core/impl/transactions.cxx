/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2026-Present Couchbase, Inc.
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

#include <couchbase/cluster.hxx>
#include <couchbase/transactions.hxx>

#include "core/impl/error.hxx"
#include "core/transactions.hxx"
#include "core/transactions/attempt_context_impl.hxx"
#include "core/transactions/exceptions.hxx"

#include <spdlog/fmt/bundled/format.h>

#include <couchbase/fmt/error.hxx>

namespace couchbase::transactions
{
void
maybe_throw(error err)
{
  if (err && err.ec() != errc::transaction_op::transaction_op_failed) {
    // We intentionally don't handle transaction_op_failed here, as we must have cached the
    // transaction error internally already, which has the full context with the right error class
    // etc.
    if (err.ec().category() == core::impl::transaction_op_category()) {
      throw core::transactions::op_exception(std::move(err));
    }
    throw std::system_error(err.ec(), fmt::format("{}", err));
  }
}

class transactions_impl
{
public:
  explicit transactions_impl(std::shared_ptr<core::transactions::transactions> core_txns)
    : core_txns_{ std::move(core_txns) }
  {
  }

  auto run(txn_logic&& logic, const transaction_options& options) const
    -> std::pair<error, transaction_result>
  {
    try {
      auto res = core_txns_->run(options, [logic = std::move(logic)](auto ctx) mutable {
        const auto err =
          logic(std::static_pointer_cast<core::transactions::attempt_context_impl>(ctx));
        maybe_throw(std::move(err));
      });
      return { {}, res };
    } catch (const core::transactions::transaction_exception& e) {
      auto [err_ctx, res_from_exc] = e.get_transaction_result();
      return std::make_pair(core::impl::make_error(err_ctx), std::move(res_from_exc));
    }
  }

  void run(async_txn_logic&& logic,
           async_txn_complete_logic&& callback,
           const transaction_options& options) const
  {
    core_txns_->run(
      options,
      [logic = std::move(logic)](auto ctx) mutable {
        auto err = logic(std::static_pointer_cast<core::transactions::attempt_context_impl>(ctx));
        maybe_throw(std::move(err));
      },
      [callback = std::move(callback)](auto exc, auto res) mutable {
        if (exc.has_value()) {
          auto [err_ctx, res_from_exc] = exc.value().get_transaction_result();
          return callback(core::impl::make_error(err_ctx), std::move(res_from_exc));
        }
        return callback({}, std::move(res.value()));
      });
  }

  [[nodiscard]] auto core() const -> std::shared_ptr<core::transactions::transactions>
  {
    return core_txns_;
  }

private:
  std::shared_ptr<core::transactions::transactions> core_txns_;
};

transactions::transactions(std::shared_ptr<core::transactions::transactions> core_txns)
  : impl_{ std::make_shared<transactions_impl>(std::move(core_txns)) }
{
}

auto
transactions::run(txn_logic&& logic, const transaction_options& cfg)
  -> std::pair<error, transaction_result>
{
  return impl_->run(std::move(logic), cfg);
}

void
transactions::run(async_txn_logic&& logic,
                  async_txn_complete_logic&& complete_callback,
                  const transaction_options& cfg)
{
  return impl_->run(std::move(logic), std::move(complete_callback), cfg);
}
} // namespace couchbase::transactions

namespace couchbase::core::transactions
{
auto
get_core_transactions(const std::shared_ptr<couchbase::transactions::transactions>& transactions)
  -> std::shared_ptr<core::transactions::transactions>
{
  return reinterpret_cast<const std::shared_ptr<couchbase::transactions::transactions_impl>*>(
           transactions.get())
    ->get()
    ->core();
}
} // namespace couchbase::core::transactions
