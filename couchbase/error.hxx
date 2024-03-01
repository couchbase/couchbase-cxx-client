#pragma once

#include <system_error>
#include <utility>

#include "operation_error_context.hxx"

namespace couchbase {


class error {
  public:

    explicit operator bool () const { return ec_.value() != 0; }

    error() = default;

    error(std::error_code ec,
          std::string message,
          operation_error_context  ctx)
      : ec_{ ec },
      message_{ std::move( message ) },
      ctx_{ std::move( ctx ) }
    {
    }

    error(std::error_code ec,
          std::string message,
          operation_error_context  ctx,
          couchbase::error cause)
      : ec_{ ec },
      message_{ std::move( message ) },
      ctx_{std::move( ctx )},
      cause_{ std::make_unique<couchbase::error>(std::move(cause)) }
    {
    }

    auto cause(couchbase::error cause) -> void
    {
        cause_ = std::make_unique<couchbase::error>(std::move(cause));
    }

    [[nodiscard]] auto ec() const -> std::error_code
    {
        return ec_;
    }

    [[nodiscard]] auto message() const -> std::string
    {
        return message_;
    }

    [[nodiscard]] auto ctx() const -> const operation_error_context&
    {
        return ctx_;
    }

    [[nodiscard]] auto cause() -> std::optional<std::unique_ptr<error>>
    {
        return std::move(cause_);
    }

  private:
    std::error_code ec_{};
    std::string message_{};
    operation_error_context ctx_{};
    std::optional<std::unique_ptr<error>> cause_{};
};

} // namespace couchbase
