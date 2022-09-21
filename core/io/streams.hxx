/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
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

#pragma once

#include "ip_protocol.hxx"

#include <asio.hpp>
#include <asio/ssl.hpp>

#include <functional>

namespace couchbase::core::io
{

template<typename Resolver, typename Handler>
static void
async_resolve(ip_protocol protocol, Resolver& resolver, const std::string& hostname, const std::string& service, Handler&& handler)
{

    switch (protocol) {
        case ip_protocol::force_ipv4:
            return resolver.async_resolve(asio::ip::tcp::v4(), hostname, service, std::forward<Handler>(handler));
        case ip_protocol::force_ipv6:
            return resolver.async_resolve(asio::ip::tcp::v6(), hostname, service, std::forward<Handler>(handler));
        case ip_protocol::any:
            /* fall-through */
        default:
            /* use any protocol */
            break;
    }
    return resolver.async_resolve(hostname, service, std::forward<Handler>(handler));
}

class stream_impl
{
  protected:
    asio::strand<asio::io_context::executor_type> strand_;
    bool tls_;
    std::string id_{};
    std::atomic_bool open_{ false };

  public:
    stream_impl(asio::io_context& ctx, bool is_tls)
      : strand_(asio::make_strand(ctx))
      , tls_(is_tls)
      , id_(uuid::to_string(uuid::random()))
    {
    }

    virtual ~stream_impl() = default;

    [[nodiscard]] std::string_view log_prefix() const
    {
        return tls_ ? "tls" : "plain";
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_;
    }

    [[nodiscard]] bool is_open() const
    {
        return open_;
    }

    auto get_executor() const noexcept
    {
        return strand_;
    }

    [[nodiscard]] virtual asio::ip::tcp::endpoint local_endpoint() const = 0;

    virtual void close(std::function<void(std::error_code)>&& handler) = 0;

    virtual void reopen() = 0;

    virtual void set_options() = 0;

    virtual void async_connect(const asio::ip::tcp::resolver::results_type::endpoint_type& endpoint,
                               std::function<void(std::error_code)>&& handler) = 0;

    virtual void async_write(std::vector<asio::const_buffer>& buffers, std::function<void(std::error_code, std::size_t)>&& handler) = 0;

    virtual void async_read_some(asio::mutable_buffer buffer, std::function<void(std::error_code, std::size_t)>&& handler) = 0;
};

class plain_stream_impl : public stream_impl
{
  private:
    std::shared_ptr<asio::ip::tcp::socket> stream_;

  public:
    explicit plain_stream_impl(asio::io_context& ctx)
      : stream_impl(ctx, false)
      , stream_(std::make_shared<asio::ip::tcp::socket>(strand_))
    {
    }

    [[nodiscard]] asio::ip::tcp::endpoint local_endpoint() const override
    {
        std::error_code ec;
        auto res = stream_->local_endpoint(ec);
        if (ec) {
            return {};
        }
        return res;
    }

    void close(std::function<void(std::error_code)>&& handler) override
    {
        open_ = false;
        return asio::post(strand_, [stream = stream_, h = std::move(handler)]() {
            asio::error_code ec{};
            stream->shutdown(asio::socket_base::shutdown_both, ec);
            stream->close(ec);
            h(ec);
        });
    }

    void reopen() override
    {
        return close([this](std::error_code) {
            id_ = uuid::to_string(uuid::random());
            stream_ = std::make_shared<asio::ip::tcp::socket>(strand_);
        });
    }

    void set_options() override
    {
        if (!open_ || !stream_) {
            return;
        }
        std::error_code ec{};
        stream_->set_option(asio::ip::tcp::no_delay{ true }, ec);
        stream_->set_option(asio::socket_base::keep_alive{ true }, ec);
    }

    void async_connect(const asio::ip::tcp::resolver::results_type::endpoint_type& endpoint,
                       std::function<void(std::error_code)>&& handler) override
    {
        return stream_->async_connect(endpoint, [this, h = std::move(handler)](std::error_code ec) {
            open_ = stream_->is_open();
            h(ec);
        });
    }

    void async_write(std::vector<asio::const_buffer>& buffers, std::function<void(std::error_code, std::size_t)>&& handler) override
    {
        return asio::async_write(*stream_, buffers, std::move(handler));
    }

    void async_read_some(asio::mutable_buffer buffer, std::function<void(std::error_code, std::size_t)>&& handler) override
    {
        return stream_->async_read_some(buffer, std::move(handler));
    }
};

class tls_stream_impl : public stream_impl
{
  private:
    std::shared_ptr<asio::ssl::stream<asio::ip::tcp::socket>> stream_;
    asio::ssl::context& tls_;

  public:
    tls_stream_impl(asio::io_context& ctx, asio::ssl::context& tls)
      : stream_impl(ctx, true)
      , stream_(std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(asio::ip::tcp::socket(strand_), tls))
      , tls_(tls)
    {
    }

    [[nodiscard]] asio::ip::tcp::endpoint local_endpoint() const override
    {
        std::error_code ec;
        auto res = stream_->lowest_layer().local_endpoint(ec);
        if (ec) {
            return {};
        }
        return res;
    }

    void close(std::function<void(std::error_code)>&& handler) override
    {
        open_ = false;
        return asio::post(strand_, [stream = stream_, h = std::move(handler)]() {
            asio::error_code ec{};
            stream->lowest_layer().shutdown(asio::socket_base::shutdown_both, ec);
            stream->lowest_layer().close(ec);
            h(ec);
        });
    }

    void reopen() override
    {
        return close([this](std::error_code) {
            id_ = uuid::to_string(uuid::random());
            stream_ = std::make_shared<asio::ssl::stream<asio::ip::tcp::socket>>(asio::ip::tcp::socket(strand_), tls_);
        });
    }

    void set_options() override
    {
        if (!open_ || !stream_) {
            return;
        }
        std::error_code ec{};
        stream_->lowest_layer().set_option(asio::ip::tcp::no_delay{ true }, ec);
        stream_->lowest_layer().set_option(asio::socket_base::keep_alive{ true }, ec);
    }

    void async_connect(const asio::ip::tcp::resolver::results_type::endpoint_type& endpoint,
                       std::function<void(std::error_code)>&& handler) override
    {
        return stream_->lowest_layer().async_connect(endpoint, [this, handler](std::error_code ec_connect) mutable {
            if (ec_connect == asio::error::operation_aborted) {
                return;
            }
            if (ec_connect) {
                return handler(ec_connect);
            }
            open_ = stream_->lowest_layer().is_open();
            stream_->async_handshake(asio::ssl::stream_base::client, [handler](std::error_code ec_handshake) mutable {
                if (ec_handshake == asio::error::operation_aborted) {
                    return;
                }
                return handler(ec_handshake);
            });
        });
    }

    void async_write(std::vector<asio::const_buffer>& buffers, std::function<void(std::error_code, std::size_t)>&& handler) override
    {
        return asio::async_write(*stream_, buffers, std::move(handler));
    }

    void async_read_some(asio::mutable_buffer buffer, std::function<void(std::error_code, std::size_t)>&& handler) override
    {
        return stream_->async_read_some(buffer, std::move(handler));
    }
};

} // namespace couchbase::core::io
