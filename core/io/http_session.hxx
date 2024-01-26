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

#include "core/diagnostics.hxx"
#include "core/logger/logger.hxx"
#include "core/meta/version.hxx"
#include "core/origin.hxx"
#include "core/platform/base64.h"
#include "core/platform/uuid.h"
#include "core/utils/movable_function.hxx"
#include "http_context.hxx"
#include "http_message.hxx"
#include "http_parser.hxx"
#include "streams.hxx"

#include <couchbase/error_codes.hxx>

#include <spdlog/fmt/bin_to_hex.h>

#include <asio.hpp>

#include <list>
#include <memory>
#include <utility>

namespace couchbase::core::io
{
class http_session_info
{
  public:
    http_session_info(const std::string& client_id, const std::string& session_id)
      : log_prefix_(fmt::format("[{}/{}]", client_id, session_id))
    {
    }

    http_session_info(const std::string& client_id,
                      const std::string& session_id,
                      const asio::ip::tcp::endpoint& local_endpoint,
                      const asio::ip::tcp::endpoint& remote_endpoint)
    {
        local_endpoint_ = local_endpoint;
        local_endpoint_address_ = local_endpoint_.address().to_string();
        if (local_endpoint_.protocol() == asio::ip::tcp::v6()) {
            local_endpoint_address_ = fmt::format("[{}]:{}", local_endpoint_address_, local_endpoint_.port());
        } else {
            local_endpoint_address_ = fmt::format("{}:{}", local_endpoint_address_, local_endpoint_.port());
        }

        remote_endpoint_ = remote_endpoint;
        remote_endpoint_address_ = remote_endpoint_.address().to_string();
        if (remote_endpoint_.protocol() == asio::ip::tcp::v6()) {
            remote_endpoint_address_ = fmt::format("[{}]:{}", remote_endpoint_address_, remote_endpoint_.port());
        } else {
            remote_endpoint_address_ = fmt::format("{}:{}", remote_endpoint_address_, remote_endpoint_.port());
        }

        log_prefix_ =
          fmt::format("[{}/{}] <{}:{}>", client_id, session_id, remote_endpoint_.address().to_string(), remote_endpoint_.port());
    }

    [[nodiscard]] const asio::ip::tcp::endpoint& remote_endpoint() const
    {
        return remote_endpoint_;
    }

    [[nodiscard]] const std::string& remote_address() const
    {
        return remote_endpoint_address_;
    }

    [[nodiscard]] const asio::ip::tcp::endpoint& local_endpoint() const
    {
        return local_endpoint_;
    }

    [[nodiscard]] const std::string& local_address() const
    {
        return local_endpoint_address_;
    }

    [[nodiscard]] const std::string& log_prefix() const
    {
        return log_prefix_;
    }

  private:
    std::string log_prefix_;
    asio::ip::tcp::endpoint remote_endpoint_{}; // connected endpoint
    std::string remote_endpoint_address_{};     // cached string with endpoint address
    asio::ip::tcp::endpoint local_endpoint_{};
    std::string local_endpoint_address_{};
};

class http_session : public std::enable_shared_from_this<http_session>
{
  public:
    http_session(service_type type,
                 const std::string& client_id,
                 asio::io_context& ctx,
                 const cluster_credentials& credentials,
                 const std::string& hostname,
                 const std::string& service,
                 http_context http_ctx)
      : type_(type)
      , client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<plain_stream_impl>(ctx_))
      , deadline_timer_(stream_->get_executor())
      , idle_timer_(stream_->get_executor())
      , credentials_(credentials)
      , hostname_(hostname)
      , service_(service)
      , user_agent_(meta::user_agent_for_http(client_id_, id_, http_ctx.options.user_agent_extra))
      , info_(client_id_, id_)
      , http_ctx_(std::move(http_ctx))
    {
    }

    http_session(service_type type,
                 const std::string& client_id,
                 asio::io_context& ctx,
                 asio::ssl::context& tls,
                 const cluster_credentials& credentials,
                 const std::string& hostname,
                 const std::string& service,
                 http_context http_ctx)
      : type_(type)
      , client_id_(client_id)
      , id_(uuid::to_string(uuid::random()))
      , ctx_(ctx)
      , resolver_(ctx_)
      , stream_(std::make_unique<tls_stream_impl>(ctx_, tls))
      , deadline_timer_(ctx_)
      , idle_timer_(ctx_)
      , credentials_(credentials)
      , hostname_(hostname)
      , service_(service)
      , user_agent_(meta::user_agent_for_http(client_id_, id_, http_ctx.options.user_agent_extra))
      , info_(client_id_, id_)
      , http_ctx_(std::move(http_ctx))
    {
    }

    ~http_session()
    {
        stop();
    }

    auto get_executor() const
    {
        return stream_->get_executor();
    }

    [[nodiscard]] couchbase::core::http_context& http_context()
    {
        return http_ctx_;
    }

    [[nodiscard]] std::string remote_address()
    {
        std::scoped_lock lock(info_mutex_);
        return info_.remote_address();
    }

    [[nodiscard]] std::string local_address()
    {
        std::scoped_lock lock(info_mutex_);
        return info_.local_address();
    }

    [[nodiscard]] diag::endpoint_diag_info diag_info()
    {
        return { type_,
                 id_,
                 last_active_.time_since_epoch().count() == 0 ? std::nullopt
                                                              : std::make_optional(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                  std::chrono::steady_clock::now() - last_active_)),
                 remote_address(),
                 local_address(),
                 state_ };
    }

    void start()
    {
        state_ = diag::endpoint_state::connecting;
        async_resolve(http_ctx_.options.use_ip_protocol,
                      resolver_,
                      hostname_,
                      service_,
                      std::bind(&http_session::on_resolve, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
    }

    [[nodiscard]] std::string log_prefix()
    {
        std::scoped_lock lock(info_mutex_);
        return info_.log_prefix();
    }

    [[nodiscard]] const std::string& id() const
    {
        return id_;
    }

    [[nodiscard]] service_type type() const
    {
        return type_;
    }

    [[nodiscard]] const std::string& hostname() const
    {
        return hostname_;
    }

    [[nodiscard]] const std::string& port() const
    {
        return service_;
    }

    [[nodiscard]] const asio::ip::tcp::endpoint& endpoint()
    {
        std::scoped_lock lock(info_mutex_);
        return info_.remote_endpoint();
    }

    void on_stop(std::function<void()> handler)
    {
        on_stop_handler_ = std::move(handler);
    }

    void stop()
    {
        if (stopped_) {
            return;
        }
        stopped_ = true;
        state_ = diag::endpoint_state::disconnecting;
        stream_->close([](std::error_code) {});
        deadline_timer_.cancel();
        idle_timer_.cancel();

        {
            std::scoped_lock lock(current_response_mutex_);
            auto ctx = std::move(current_response_);
            if (ctx.handler) {
                ctx.handler(errc::common::ambiguous_timeout, {});
            }
        }

        if (auto handler = std::move(on_stop_handler_); handler) {
            handler();
        }
        state_ = diag::endpoint_state::disconnected;
    }

    bool keep_alive() const
    {
        return keep_alive_;
    }

    bool is_stopped() const
    {
        return stopped_;
    }

    void write(const std::vector<std::uint8_t>& buf)
    {
        if (stopped_) {
            return;
        }
        std::scoped_lock lock(output_buffer_mutex_);
        output_buffer_.push_back(buf);
    }

    void write(const std::string_view& buf)
    {
        if (stopped_) {
            return;
        }
        std::scoped_lock lock(output_buffer_mutex_);
        output_buffer_.emplace_back(buf.begin(), buf.end());
    }

    void flush()
    {
        if (!connected_) {
            return;
        }
        if (stopped_) {
            return;
        }
        asio::post(asio::bind_executor(ctx_, [self = shared_from_this()]() { self->do_write(); }));
    }

    template<typename Handler>
    void write_and_subscribe(io::http_request& request, Handler&& handler)
    {
        if (stopped_) {
            return;
        }
        {
            response_context ctx{ std::forward<Handler>(handler) };
            if (request.streaming) {
                ctx.parser.response.body.use_json_streaming(std::move(request.streaming.value()));
            }
            std::scoped_lock lock(current_response_mutex_);
            std::swap(current_response_, ctx);
        }
        if (request.headers["connection"] == "keep-alive") {
            keep_alive_ = true;
        }
        request.headers["user-agent"] = user_agent_;
        auto credentials = fmt::format("{}:{}", credentials_.username, credentials_.password);
        request.headers["authorization"] =
          fmt::format("Basic {}", base64::encode(gsl::as_bytes(gsl::span{ credentials.data(), credentials.size() })));
        write(fmt::format("{} {} HTTP/1.1\r\nhost: {}:{}\r\n", request.method, request.path, hostname_, service_));
        if (!request.body.empty()) {
            request.headers["content-length"] = std::to_string(request.body.size());
        }
        for (const auto& [name, value] : request.headers) {
            write(fmt::format("{}: {}\r\n", name, value));
        }
        write("\r\n");
        write(request.body);
        flush();
    }

    void set_idle(std::chrono::milliseconds timeout)
    {
        idle_timer_.expires_after(timeout);
        return idle_timer_.async_wait([self = shared_from_this()](std::error_code ec) {
            if (ec == asio::error::operation_aborted) {
                return;
            }
            self->stop();
        });
    }

    void reset_idle()
    {
        idle_timer_.cancel();
    }

  private:
    struct response_context {
        utils::movable_function<void(std::error_code, io::http_response&&)> handler{};
        http_parser parser{};
    };

    void on_resolve(std::error_code ec, const asio::ip::tcp::resolver::results_type& endpoints)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        if (ec) {
            CB_LOG_ERROR("{} error on resolve \"{}:{}\": {}", info_.log_prefix(), hostname_, service_, ec.message());
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        endpoints_ = endpoints;
        CB_LOG_TRACE("{} resolved \"{}:{}\" to {} endpoint(s)", info_.log_prefix(), hostname_, service_, endpoints_.size());
        do_connect(endpoints_.begin());
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_connect(asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (stopped_) {
            return;
        }
        if (it != endpoints_.end()) {
            CB_LOG_DEBUG("{} connecting to {}:{} (\"{}:{}\"), timeout={}ms",
                         info_.log_prefix(),
                         it->endpoint().address().to_string(),
                         it->endpoint().port(),
                         hostname_,
                         service_,
                         http_ctx_.options.connect_timeout.count());
            deadline_timer_.expires_after(http_ctx_.options.connect_timeout);
            stream_->async_connect(it->endpoint(), std::bind(&http_session::on_connect, shared_from_this(), std::placeholders::_1, it));
        } else {
            CB_LOG_ERROR("{} no more endpoints left to connect, \"{}:{}\" is not reachable", info_.log_prefix(), hostname_, service_);
            stop();
        }
    }

    void on_connect(const std::error_code& ec, asio::ip::tcp::resolver::results_type::iterator it)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        last_active_ = std::chrono::steady_clock::now();
        if (!stream_->is_open() || ec) {
            CB_LOG_WARNING("{} unable to connect to {}:{}: {}{}",
                           info_.log_prefix(),
                           it->endpoint().address().to_string(),
                           it->endpoint().port(),
                           ec.message(),
                           (ec == asio::error::connection_refused) ? ", check server ports and cluster encryption setting" : "");
            if (stream_->is_open()) {
                stream_->close(std::bind(&http_session::do_connect, shared_from_this(), ++it));
            } else {
                do_connect(++it);
            }
        } else {
            state_ = diag::endpoint_state::connected;
            connected_ = true;
            CB_LOG_DEBUG("{} connected to {}:{}", info_.log_prefix(), it->endpoint().address().to_string(), it->endpoint().port());
            {
                std::scoped_lock lock(info_mutex_);
                info_ = http_session_info(client_id_, id_, stream_->local_endpoint(), it->endpoint());
            }
            deadline_timer_.cancel();
            flush();
        }
    }

    void check_deadline(std::error_code ec)
    {
        if (ec == asio::error::operation_aborted || stopped_) {
            return;
        }
        if (deadline_timer_.expiry() <= asio::steady_timer::clock_type::now()) {
            stream_->close([](std::error_code) {});
            deadline_timer_.cancel();
            return;
        }
        deadline_timer_.async_wait(std::bind(&http_session::check_deadline, shared_from_this(), std::placeholders::_1));
    }

    void do_read()
    {
        if (stopped_ || reading_ || !stream_->is_open()) {
            return;
        }
        reading_ = true;
        stream_->async_read_some(
          asio::buffer(input_buffer_), [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
              if (ec == asio::error::operation_aborted || self->stopped_) {
                  CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}",
                                  self->type_,
                                  self->info_.remote_address(),
                                  ec ? ec.message() : "ok",
                                  bytes_transferred);
                  return;
              } else {
                  CB_LOG_PROTOCOL("[HTTP, IN] type={}, host=\"{}\", rc={}, bytes_received={}{:a}",
                                  self->type_,
                                  self->info_.remote_address(),
                                  ec ? ec.message() : "ok",
                                  bytes_transferred,
                                  spdlog::to_hex(self->input_buffer_.data(),
                                                 self->input_buffer_.data() + static_cast<std::ptrdiff_t>(bytes_transferred)));
              }
              self->last_active_ = std::chrono::steady_clock::now();
              if (ec) {
                  CB_LOG_ERROR("{} IO error while reading from the socket: {}", self->info_.log_prefix(), ec.message());
                  return self->stop();
              }

              http_parser::feeding_result res{};
              {
                  std::scoped_lock lock(self->current_response_mutex_);
                  res = self->current_response_.parser.feed(reinterpret_cast<const char*>(self->input_buffer_.data()), bytes_transferred);
              }
              if (res.failure) {
                  return self->stop();
              }
              if (res.complete) {
                  response_context ctx{};
                  {
                      std::scoped_lock lock(self->current_response_mutex_);
                      std::swap(self->current_response_, ctx);
                  }
                  if (ctx.parser.response.must_close_connection()) {
                      self->keep_alive_ = false;
                  }
                  ctx.handler({}, std::move(ctx.parser.response));
                  self->reading_ = false;
                  return;
              }
              self->reading_ = false;
              return self->do_read();
          });
    }

    void do_write()
    {
        if (stopped_) {
            return;
        }
        std::scoped_lock lock(writing_buffer_mutex_, output_buffer_mutex_);
        if (!writing_buffer_.empty() || output_buffer_.empty()) {
            return;
        }
        std::swap(writing_buffer_, output_buffer_);
        std::vector<asio::const_buffer> buffers;
        buffers.reserve(writing_buffer_.size());
        for (auto& buf : writing_buffer_) {
            CB_LOG_PROTOCOL(
              "[HTTP, OUT] type={}, host=\"{}\", buffer_size={}{:a}", type_, info_.remote_address(), buf.size(), spdlog::to_hex(buf));
            buffers.emplace_back(asio::buffer(buf));
        }
        stream_->async_write(buffers, [self = shared_from_this()](std::error_code ec, std::size_t bytes_transferred) {
            CB_LOG_PROTOCOL("[HTTP, OUT] type={}, host=\"{}\", rc={}, bytes_sent={}",
                            self->type_,
                            self->info_.remote_address(),
                            ec ? ec.message() : "ok",
                            bytes_transferred);
            if (ec == asio::error::operation_aborted || self->stopped_) {
                return;
            }
            self->last_active_ = std::chrono::steady_clock::now();
            if (ec) {
                CB_LOG_ERROR("{} IO error while writing to the socket: {}", self->info_.log_prefix(), ec.message());
                return self->stop();
            }
            {
                std::scoped_lock inner_lock(self->writing_buffer_mutex_);
                self->writing_buffer_.clear();
            }
            bool want_write = false;
            {
                std::scoped_lock inner_lock(self->output_buffer_mutex_);
                want_write = !self->output_buffer_.empty();
            }
            if (want_write) {
                return self->do_write();
            }
            self->do_read();
        });
    }

    service_type type_;
    std::string client_id_;
    std::string id_;
    asio::io_context& ctx_;
    asio::ip::tcp::resolver resolver_;
    std::unique_ptr<stream_impl> stream_;
    asio::steady_timer deadline_timer_;
    asio::steady_timer idle_timer_;

    cluster_credentials credentials_;
    std::string hostname_;
    std::string service_;
    std::string user_agent_;

    std::atomic_bool stopped_{ false };
    std::atomic_bool connected_{ false };
    std::atomic_bool keep_alive_{ false };
    std::atomic_bool reading_{ false };

    std::function<void()> on_stop_handler_{ nullptr };

    response_context current_response_{};
    std::mutex current_response_mutex_{};

    std::array<std::uint8_t, 16384> input_buffer_{};
    std::vector<std::vector<std::uint8_t>> output_buffer_{};
    std::vector<std::vector<std::uint8_t>> writing_buffer_{};
    std::mutex output_buffer_mutex_{};
    std::mutex writing_buffer_mutex_{};
    asio::ip::tcp::resolver::results_type endpoints_{};
    http_session_info info_;
    std::mutex info_mutex_{};
    couchbase::core::http_context http_ctx_;

    std::chrono::time_point<std::chrono::steady_clock> last_active_{};
    diag::endpoint_state state_{ diag::endpoint_state::disconnected };
};
} // namespace couchbase::core::io
