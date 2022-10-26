/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2021-Present Couchbase, Inc.
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

#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>

#include <iostream>

void
send_loop(int i, asio::steady_timer& timer, asio::experimental::concurrent_channel<void(std::error_code, int)>& ch)
{
    if (i < 10) {
        timer.expires_after(std::chrono::seconds(1));
        timer.async_wait([i, &timer, &ch](std::error_code error) {
            if (!error) {
                std::cout << "Sending " << i << "\n";
                ch.async_send(std::error_code(), i, [i, &timer, &ch](std::error_code error) {
                    if (!error) {
                        std::cout << "Sent " << i << "\n";
                        send_loop(i + 1, timer, ch);
                    } else {
                        std::cout << "send error " << error << ", " << error.message() << "\n";
                    }
                });
            }
        });
    } else {
        ch.close();
    }
}

void
receive_loop(asio::steady_timer& timer, asio::experimental::concurrent_channel<void(std::error_code, int)>& ch)
{
    timer.expires_after(std::chrono::seconds(3));
    timer.async_wait([&timer, &ch](std::error_code error) {
        if (error) {
            std::cout << "timer error " << error << ", " << error.message() << "\n";
            return;
        }
        ch.async_receive([&timer, &ch](std::error_code error, int i) {
            if (!error) {
                std::cout << "Received " << i << "\n";
                receive_loop(timer, ch);
                if (i == 5) {
                    std::cout << "Closing after 5"
                              << "\n";
                    ch.close();
                }
            } else {
                std::cout << "receive error " << error << ", " << error.message() << "\n";
            }
        });
    });
}

int
main()
{
    asio::io_context io;

    asio::steady_timer sender_timer{ io };
    asio::steady_timer receiver_timer{ io };
    asio::experimental::concurrent_channel<void(std::error_code, int)> ch{ io, 1 };

    send_loop(0, sender_timer, ch);
    receive_loop(receiver_timer, ch);

    io.run();

    return 0;
}
