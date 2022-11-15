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

int
main()
{
    asio::io_context io;

    asio::experimental::concurrent_channel<void(std::error_code, int)> ch{ io, 1 };

    ch.async_send(std::error_code(), 42, [](std::error_code error) {
        if (!error) {
            std::cout << "Sent "
                      << "\n";
        } else {
            std::cout << "send error " << error << ", " << error.message() << "\n";
        }
    });

    ch.async_receive([](std::error_code error, int i) {
        if (!error) {
            std::cout << "Received " << i << "\n";
        } else {
            std::cout << "receive error " << error << ", " << error.message() << "\n";
        }
    });

    io.run();

    return 0;
}
