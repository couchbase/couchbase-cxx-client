/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-Present Couchbase, Inc.
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

/**
 * @mainpage
 *
 * @note You may read about related Couchbase software at https://docs.couchbase.com/
 *
 * @anchor start-using
 * ### Start Using
 *
 * The following example shows the most basic usage of the library. It performs the following operations:
 * * [1] connect to local cluster (location and credentials given in program arguments),
 * * [2] persists document to the collection using @ref couchbase::collection#upsert(),
 * * [3] retrieves document back with @ref couchbase::collection#get(), extracts content as generic JSON
 * value, and prints one of the fields,
 * * [4] performs N1QL query with @ref couchbase::scope#query() and prints the result.
 * * [5] closes the cluster and deallocate resources
 *
 * @snippetlineno test_integration_examples.cxx start-using
 *
 * @anchor start-transactions
 * ### Using Transactions
 *
 * Next example shows transactions API. Read more details in @ref couchbase::transactions::transactions
 *
 * * [1] connect to cluster using the given connection string and the options
 * * [2] persist three documents to the default collection of bucket "default"
 * * [3] blocking transaction
 *   * [3.1] closure argument to @ref couchbase::transactions::transactions::run method encapsulates logic, that has to be run in
 *     transaction
 *   * [3.2] get document
 *   * [3.4] replace document's content
 *   * [3.5] check the overall status of the transaction
 * * [4] asynchronous transaction
 *   * [4.1] create promise to retrieve result from the transaction
 *   * [4.2] closure argument to @ref couchbase::transactions::transactions::run method encapsulates logic, that has to be run in
 *     transaction
 *   * [4.3] get document
 *   * [4.4] replace document's content
 *   * [4.5] second closure argument to @ref couchbase::transactions::transactions::run represents transaction completion logic
 * * [5] close cluster connection
 *
 * @snippetlineno test_transaction_examples.cxx blocking-txn
 *
 * See also simple class that implements an operation in fictional game backend: @ref game_server.cxx (and its asynchronous version in @ref
 * async_game_server.cxx)
 *
 * @example minimal.cxx - minimal example of SDK, stores single document into the collection.
 *
 * @example game_server.cxx - more complicated example of using transactions (synchronous version).
 *
 * @example async_game_server.cxx - asynchronous version of the game_server.cxx.
 *
 * @example distributed_mutex.cxx - example of distributed mutex object, that backed by a document inside collection.
 *
 */
