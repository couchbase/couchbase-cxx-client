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
 * ### Start Using
 *
 * The following example shows the most basic usage of the library. It performs the following operations:
 * * connect to local cluster (location and credentials given in program arguments),
 * * in lines <a href="#l00071">70-78</a>, persists document to the collection using @ref couchbase::collection#upsert(),
 * * in lines <a href="#l00081">80-89</a>, retrieves document back with @ref couchbase::collection#get(), extracts content as generic JSON
 * value, and prints one of the fields,
 * * in lines <a href="#l00092">91-102</a>, performs N1QL query with @ref couchbase::scope#query() and prints the result.
 * * close the cluster and deallocate resources
 *
 * @snippetlineno test_integration_examples.cxx start-using
 */
