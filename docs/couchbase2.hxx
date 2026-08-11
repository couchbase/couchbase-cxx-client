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
 * @page couchbase2 The couchbase2:// transport
 * @brief Connecting through a Cloud Native Gateway instead of directly to cluster nodes.
 *
 * A connection string with the @c couchbase2:// scheme routes every operation over gRPC to a single
 * Cloud Native Gateway (CNG) endpoint, rather than opening MCBP and HTTP connections to each node
 * of a cluster. The gateway performs the routing the library would otherwise do itself.
 *
 * The public API is the same one used for @c couchbase:// and @c couchbases://. No type or method
 * is specific to this transport; what changes is which operations a cluster can serve, and how a
 * connection string is written.
 *
 * @warning The transport as a whole carries the stability level of this page. Individual API
 * entities keep their own stability, which this page does not lower: @ref couchbase::collection#get
 * is @ref stability_committed "committed" whether it runs over @c couchbase:// or @c couchbase2://.
 * What is uncommitted is the behaviour of running it over this transport at all.
 *
 * ### Enabling it
 *
 * Support is a compile-time option and is @b off by default. A library built without
 * @c COUCHBASE_CXX_CLIENT_BUILD_COUCHBASE2 refuses a @c couchbase2:// connection string, reporting
 * @ref couchbase::errc::common::feature_not_available from @ref couchbase::cluster#connect rather
 * than attempting a handshake the gateway cannot answer.
 *
 * ### Writing the connection string
 *
 * The scheme implies TLS: a @c couchbase2:// connection is always encrypted, and the default port
 * is 18098. Neither is inferred from the scheme name, so an endpoint published on another port must
 * say so explicitly, and a private certificate authority must be trusted as it would be for
 * @c couchbases://.
 *
 * Two further rules apply to @c couchbase2:// that do not apply to the other schemes:
 *
 * * Exactly one host. The gateway fronts the whole cluster, so there is no node list to rotate
 *   through. A string naming several hosts still connects, to a single one of them: the bootstrap
 *   list is shuffled unless @c preserve_bootstrap_nodes_order is set, so which one is not
 *   predictable from the order they were written in. The log records the endpoint chosen and that
 *   the rest were ignored.
 * * No bootstrap-mode suffix on the host, since the gateway uses neither MCBP nor HTTP config
 *   bootstrap. A suffix is ignored.
 *
 * ### Operations the library refuses
 *
 * These report @ref couchbase::errc::common::feature_not_available without reaching the gateway,
 * because the transport has no way to carry them:
 *
 * * @ref couchbase::cluster#ping, @ref couchbase::bucket#ping and
 *   @ref couchbase::cluster#diagnostics -- there are no per-node or per-bucket sessions to probe.
 * * @ref couchbase::collection#get_all_replicas, @ref couchbase::collection#get_any_replica,
 *   @ref couchbase::collection#lookup_in_all_replicas and
 *   @ref couchbase::collection#lookup_in_any_replica.
 * * Subdocument operations against the active node, @ref couchbase::collection#lookup_in and
 *   @ref couchbase::collection#mutate_in.
 * * Requests carrying an option or a value the transport has no equivalent for, across query,
 *   analytics, search, views and management -- a memcached bucket, for instance. The refusal
 *   depends on what the request carries rather than on the operation, so the same call may be
 *   served or refused according to how it was built.
 *
 * ### Operations that fail without being refused
 *
 * Two things are not served either, but do not report
 * @ref couchbase::errc::common::feature_not_available and are not turned away before work begins.
 * Both need the bucket configuration and the per-node connections this transport does not open, so
 * they fail on that path instead, and the error describes that failure rather than the missing
 * feature.
 *
 * The first is @ref couchbase::collection#scan.
 *
 * The second is poll-based durability, and it is the one to know about, because the mutation is
 * already applied by the time it fails.
 *
 * It is requested by passing @ref couchbase::persist_to and @ref couchbase::replicate_to to
 * @ref couchbase::common_durability_options. The mutation is sent to the gateway and applied, and
 * only the polling that follows fails, so the document is already written when the error arrives
 * and the durability it asked for has not been established.
 *
 * Level-based durability, requested with a @ref couchbase::durability_level, is served normally and
 * is the durability to use over this transport.
 *
 * Anything not listed above is sent to the gateway, which decides whether it can serve it. An
 * operation the gateway does not implement is reported in the gateway's own terms, so the set of
 * working operations depends on the gateway version as well as on this library.
 *
 * @cng_since{1.4.0}
 * @cng_uncommitted
 */
