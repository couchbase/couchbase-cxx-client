/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2024. Couchbase, Inc.
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

#include <couchbase/error.hxx>
#include <couchbase/error_codes.hxx>

namespace couchbase::transactions
{

enum final_error { FAILED, EXPIRED, FAILED_POST_COMMIT, AMBIGUOUS };

enum error_class {
    FAIL_HARD = 0,
    FAIL_OTHER,
    FAIL_TRANSIENT,
    FAIL_AMBIGUOUS,
    FAIL_DOC_ALREADY_EXISTS,
    FAIL_DOC_NOT_FOUND,
    FAIL_PATH_NOT_FOUND,
    FAIL_CAS_MISMATCH,
    FAIL_WRITE_WRITE_CONFLICT,
    FAIL_ATR_FULL,
    FAIL_PATH_ALREADY_EXISTS,
    FAIL_EXPIRY
};

/**
 * This operation (such as a replace, get or insert) either failed or ambiguously succeeded.
 *
 * The details of the failure are opaque, as the application is not expected to take action on this failure.
 *
 * @internal All methods on this class are for internal use only.
 */
class transaction_operation_failed : public error
{
  public:
    transaction_operation_failed(error_class err_class,
                                          const std::string& message,
                                          bool retry,
                                          bool rollback,
                                          final_error to_raise,
                                          const error& cause)
      : error(couchbase::errc::transaction_op::transaction_op_failed, message, {}, cause)
      , error_class_(err_class)
      , retry_(retry)
      , rollback_(rollback)
      , to_raise_(to_raise)
    {
    }

    [[nodiscard]] auto rollback() const -> bool
    {
        return rollback_;
    }

    [[nodiscard]] auto retry() const -> bool
    {
        return retry_;
    }

    [[nodiscard]] auto error_class() const -> error_class
    {
        return error_class_;
    }

    [[nodiscard]] auto to_raise() const -> final_error
    {
        return to_raise_;
    }

  private:
    enum error_class error_class_;
    bool retry_;
    bool rollback_;
    final_error to_raise_;
};
} // namespace couchbase::transactions
