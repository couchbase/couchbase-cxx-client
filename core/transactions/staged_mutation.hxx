/*
 *     Copyright 2021-Present Couchbase, Inc.
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

#include "attempt_context_impl.hxx"
#include "internal/utils.hxx"
#include "transaction_get_result.hxx"
#include "uid_generator.hxx"

#include <mutex>
#include <string>
#include <vector>

namespace couchbase::core::transactions
{
enum class staged_mutation_type { INSERT, REMOVE, REPLACE };

class staged_mutation
{
  private:
    transaction_get_result doc_;
    staged_mutation_type type_;
    std::vector<std::byte> content_;
    std::string operation_id_;

  public:
    template<typename Content>
    staged_mutation(transaction_get_result& doc,
                    Content content,
                    staged_mutation_type type,
                    std::string operation_id = uid_generator::next())
      : doc_(doc)
      , type_(type)
      , content_(std::move(content))
      , operation_id_(std::move(operation_id))
    {
    }

    staged_mutation(const staged_mutation& o) = default;
    staged_mutation(staged_mutation&& o) = default;
    staged_mutation& operator=(const staged_mutation& o) = default;
    staged_mutation& operator=(staged_mutation&& o) = default;

    const core::document_id& id() const
    {
        return doc_.id();
    }

    [[nodiscard]] const transaction_get_result& doc() const
    {
        return doc_;
    }

    [[nodiscard]] transaction_get_result& doc()
    {
        return doc_;
    }

    [[nodiscard]] const staged_mutation_type& type() const
    {
        return type_;
    }

    void type(staged_mutation_type& type)
    {
        type_ = type;
    }

    [[nodiscard]] const std::vector<std::byte>& content() const
    {
        return content_;
    }

    void content(const std::vector<std::byte>& content)
    {
        content_ = content;
    }

    [[nodiscard]] std::string type_as_string() const
    {
        switch (type_) {
            case staged_mutation_type::INSERT:
                return "INSERT";
            case staged_mutation_type::REMOVE:
                return "REMOVE";
            case staged_mutation_type::REPLACE:
                return "REPLACE";
        }
        throw std::runtime_error("unknown type of staged mutation");
    }

    [[nodiscard]] const std::string& operation_id() const
    {
        return operation_id_;
    }
};

class staged_mutation_queue
{
  private:
    std::mutex mutex_;
    std::vector<staged_mutation> queue_;
    void commit_doc(attempt_context_impl* ctx, staged_mutation& item, bool ambiguity_resolution_mode = false, bool cas_zero_mode = false);
    void remove_doc(attempt_context_impl* ctx, const staged_mutation& item);
    void rollback_insert(attempt_context_impl* ctx, const staged_mutation& item);
    void rollback_remove_or_replace(attempt_context_impl* ctx, const staged_mutation& item);

  public:
    bool empty();
    void add(const staged_mutation& mutation);
    void extract_to(const std::string& prefix, core::operations::mutate_in_request& req);
    void commit(attempt_context_impl* ctx);
    void rollback(attempt_context_impl* ctx);
    void iterate(std::function<void(staged_mutation&)>);
    void remove_any(const core::document_id&);

    staged_mutation* find_any(const core::document_id& id);
    staged_mutation* find_replace(const core::document_id& id);
    staged_mutation* find_insert(const core::document_id& id);
    staged_mutation* find_remove(const core::document_id& id);
};
} // namespace couchbase::core::transactions
