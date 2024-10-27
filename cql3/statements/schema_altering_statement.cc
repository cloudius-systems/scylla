/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include <tuple>
#include "cql3/statements/schema_altering_statement.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "service/raft/raft_group0_client.hh"
#include "replica/database.hh"

namespace cql3 {

namespace statements {

static logging::logger logger("schema_altering_statement");

schema_altering_statement::schema_altering_statement(timeout_config_selector timeout_selector)
    : cf_statement(cf_name())
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{false} {
}

schema_altering_statement::schema_altering_statement(cf_name name, timeout_config_selector timeout_selector)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{true} {
}

bool schema_altering_statement::needs_guard(query_processor&, service::query_state&) const {
    return true;
}

future<> schema_altering_statement::grant_permissions_to_creator(const service::client_state&, service::group0_batch&) const {
    return make_ready_future<>();
}

bool schema_altering_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return false;
}

uint32_t schema_altering_statement::get_bound_terms() const
{
    return 0;
}

void schema_altering_statement::prepare_keyspace(const service::client_state& state)
{
    if (_is_column_family_level) {
        cf_statement::prepare_keyspace(state);
    }
}

future<service::group0_guard>
schema_altering_statement::prepare_to_execute(query_processor&, std::optional<service::group0_guard> guard) const {
    // guard is expected to always be engaged
    return make_ready_future<service::group0_guard>(std::move(*guard));
}

future<> schema_altering_statement::cleanup_after_execute(query_processor& qp) const {
    return make_ready_future();
}

future<service::group0_guard>
drop_statement::prepare_to_execute(query_processor& qp, std::optional<service::group0_guard> guard) const {
    if (qp.db().has_keyspace(keyspace()) && qp.db().real_database().find_keyspace(keyspace()).uses_tablets() &&
        qp.db().real_database().get_token_metadata().tablets().balancing_enabled()) {
        // Hold table migrations and wait for topology not busy
        // to prevent race between tablet migrations and dropping tables
        // FIXME: abort on-going migrations of the affected tables
        // and restrict waiting only on the dropped tables.
        _disabled_tablet_balancing = true;
        co_return co_await qp.set_tablet_balancing_enabled(false, std::move(guard));
    }
    co_return std::move(*guard);
}

future<>
drop_statement::cleanup_after_execute(query_processor& qp) const {
    if (_disabled_tablet_balancing) {
        co_await qp.set_tablet_balancing_enabled(true);
    }
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    bool internal = state.get_client_state().is_internal();
    if (internal) {
        auto replication_type = locator::replication_strategy_type::everywhere_topology;
        data_dictionary::database db = qp.db();
        if (_cf_name && _cf_name->has_keyspace()) {
           const auto& ks = db.find_keyspace(_cf_name->get_keyspace());
           replication_type = ks.get_replication_strategy().get_type();
        }
        if (replication_type != locator::replication_strategy_type::local) {
            sstring info = _cf_name ? _cf_name->to_string() : "schema";
            throw std::logic_error(format("Attempted to modify {} via internal query: such schema changes are not propagated and thus illegal", info));
        }
    }
    guard = co_await prepare_to_execute(qp, std::move(guard));
    service::group0_batch mc{std::move(guard)};
    auto result = co_await qp.execute_schema_statement(*this, state, options, mc);
    co_await qp.announce_schema_statement(*this, mc);
    co_await cleanup_after_execute(qp);
    co_return std::move(result);
}

future<std::tuple<::shared_ptr<schema_altering_statement::event_t>, std::vector<mutation>, cql3::cql_warnings_vec>> schema_altering_statement::prepare_schema_mutations(query_processor& qp, const query_options& options, api::timestamp_type) const {
    // derived class must implement one of prepare_schema_mutations overloads
    on_internal_error(logger, "not implemented");
    co_return std::make_tuple(::shared_ptr<event_t>(nullptr), std::vector<mutation>{}, cql3::cql_warnings_vec{});
}

future<std::tuple<::shared_ptr<schema_altering_statement::event_t>, cql3::cql_warnings_vec>> schema_altering_statement::prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const {
    auto [ret, muts, cql_warnings] = co_await prepare_schema_mutations(qp, options, mc.write_timestamp());
    mc.add_mutations(std::move(muts));
    co_return std::make_tuple(ret, cql_warnings);
}

}

}
