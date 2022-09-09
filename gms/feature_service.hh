/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <unordered_map>
#include <functional>
#include <set>
#include <any>
#include "seastarx.hh"
#include "db/config.hh" // for `db::experimental_features_t`
#include "db/schema_features.hh"
#include "gms/feature.hh"

namespace db { class config; }
namespace service { class storage_service; }

namespace gms {

class feature_service;

// Disable some features additionally to the ones disabled by `db::config`.
//
// Use for tests only!
// `feature_service` recalculates features dynamically when `db::config` changes.
// This mechanism may not work correctly in tests if they use this struct to disable features.
struct custom_feature_config_for_tests {
    std::set<sstring> extra_disabled_features;
};

using namespace std::literals;

/**
 * A gossip feature tracks whether all the nodes the current one is
 * aware of support the specified feature.
 *
 * A pointer to `cql3::query_processor` can be optionally supplied
 * if the instance needs to persist enabled features in a system table.
 */
class feature_service final {
public:
    using features_changed_callback_t = noncopyable_function<void() noexcept>;

private:
    void register_feature(feature& f);
    void unregister_feature(feature& f);
    friend class feature;
    std::unordered_map<sstring, std::reference_wrapper<feature>> _registered_features;

    std::set<sstring> _disabled_features;
    utils::observer<std::vector<enum_option<db::experimental_features_t>>> _experimental_features_observer;

    std::list<features_changed_callback_t> _supported_features_change_callbacks;
public:
    explicit feature_service(db::config& cfg, custom_feature_config_for_tests custom_cfg = {});
    ~feature_service() = default;
    future<> stop();
    // Has to run inside seastar::async context
    void enable(const sstring& name);
    void enable(const std::set<std::string_view>& list);
    db::schema_features cluster_schema_features() const;

    // The set of features this node supports.
    // It is only legal to enable a feature once every node in the cluster supports it.
    //
    // The set of supported features may grow while the node is running (e.g. when node configuration is reloaded).
    // One can subscribe to notifications for this set changing using `on_supported_features_change`.
    std::set<std::string_view> supported_feature_set();

    class supported_features_change_subscription {
        friend class feature_service;
        struct impl;
        std::unique_ptr<impl> _impl;
    public:
        supported_features_change_subscription(std::unique_ptr<impl>);
        ~supported_features_change_subscription();
    };
    // Subscribe the given callback to be called whenever the set of features supported by this node grows.
    // The callback will stop being called when the subscription returned by this function is destroyed.
    //
    // The returned subscription must not outlive `feature_service`.
    supported_features_change_subscription on_supported_features_change(features_changed_callback_t);

    // Key in the 'system.scylla_local' table, that is used to
    // persist enabled features
    static constexpr const char* ENABLED_FEATURES_KEY = "enabled_features";

public:
    gms::feature user_defined_functions { *this, "UDF"sv };
    gms::feature md_sstable { *this, "MD_SSTABLE_FORMAT"sv };
    gms::feature me_sstable { *this, "ME_SSTABLE_FORMAT"sv };
    gms::feature view_virtual_columns { *this, "VIEW_VIRTUAL_COLUMNS"sv };
    gms::feature digest_insensitive_to_expiry { *this, "DIGEST_INSENSITIVE_TO_EXPIRY"sv };
    gms::feature computed_columns { *this, "COMPUTED_COLUMNS"sv };
    gms::feature cdc { *this, "CDC"sv };
    gms::feature nonfrozen_udts { *this, "NONFROZEN_UDTS"sv };
    gms::feature hinted_handoff_separate_connection { *this, "HINTED_HANDOFF_SEPARATE_CONNECTION"sv };
    gms::feature lwt { *this, "LWT"sv };
    gms::feature per_table_partitioners { *this, "PER_TABLE_PARTITIONERS"sv };
    gms::feature per_table_caching { *this, "PER_TABLE_CACHING"sv };
    gms::feature digest_for_null_values { *this, "DIGEST_FOR_NULL_VALUES"sv };
    gms::feature correct_idx_token_in_secondary_index { *this, "CORRECT_IDX_TOKEN_IN_SECONDARY_INDEX"sv };
    gms::feature alternator_streams { *this, "ALTERNATOR_STREAMS"sv };
    gms::feature alternator_ttl { *this, "ALTERNATOR_TTL"sv };
    gms::feature range_scan_data_variant { *this, "RANGE_SCAN_DATA_VARIANT"sv };
    gms::feature cdc_generations_v2 { *this, "CDC_GENERATIONS_V2"sv };
    gms::feature user_defined_aggregates { *this, "UDA"sv };
    // Historically max_result_size contained only two fields: soft_limit and
    // hard_limit. It was somehow obscure because for normal paged queries both
    // fields were equal and meant page size. For unpaged queries and reversed
    // queries soft_limit was used to warn when the size of the result exceeded
    // the soft_limit and hard_limit was used to throw when the result was
    // bigger than this hard_limit. To clean things up, we introduced the third
    // field into max_result_size. It's name is page_size. Now page_size always
    // means the size of the page while soft and hard limits are just what their
    // names suggest. They are no longer interepreted as page size. This is not
    // a backwards compatible change so this new cluster feature is used to make
    // sure the whole cluster supports the new page_size field and we can safely
    // send it to replicas.
    gms::feature separate_page_size_and_safety_limit { *this, "SEPARATE_PAGE_SIZE_AND_SAFETY_LIMIT"sv };
    // Replica is allowed to send back empty pages to coordinator on queries.
    gms::feature empty_replica_pages { *this, "EMPTY_REPLICA_PAGES"sv };
    gms::feature supports_raft_cluster_mgmt { *this, "SUPPORTS_RAFT_CLUSTER_MANAGEMENT"sv };
    gms::feature tombstone_gc_options { *this, "TOMBSTONE_GC_OPTIONS"sv };
    gms::feature parallelized_aggregation { *this, "PARALLELIZED_AGGREGATION"sv };
    gms::feature keyspace_storage_options { *this, "KEYSPACE_STORAGE_OPTIONS"sv };
    gms::feature typed_errors_in_read_rpc { *this, "TYPED_ERRORS_IN_READ_RPC"sv };
    gms::feature schema_commitlog { *this, "SCHEMA_COMMITLOG"sv };
    gms::feature uda_native_parallelized_aggregation { *this, "UDA_NATIVE_PARALLELIZED_AGGREGATION"sv };
    gms::feature aggregate_storage_options { *this, "AGGREGATE_STORAGE_OPTIONS"sv };
    gms::feature collection_indexing { *this, "COLLECTION_INDEXING"sv };

public:

    const std::unordered_map<sstring, std::reference_wrapper<feature>>& registered_features() const;

    static std::set<sstring> to_feature_set(sstring features_string);
    // Persist enabled feature in the `system.scylla_local` table under the "enabled_features" key.
    // The key itself is maintained as an `unordered_set<string>` and serialized via `to_string`
    // function to preserve readability.
    void persist_enabled_feature_info(const gms::feature& f) const;

private:
    // Called when the set of supported features changes according to `db::config&`.
    void recalculate(db::config&);
};

} // namespace gms
