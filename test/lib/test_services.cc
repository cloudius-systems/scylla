/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "dht/i_partitioner.hh"
#include "gms/feature_service.hh"
#include "repair/row_level.hh"

dht::token create_token_from_key(const dht::i_partitioner& partitioner, sstring key) {
    sstables::key_view key_view = sstables::key_view(bytes_view(reinterpret_cast<const signed char*>(key.c_str()), key.size()));
    dht::token token = partitioner.get_token(key_view);
    assert(token == partitioner.get_token(key_view));
    return token;
}

range<dht::token> create_token_range_from_keys(const dht::sharder& sinfo, const dht::i_partitioner& partitioner, sstring start_key, sstring end_key) {
    dht::token start = create_token_from_key(partitioner, start_key);
    assert(this_shard_id() == sinfo.shard_of(start));
    dht::token end = create_token_from_key(partitioner, end_key);
    assert(this_shard_id() == sinfo.shard_of(end));
    assert(end >= start);
    return range<dht::token>::make(start, end);
}

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

table_for_tests::data::data()
    : semaphore(reader_concurrency_semaphore::no_limits{}, "table_for_tests")
{ }

table_for_tests::data::~data() {}

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager)
    : table_for_tests(
        sstables_manager,
        schema_builder(some_keyspace, some_column_family)
            .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
            .build()
    )
{ }

class table_for_tests::table_state : public compaction::table_state {
    table_for_tests::data& _data;
    sstables::sstables_manager& _sstables_manager;
    std::vector<sstables::shared_sstable> _compacted_undeleted;
    tombstone_gc_state _tombstone_gc_state;
    mutable compaction_backlog_tracker _backlog_tracker;
private:
    replica::table& table() const noexcept {
        return *_data.cf;
    }
public:
    explicit table_state(table_for_tests::data& data, sstables::sstables_manager& sstables_manager)
            : _data(data)
            , _sstables_manager(sstables_manager)
            , _tombstone_gc_state(nullptr)
            , _backlog_tracker(get_compaction_strategy().make_backlog_tracker())
    {
    }
    const schema_ptr& schema() const noexcept override {
        return table().schema();
    }
    unsigned min_compaction_threshold() const noexcept override {
        return schema()->min_compaction_threshold();
    }
    bool compaction_enforce_min_threshold() const noexcept override {
        return true;
    }
    const sstables::sstable_set& main_sstable_set() const override {
        return table().as_table_state().main_sstable_set();
    }
    const sstables::sstable_set& maintenance_sstable_set() const override {
        return table().as_table_state().maintenance_sstable_set();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point query_time) const override {
        return sstables::get_fully_expired_sstables(*this, sstables, query_time);
    }
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override {
        return _compacted_undeleted;
    }
    sstables::compaction_strategy& get_compaction_strategy() const noexcept override {
        return table().get_compaction_strategy();
    }
    reader_permit make_compaction_reader_permit() const override {
        return _data.semaphore.make_tracking_only_permit(schema(), "table_for_tests::table_state", db::no_timeout);
    }
    sstables::sstables_manager& get_sstables_manager() noexcept override {
        return _sstables_manager;
    }
    sstables::shared_sstable make_sstable() const override {
        return table().make_sstable();
    }
    sstables::sstable_writer_config configure_writer(sstring origin) const override {
        return _sstables_manager.configure_writer(std::move(origin));
    }

    api::timestamp_type min_memtable_timestamp() const override {
        return table().min_memtable_timestamp();
    }
    future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        return table().as_table_state().on_compaction_completion(std::move(desc), offstrategy);
    }
    bool is_auto_compaction_disabled_by_user() const noexcept override {
        return table().is_auto_compaction_disabled_by_user();
    }
    const tombstone_gc_state& get_tombstone_gc_state() const noexcept override {
        return _tombstone_gc_state;
    }
    compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s, std::optional<sstring> datadir)
    : _data(make_lw_shared<data>())
{
    _data->s = s;
    _data->cfg = replica::table::config{.compaction_concurrency_semaphore = &_data->semaphore};
    _data->cfg.enable_disk_writes = bool(datadir);
    _data->cfg.datadir = datadir.value_or(sstring());
    _data->cfg.cf_stats = &_data->cf_stats;
    _data->cfg.enable_commitlog = false;
    _data->cm.enable();
    _data->cf = make_lw_shared<replica::column_family>(_data->s, _data->cfg, replica::column_family::no_commitlog(), _data->cm, sstables_manager, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
    _data->table_s = std::make_unique<table_state>(*_data, sstables_manager);
    _data->cm.add(*_data->table_s);
}

compaction::table_state& table_for_tests::as_table_state() noexcept {
    return *_data->table_s;
}

future<> table_for_tests::stop() {
    auto data = _data;
    co_await data->cm.remove(*data->table_s);
    co_await when_all_succeed(data->cm.stop(), data->semaphore.stop()).discard_result();
}

namespace sstables {

<<<<<<< HEAD
test_env::impl::impl(test_env_config cfg)
    : dir_sem(1)
    , feature_service(gms::feature_config_from_db_config(db_config))
    , mgr(cfg.large_data_handler == nullptr ? nop_ld_handler : *cfg.large_data_handler, db_config, feature_service, cache_tracker, memory::stats().total_memory(), dir_sem)
    , semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::test_env")
{ }
=======
std::unordered_map<sstring, s3::endpoint_config> make_storage_options_config(const data_dictionary::storage_options& so) {
    std::unordered_map<sstring, s3::endpoint_config> cfg;
    std::visit(overloaded_functor {
        [] (const data_dictionary::storage_options::local& loc) mutable -> void {
        },
        [&cfg] (const data_dictionary::storage_options::s3& os) mutable -> void {
            cfg[os.endpoint] = s3::endpoint_config {
                .port = std::stoul(tests::getenv_safe("S3_SERVER_PORT_FOR_TEST")),
                .use_https = ::getenv("AWS_DEFAULT_REGION") != nullptr,
                .aws = {{
                    .access_key_id = tests::getenv_safe("AWS_ACCESS_KEY_ID"),
                    .secret_access_key = tests::getenv_safe("AWS_SECRET_ACCESS_KEY"),
                    .session_token = ::getenv("AWS_SESSION_TOKEN") ? : "",
                    .region = ::getenv("AWS_DEFAULT_REGION") ? : "local",
                }},
            };
        }
    }, so.value);
    return cfg;
}

std::unique_ptr<db::config> make_db_config(sstring temp_dir, const data_dictionary::storage_options so) {
    auto cfg = std::make_unique<db::config>();
    cfg->data_file_directories.set({ temp_dir });
    cfg->object_storage_config.set(make_storage_options_config(so));
    return cfg;
}

struct test_env::impl {
    tmpdir dir;
    std::unique_ptr<db::config> db_config;
    directory_semaphore dir_sem;
    ::cache_tracker cache_tracker;
    gms::feature_service feature_service;
    db::nop_large_data_handler nop_ld_handler;
    test_env_sstables_manager mgr;
    std::unique_ptr<test_env_compaction_manager> cmgr;
    reader_concurrency_semaphore semaphore;
    sstables::sstable_generation_generator gen{0};
    sstables::uuid_identifiers use_uuid;
    data_dictionary::storage_options storage;

    impl(test_env_config cfg, sstables::storage_manager* sstm);
    impl(impl&&) = delete;
    impl(const impl&) = delete;

    sstables::generation_type new_generation() noexcept {
        return gen(use_uuid);
    }
};

test_env::impl::impl(test_env_config cfg, sstables::storage_manager* sstm)
    : dir()
    , db_config(make_db_config(dir.path().native(), cfg.storage))
    , dir_sem(1)
    , feature_service(gms::feature_config_from_db_config(*db_config))
    , mgr("test_env", cfg.large_data_handler == nullptr ? nop_ld_handler : *cfg.large_data_handler, *db_config,
        feature_service, cache_tracker, cfg.available_memory, dir_sem,
        [host_id = locator::host_id::create_random_id()]{ return host_id; }, sstm)
    , semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::test_env", reader_concurrency_semaphore::register_metrics::no)
    , use_uuid(cfg.use_uuid)
    , storage(std::move(cfg.storage))
{
    if (cfg.use_uuid) {
        feature_service.uuid_sstable_identifiers.enable();
    }
    if (!storage.is_local_type()) {
        // remote storage requires uuid-based identifier for naming sstables
        assert(use_uuid == uuid_identifiers::yes);
    }
}

test_env::test_env(test_env_config cfg, sstables::storage_manager* sstm)
        : _impl(std::make_unique<impl>(std::move(cfg), sstm)) {
}

test_env::test_env(test_env&&) noexcept = default;

test_env::~test_env() = default;

void test_env::maybe_start_compaction_manager(bool enable) {
    if (!_impl->cmgr) {
        _impl->cmgr = std::make_unique<test_env_compaction_manager>();
        if (enable) {
            _impl->cmgr->get_compaction_manager().enable();
        }
    }
}

future<> test_env::stop() {
    if (_impl->cmgr) {
        co_await _impl->cmgr->get_compaction_manager().stop();
    }
    co_await _impl->mgr.close();
    co_await _impl->semaphore.stop();
}

class mock_sstables_registry : public sstables::sstables_registry {
    struct entry {
        sstring status;
        sstables::sstable_state state;
        sstables::entry_descriptor desc;
    };
    std::map<sstring, entry> _entries;
public:
    virtual future<> create_entry(sstring location, sstring status, sstable_state state, sstables::entry_descriptor desc) override {
        _entries.emplace(location, entry { status, state, desc });
        co_return;
    };
    virtual future<> update_entry_status(sstring location, sstables::generation_type gen, sstring status) override {
        auto it = _entries.find(location);
        if (it != _entries.end()) {
            it->second.status = status;
        } else {
            throw std::runtime_error("update_entry_status: not found");
        }
        co_return;
    }
    virtual future<> update_entry_state(sstring location, sstables::generation_type gen, sstables::sstable_state state) override {
        auto it = _entries.find(location);
        if (it != _entries.end()) {
            it->second.state = state;
        } else {
            throw std::runtime_error("update_entry_state: not found");
        }
        co_return;
    }
    virtual future<> delete_entry(sstring location, sstables::generation_type gen) override {
        auto it = _entries.find(location);
        if (it != _entries.end()) {
            _entries.erase(it);
        } else {
            throw std::runtime_error("delete_entry: not found");
        }
        co_return;
    }
    virtual future<> sstables_registry_list(sstring location, entry_consumer consumer) override {
        for (auto& [loc, e] : _entries) {
            co_await consumer(loc, e.state, e.desc);
        }
    }
};

future<> test_env::do_with_async(noncopyable_function<void (test_env&)> func, test_env_config cfg) {
    if (!cfg.storage.is_local_type()) {
        auto db_cfg = make_shared<db::config>();
        db_cfg->experimental_features({db::experimental_features_t::feature::KEYSPACE_STORAGE_OPTIONS});
        db_cfg->object_storage_config.set(make_storage_options_config(cfg.storage));
        return seastar::async([func = std::move(func), cfg = std::move(cfg), db_cfg = std::move(db_cfg)] () mutable {
            sharded<sstables::storage_manager> sstm;
            sstm.start(std::ref(*db_cfg), sstables::storage_manager::config{}).get();
            auto stop_sstm = defer([&] { sstm.stop().get(); });
            test_env env(std::move(cfg), &sstm.local());
            auto close_env = defer([&] { env.stop().get(); });
            env.manager().plug_sstables_registry(std::make_unique<mock_sstables_registry>());
            auto unplu = defer([&env] { env.manager().unplug_sstables_registry(); });
            func(env);
        });
    }

    return seastar::async([func = std::move(func), cfg = std::move(cfg)] () mutable {
        test_env env(std::move(cfg));
        auto close_env = defer([&] { env.stop().get(); });
        func(env);
    });
}

sstables::generation_type
test_env::new_generation() noexcept {
    return _impl->new_generation();
}

shared_sstable
test_env::make_sstable(schema_ptr schema, sstring dir, sstables::generation_type generation,
        sstable::version_types v, sstable::format_types f,
        size_t buffer_size, gc_clock::time_point now) {
    return _impl->mgr.make_sstable(std::move(schema), dir, _impl->storage, generation, sstables::sstable_state::normal, v, f, now, default_io_error_handler_gen(), buffer_size);
}

shared_sstable
test_env::make_sstable(schema_ptr schema, sstring dir, sstable::version_types v) {
    return make_sstable(std::move(schema), std::move(dir), new_generation(), std::move(v));
}

shared_sstable
test_env::make_sstable(schema_ptr schema, sstables::generation_type generation,
        sstable::version_types v, sstable::format_types f,
        size_t buffer_size, gc_clock::time_point now) {
    return make_sstable(std::move(schema), _impl->dir.path().native(), generation, std::move(v), std::move(f), buffer_size, now);
}

shared_sstable
test_env::make_sstable(schema_ptr schema, sstable::version_types v) {
    return make_sstable(std::move(schema), _impl->dir.path().native(), std::move(v));
}

std::function<shared_sstable()>
test_env::make_sst_factory(schema_ptr s) {
    return [this, s = std::move(s)] {
        return make_sstable(s, new_generation());
    };
}

std::function<shared_sstable()>
test_env::make_sst_factory(schema_ptr s, sstable::version_types version) {
    return [this, s = std::move(s), version] {
        return make_sstable(s, new_generation(), version);
    };
}

future<shared_sstable>
test_env::reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type generation,
        sstable::version_types version, sstable::format_types f) {
    auto sst = make_sstable(std::move(schema), dir, generation, version, f);
    sstable_open_config cfg { .load_first_and_last_position_metadata = true };
    return sst->load(sst->get_schema()->get_sharder(), cfg).then([sst = std::move(sst)] {
        return make_ready_future<shared_sstable>(std::move(sst));
    });
}

future<shared_sstable>
test_env::reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type::int_t gen_value,
        sstable::version_types version, sstable::format_types f) {
    return reusable_sst(std::move(schema), std::move(dir), sstables::generation_type(gen_value), version, f);
}

future<shared_sstable>
test_env::reusable_sst(schema_ptr schema, sstables::generation_type generation,
        sstable::version_types version, sstable::format_types f) {
    return reusable_sst(std::move(schema), _impl->dir.path().native(), std::move(generation), std::move(version), std::move(f));
}

future<shared_sstable>
test_env::reusable_sst(schema_ptr schema, shared_sstable sst) {
    return reusable_sst(std::move(schema), sst->get_storage().prefix(), sst->generation(), sst->get_version());
}

future<shared_sstable>
test_env::reusable_sst(shared_sstable sst) {
    return reusable_sst(sst->get_schema(), std::move(sst));
}

future<shared_sstable>
test_env::reusable_sst(schema_ptr schema, sstables::generation_type generation) {
    return reusable_sst(std::move(schema), _impl->dir.path().native(), generation);
}

test_env_sstables_manager&
test_env::manager() {
    return _impl->mgr;
}

test_env_compaction_manager&
test_env::test_compaction_manager() {
    return *_impl->cmgr;
}

reader_concurrency_semaphore&
test_env::semaphore() {
    return _impl->semaphore;
}

db::config&
test_env::db_config() {
    return *_impl->db_config;
}

tmpdir&
test_env::tempdir() noexcept {
    return _impl->dir;
}

data_dictionary::storage_options
test_env::get_storage_options() const noexcept {
    return _impl->storage;
}

reader_permit
test_env::make_reader_permit(const schema_ptr &s, const char* n, db::timeout_clock::time_point timeout) {
    return _impl->semaphore.make_tracking_only_permit(s, n, timeout, {});
}

reader_permit
test_env::make_reader_permit(db::timeout_clock::time_point timeout) {
    return _impl->semaphore.make_tracking_only_permit(nullptr, "test", timeout, {});
}

replica::table::config
test_env::make_table_config() {
    return replica::table::config{.compaction_concurrency_semaphore = &_impl->semaphore};
}

future<>
test_env::do_with_sharded_async(noncopyable_function<void (sharded<test_env>&)> func) {
    return seastar::async([func = std::move(func)] {
        sharded<test_env> env;
        env.start().get();
        auto stop = defer([&] { env.stop().get(); });
        func(env);
    });
}

table_for_tests
test_env::make_table_for_tests(schema_ptr s, sstring dir) {
    maybe_start_compaction_manager();
    auto cfg = make_table_config();
    cfg.datadir = dir;
    cfg.enable_commitlog = false;
    return table_for_tests(manager(), _impl->cmgr->get_compaction_manager(), s, std::move(cfg), _impl->storage);
}

table_for_tests
test_env::make_table_for_tests(schema_ptr s) {
    maybe_start_compaction_manager();
    auto cfg = make_table_config();
    cfg.datadir = _impl->dir.path().native();
    cfg.enable_commitlog = false;
    return table_for_tests(manager(), _impl->cmgr->get_compaction_manager(), s, std::move(cfg), _impl->storage);
}

data_dictionary::storage_options make_test_object_storage_options() {
    data_dictionary::storage_options ret;
    ret.value = data_dictionary::storage_options::s3 {
        .bucket = tests::getenv_safe("S3_BUCKET_FOR_TEST"),
        .endpoint = tests::getenv_safe("S3_SERVER_ADDRESS_FOR_TEST"),
    };
    return ret;
}

static sstring toc_filename(const sstring& dir, schema_ptr schema, sstables::generation_type generation, sstable_version_types v) {
    return sstable::filename(dir, schema->ks_name(), schema->cf_name(), v, generation,
                             sstable_format_types::big, component_type::TOC);
}

future<shared_sstable> test_env::reusable_sst(schema_ptr schema, sstring dir, sstables::generation_type generation) {
    for (auto v : boost::adaptors::reverse(all_sstable_versions)) {
        if (co_await file_exists(toc_filename(dir, schema, generation, v))) {
            co_return co_await reusable_sst(schema, dir, generation, v);
        }
    }
    throw sst_not_found(dir, generation);
}

void test_env_compaction_manager::propagate_replacement(compaction::table_state& table_s, const std::vector<shared_sstable>& removed, const std::vector<shared_sstable>& added) {
    _cm.propagate_replacement(table_s, removed, added);
}

// Test version of compaction_manager::perform_compaction<>()
future<> test_env_compaction_manager::perform_compaction(shared_ptr<compaction::compaction_task_executor> task) {
    _cm._tasks.push_back(task);
    auto unregister_task = defer([this, task] {
        if (_cm._tasks.remove(task) == 0) {
            testlog.error("compaction_manager_test: deregister_compaction uuid={}: task not found", task->compaction_data().compaction_uuid);
        }
        task->switch_state(compaction_task_executor::state::none);
    });
    co_await task->run_compaction();
}
>>>>>>> 169629dd40 (test/lib: allow overriding available memory via test_env_config)

}
