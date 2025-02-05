/*
 * Copyright (C) 2024-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/seastar.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include "utils/lister.hh"
#include "utils/s3/client.hh"
#include "replica/database.hh"
#include "db/config.hh"
#include "db/snapshot-ctl.hh"
#include "db/snapshot/backup_task.hh"
#include "schema/schema_fwd.hh"
#include "sstables/sstables_manager.hh"
#include "utils/error_injection.hh"

extern logging::logger snap_log;

namespace db::snapshot {

backup_task_impl::backup_task_impl(tasks::task_manager::module_ptr module,
                                   snapshot_ctl& ctl,
                                   sstring endpoint,
                                   sstring bucket,
                                   sstring prefix,
                                   sstring ks,
                                   std::filesystem::path snapshot_dir,
                                   bool move_files) noexcept
    : tasks::task_manager::task::impl(module, tasks::task_id::create_random_id(), 0, "node", ks, "", "", tasks::task_id::create_null_id())
    , _snap_ctl(ctl)
    , _endpoint(std::move(endpoint))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _snapshot_dir(std::move(snapshot_dir))
    , _remove_on_uploaded(move_files) {
    _status.progress_units = "bytes ('total' may grow along the way)";
}

std::string backup_task_impl::type() const {
    return "backup";
}

tasks::is_internal backup_task_impl::is_internal() const noexcept {
    return tasks::is_internal::no;
}

tasks::is_abortable backup_task_impl::is_abortable() const noexcept {
    return tasks::is_abortable::yes;
}

future<tasks::task_manager::task::progress> backup_task_impl::get_progress() const {
    auto p =
        co_await _snap_ctl.container().map_reduce0([this](const auto&) { return _progress_per_shard[this_shard_id()]; }, s3::upload_progress(), std::plus<s3::upload_progress>());
    co_return tasks::task_manager::task::progress{
        .completed = p.uploaded,
        .total = p.total,
    };
}

tasks::is_user_task backup_task_impl::is_user_task() const noexcept {
    return tasks::is_user_task::yes;
}

future<> backup_task_impl::upload_component(shared_ptr<s3::client>& client, abort_source& as, s3::upload_progress& progress, sstring name) {
    auto component_name = _snapshot_dir / name;
    auto destination = fmt::format("/{}/{}/{}", _bucket, _prefix, name);
    snap_log.trace("Upload {} to {}", component_name.native(), destination);

    // Start uploading in the background. The caller waits for these fibers
    // with the uploads gate.
    // Parallelism is implicitly controlled in two ways:
    //  - s3::client::claim_memory semaphore
    //  - http::client::max_connections limitation
    try {
        co_await client->upload_file(component_name, destination, progress, &as);
    } catch (...) {
        snap_log.error("Error uploading {}: {}", component_name.native(), std::current_exception());
        throw;
    }

    if (!_remove_on_uploaded) {
        co_return;
    }

    // Delete the uploaded component to:
    // 1. Free up disk space immediately
    // 2. Avoid costly S3 existence checks on future backup attempts
    try {
        co_await remove_file(component_name.native());
    } catch (...) {
        // If deletion of an uploaded file fails, the backup process will continue.
        // While this doesn't halt the backup, it may indicate filesystem permissions
        // issues or system constraints that should be investigated.
        snap_log.warn("Failed to remove {}: {}", component_name, std::current_exception());
    }
}

namespace {

struct shard_files {
    size_t size{0};
    std::vector<sstring> files;
    std::strong_ordering operator<=>(const shard_files& other) const noexcept { return size <=> other.size; }
};

using sharded_files = std::vector<shard_files>;

sharded_files greedy_partitioning(std::vector<std::tuple<size_t, sstring>>&& files, size_t partitions) {
    sharded_files result(partitions);
    std::ranges::sort(files, std::greater());
    for (auto&& [size, file] : files) {
        auto& smallest = *std::ranges::min_element(result, std::less());
        smallest.files.emplace_back(std::move(file));
        smallest.size += size;
    }
    return result;
}

[[maybe_unused]] sharded_files round_robin_partitioning(std::vector<std::tuple<size_t, sstring>>&& files, size_t partitions) {
    sharded_files result(partitions);
    std::ranges::sort(files, std::greater());
    size_t pos = 0;
    for (auto&& [size, file] : files) {
        auto& elem = result[pos++ % partitions];
        elem.files.emplace_back(std::move(file));
        elem.size += size;
    }
    return result;
}

sstring print_results(const sharded_files& files) {
    sstring sizes_table;
    sizes_table += "\n+-------+----------------------------------------------+\n";
    sizes_table += "|  Shard  |    Accumulated size    |  Number of files  |\n";
    sizes_table += "+---------+--------------------------------------------+\n";
    for (size_t i = 0; i < files.size(); ++i) {
        sizes_table += std::format("|{:^9}|{:>17} bytes |{:>19}|\n", i, files[i].size, files[i].files.size());
    }
    sizes_table += "+---------+------------------------+-------------------+\n";
    return sizes_table;
}

future<sharded_files> get_backup_files(const std::filesystem::path& snapshot_dir) {
    std::exception_ptr ex;
    auto snapshot_dir_lister = directory_lister(snapshot_dir, lister::dir_entry_types::of<directory_entry_type::regular>());
    std::vector<std::tuple<size_t, sstring>> backup_files;
    std::optional<directory_entry> component_ent;

    do {
        try {
            component_ent = co_await snapshot_dir_lister.get();
            if (component_ent) {
                auto component_name = snapshot_dir / component_ent->name;
                auto size = co_await file_size(component_name.native());
                backup_files.emplace_back(size, std::move(component_ent->name));
            }
        } catch (...) {
            ex = std::current_exception();
            break;
        }
    } while (component_ent);
    co_await snapshot_dir_lister.close();
    if (ex)
        co_await coroutine::return_exception_ptr(std::move(ex));

    co_return greedy_partitioning(std::move(backup_files), smp::count);
}
} // namespace

future<> backup_task_impl::do_backup() {
    if (!co_await file_exists(_snapshot_dir.native())) {
        throw std::invalid_argument(fmt::format("snapshot does not exist at {}", _snapshot_dir.native()));
    }

    auto backup_shards = co_await get_backup_files(_snapshot_dir);

    if (snap_log.is_enabled(log_level::trace))
        snap_log.trace("{}", print_results(backup_shards));

    sharded<abort_source> as;
    co_await as.start();
    auto s = _as.subscribe([&]() noexcept {
        try {
            std::ignore = smp::invoke_on_all([&as, ex = _as.abort_requested_exception_ptr()] {
                as.local().request_abort_ex(ex);
            });
        } catch (...) {
        }
    });

    std::exception_ptr ex;
    co_await smp::invoke_on_all([this, &ex, &backup_shards, &as] -> future<> {
        try {
            auto shard_id = this_shard_id();
            auto cln = _snap_ctl.storage_manager().container().local().get_endpoint_client(_endpoint);

            gate uploads;

            for (const auto& name : backup_shards[shard_id].files) {
                // Pre-upload break point. For testing abort in actual s3 client usage.
                co_await utils::get_local_injector().inject("backup_task_pre_upload", utils::wait_for_message(std::chrono::minutes(2)));
                auto gh = uploads.hold();

                try {
                    co_await upload_component(cln, as.local(), _progress_per_shard[shard_id], name);
                } catch (...) {
                    ex = std::current_exception();
                    break;
                }
                co_await coroutine::maybe_yield();
                co_await utils::get_local_injector().inject("backup_task_pause", utils::wait_for_message(std::chrono::minutes(2)));
                if (as.local().abort_requested()) {
                    ex = as.local().abort_requested_exception_ptr();
                    break;
                }
            }
            co_await uploads.close();
        } catch (...) {
            ex = std::current_exception();
        }
    });
    co_await as.stop();
    if (ex)
        co_await coroutine::return_exception_ptr(std::move(ex));
}

future<> backup_task_impl::run() {
    // do_backup() removes a file once it is fully uploaded, so we are actually
    // mutating snapshots.
    co_await _snap_ctl.run_snapshot_modify_operation([this] {
        return do_backup();
    });
    snap_log.info("Finished backup");
}

} // db::snapshot namespace
