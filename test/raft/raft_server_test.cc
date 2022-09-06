#include "replication.hh"
#include "utils/error_injection.hh"
#include <seastar/util/defer.hh>

#ifdef SEASTAR_DEBUG
// Increase tick time to allow debug to process messages
 const auto tick_delay = 200ms;
#else
const auto tick_delay = 100ms;
#endif

SEASTAR_THREAD_TEST_CASE(test_release_memory_if_add_entry_throws) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
#else
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case {
                .nodes = 1,
                .config = std::vector<raft::server::configuration>({
                    raft::server::configuration {
                        .max_snapshot_trailing_bytes = 1,
                        .max_log_size = 16 + sizeof(raft::log_entry),
                        .max_command_size = 10
                    }
                })
            },
            ::apply_changes,
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get0();
    auto stop = defer([&cluster] { cluster.stop_all().get(); });

    utils::get_local_injector().enable("fsm::add_entry/test-failure", true);
    auto check_error = [](const std::runtime_error& e) {
        return e.what() == sstring("fsm::add_entry/test-failure");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get0(), std::runtime_error, check_error);

    cluster.add_entries(1, 0).get0();
    cluster.read(read_value{0, 1}).get();
#endif
}
