/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/file.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/client.hh>
#include "utils/s3/creds.hh"

using namespace seastar;
class memory_data_sink_buffers;

namespace s3 {

struct range {
    uint64_t off;
    size_t len;
};

struct tag {
    std::string key;
    std::string value;
    auto operator<=>(const tag&) const = default;
};
using tag_set = std::vector<tag>;

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_);

class client : public enable_shared_from_this<client> {
    class upload_sink_base;
    class upload_sink;
    class upload_jumbo_sink;
    class readable_file;
    std::string _host;
    endpoint_config_ptr _cfg;
    std::unordered_map<seastar::scheduling_group, http::experimental::client> _https;
    using global_factory = std::function<shared_ptr<client>(std::string)>;
    global_factory _gf;

    struct private_tag {};

    void authorize(http::request&);
    future<> make_request(http::request req, http::experimental::client::reply_handler handle = ignore_reply, http::reply::status_type expected = http::reply::status_type::ok);

    future<> get_object_header(sstring object_name, http::experimental::client::reply_handler handler);
public:
    explicit client(std::string host, endpoint_config_ptr cfg, global_factory gf, private_tag);
    static shared_ptr<client> make(std::string endpoint, endpoint_config_ptr cfg, global_factory gf = {});

    future<uint64_t> get_object_size(sstring object_name);
    struct stats {
        uint64_t size;
        std::time_t last_modified;
    };
    future<stats> get_object_stats(sstring object_name);
    future<tag_set> get_object_tagging(sstring object_name);
    future<> put_object_tagging(sstring object_name, tag_set tagging);
    future<> delete_object_tagging(sstring object_name);
    future<temporary_buffer<char>> get_object_contiguous(sstring object_name, std::optional<range> range = {});
    future<> put_object(sstring object_name, temporary_buffer<char> buf);
    future<> put_object(sstring object_name, ::memory_data_sink_buffers bufs);
    future<> delete_object(sstring object_name);

    file make_readable_file(sstring object_name);
    data_sink make_upload_sink(sstring object_name);
    data_sink make_upload_jumbo_sink(sstring object_name, std::optional<unsigned> max_parts_per_piece = {});

    void update_config(endpoint_config_ptr);

    struct handle {
        std::string _host;
        global_factory _gf;
    public:
        handle(const client& cln)
                : _host(cln._host)
                , _gf(cln._gf)
        {}

        shared_ptr<client> to_client() && {
            return _gf(std::move(_host));
        }
    };

    future<> close();
};

} // s3 namespace
