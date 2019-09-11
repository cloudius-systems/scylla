/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "alternator/executor.hh"
#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>

namespace alternator {

class server {
    seastar::httpd::http_server_control _control;
    seastar::sharded<executor>& _executor;
public:
    server(seastar::sharded<executor>& executor) : _executor(executor) {}

    seastar::future<> init(net::inet_address addr, uint16_t port);
private:
    void set_routes(seastar::httpd::routes& r);
};

}

