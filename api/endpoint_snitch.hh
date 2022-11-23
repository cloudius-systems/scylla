/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "api.hh"

namespace api {

void set_endpoint_snitch(http_context& ctx, routes& r);

}
