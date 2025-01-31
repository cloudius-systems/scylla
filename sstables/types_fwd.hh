/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "utils/UUID.hh"

namespace sstables {

using run_id = utils::tagged_uuid<struct run_id_tag>;

enum class integrity_check {
    no = 0,
    checksums_only,
    checksums_and_digest,
};

} // namespace sstables
