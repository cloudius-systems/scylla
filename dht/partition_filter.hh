/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include <algorithm>

namespace dht {

class incremental_owned_ranges_checker {
    const dht::token_range_vector& _sorted_owned_ranges;
    mutable dht::token_range_vector::const_iterator _it;
public:
    incremental_owned_ranges_checker(const dht::token_range_vector& sorted_owned_ranges)
            : _sorted_owned_ranges(sorted_owned_ranges)
            , _it(_sorted_owned_ranges.begin()) {
    }

    // Must be called with increasing token values.
    bool belongs_to_current_node(const dht::token& t) const noexcept {
        // While token T is after a range Rn, advance the iterator.
        // iterator will be stopped at a range which either overlaps with T (if T belongs to node),
        // or at a range which is after T (if T doesn't belong to this node).
        _it = std::lower_bound(_it, _sorted_owned_ranges.end(), t,
                               [] (const dht::token_range& range, const dht::token& token) {
            return range.after(token, dht::token_comparator());
        });
        return _it != _sorted_owned_ranges.end() && _it->contains(t, dht::token_comparator());
    }

    static flat_mutation_reader_v2::filter make_partition_filter(const dht::token_range_vector& sorted_owned_ranges);
};

} // dht
