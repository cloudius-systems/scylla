/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/dynamic_bitset.hpp>
#include "replica/database_fwd.hh"
#include "mutation/mutation_fwd.hh"
#include "timestamp.hh"
#include <seastar/util/noncopyable_function.hh>

namespace cdc {

// Represents a set of column ids of one kind (partition key, clustering key, regular row or static row).
// There already exists a column_set type, but it keeps ordinal_column_ids, not column_ids (ordinal column ids
// are unique across whole table, while kind-specific ids are unique only within one column kind).
// To avoid converting back and forth between ordinal and kind-specific ids, one_kind_column_set is used instead.
using one_kind_column_set = boost::dynamic_bitset<uint64_t>;

// An object that processes changes from a single, big mutation.
// It is intended to be used with process_changes_xxx_splitting. Those functions define the order and layout in which
// changes should appear in CDC log, and change_processor is responsible for producing CDC log rows from changes given
// by those two functions.
//
// The flow of calling its methods should go as follows:
//   -> begin_timestamp #1
//     -> produce_preimage (one call for each preimage row to be generated)
//     -> process_change (one call for each part generated by the splitting function)
//     -> produce_postimage (one call for each postimage row to be generated)
//   -> begin_timestamp #2
//   ...
class change_processor {
protected:
    ~change_processor() {};
public:
    // Tells the processor that changes that follow from now on will be of given timestamp.
    // This method must be called in increasing timestamp order.
    // begin_timestamp can be called only once for a given timestamp and change_processor object.
    //   ts - timestamp of mutation parts
    //   is_last - determines if this will be the last timestamp to be processed by this change_processor instance.
    virtual void begin_timestamp(api::timestamp_type ts, bool is_last) = 0;

    // Tells the processor to produce a preimage for a given clustering/static row.
    //   ck - clustering key of the row for which to produce a preimage; if nullptr, static row preimage is requested
    //   columns_to_include - include information about the current state of those columns only, leave others as null
    virtual void produce_preimage(const clustering_key* ck, const one_kind_column_set& columns_to_include) = 0;

    // Tells the processor to produce a postimage for a given clustering/static row.
    // Contrary to preimage, this requires data from all columns to be present.
    //   ck - clustering key of the row for which to produce a postimage; if nullptr, static row postimage is requested
    virtual void produce_postimage(const clustering_key* ck) = 0;

    // Processes a smaller mutation which is a subset of the big mutation.
    // The mutation provided to process_change should be simple enough for it to be possible to convert it
    // into CDC log rows - for example, it cannot represent a write to two columns of the same row, where
    // both columns have different timestamp or TTL set.
    //   m - the small mutation to be converted into CDC log rows.
    virtual void process_change(const mutation& m) = 0;

    // Tells processor we have reached end of record - last part
    // of a given timestamp batch
    virtual void end_record() = 0;
};

bool should_split(const mutation& base_mutation);
void process_changes_with_splitting(const mutation& base_mutation, change_processor& processor,
        bool enable_preimage, bool enable_postimage);
void process_changes_without_splitting(const mutation& base_mutation, change_processor& processor,
        bool enable_preimage, bool enable_postimage);

}
