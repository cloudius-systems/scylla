#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2017-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

'''
Generates compression segmentation parameters.

These parameters are used to reduce the memory footprint of the
in-memory compression database. See sstables/compress.hh for more
details.
'''

import argparse
import math


def data_size_range_log2():
    return range(4, 51)


def chunk_size_range_log2():
    return range(4, 31)


def base_offset_size(data_size, chunk_size, n):
    return int(math.ceil(math.log2(data_size)))


def relative_offset_size(data_size, chunk_size, n):
    if n == 1:
        return int(0)
    else:
        return int(math.ceil(math.log2((n - 1) * (chunk_size + 64))))


def segment_size(data_size, chunk_size, n):
    return base_offset_size(data_size, chunk_size, n) + (n - 1) * relative_offset_size(data_size, chunk_size, n)


def no_of_segments(data_size, chunk_size, n):
    return int(math.ceil((data_size / chunk_size) / n))


def n_for(data_size, chunk_size, n_values):
    nominal_data_size = int(math.ceil(math.log2(data_size)))
    chunk_size_log2 = int(math.ceil(math.log2(chunk_size)))
    return next(filter(lambda x: x[0] == nominal_data_size and x[1] == chunk_size_log2, n_values))[2]


def size_deque(data_size, chunk_size):
    return int(math.ceil(data_size / chunk_size)) * 64


def size_grouped_segments(data_size, chunk_size, n):
    return no_of_segments(data_size, chunk_size, n) * segment_size(data_size, chunk_size, n)


def best_nominal_data_size_for_bucket_size(chunk_size, bucket_size, n_values):
    def addressable_space(data_size_log2):
        data_size = 2**data_size_log2
        n = n_for(data_size, chunk_size, n_values)
        bucket_size_bits = bucket_size * 8
        total_size_bits = size_grouped_segments(data_size, chunk_size, n)

        if bucket_size_bits >= total_size_bits:
            return data_size, data_size_log2
        else:
            segments_pb = segments_per_bucket(data_size, chunk_size, n, bucket_size)
            return n * segments_pb * chunk_size, data_size_log2

    space = map(addressable_space, data_size_range_log2())
    return max(space, key=lambda x: x[0])[1]


def segments_per_bucket(data_size, chunk_size, n, bucket_size):
    # A safety padding of 7 bytes has to be reserved at the end
    bucket_size_bits = bucket_size * 8 - 56
    segment_size_bits = segment_size(data_size, chunk_size, n)

    fits = int(math.floor(bucket_size_bits / segment_size_bits))

    # We can't have more segments than the sizes support
    return min(no_of_segments(data_size, chunk_size, n), fits)


def all_n_values():
    optimal_sizes = {}

    for f in data_size_range_log2():
        for c in chunk_size_range_log2():
            optimal_size = None
            for n in range(1, 201):
                s = size_grouped_segments(2**f, 2**c, n)

                if optimal_size is None or optimal_size[3] > s:
                    optimal_size = (f, c, n, s)

            optimal_sizes[(f, c)] = optimal_size

    n_values = []
    for k in sorted(optimal_sizes.keys()):
        f, c, n, s = optimal_sizes[k]
        n_values.append((f, c, n))

    return n_values


file_str = """
/*
 * Copyright (C) 2017-present ScyllaDB
 */

// SPDX-License-Identifier: AGPL-3.0-or-later

/*
 * This file was autogenerated by gen_segmented_compress_params.py.
 */

#include "compress.hh"

#include <array>

namespace sstables {{

const uint64_t bucket_size{{{bucket_size}}};

struct bucket_info {{
    uint64_t chunk_size_log2;
    uint64_t best_data_size_log2;
    uint64_t segments_per_bucket;
}};

// The largest data chunk from the file a bucketful of offsets can
// cover, precalculated for different chunk sizes, plus the number
// of segments that are needed to address the whole area.
const std::array<bucket_info, {bucket_infos_size}> bucket_infos{{{{
{bucket_infos}}}}};

struct segment_info {{
    uint8_t data_size_log2;
    uint8_t chunk_size_log2;
    uint8_t grouped_offsets;
}};

// Precomputed optimal segment informations for different data and chunk sizes.
const std::array<segment_info, {segment_infos_size}> segment_infos{{{{
{segment_infos}}}}};

}} // namespace sstables
"""

if __name__ == '__main__':
    cmdline_parser = argparse.ArgumentParser()
    cmdline_parser.add_argument('--bucket-size-log2', action='store', help='specify bucket size (defaults to 4K)')

    args = cmdline_parser.parse_args()

    if args.bucket_size_log2 is not None:
        bucket_size_log2 = int(args.bucket_size_log2)

        if bucket_size_log2 < 10 or bucket_size_log2 > 30:
            print("Bucket size is either too large or too small")
            exit(1)
    else:
        bucket_size_log2 = 12  # 4K

    bucket_size = 2**bucket_size_log2

    n_values = all_n_values()

    with open("sstables/segmented_compress_params.hh", "w") as infos_file:
        bucket_infos = []
        data_sizes = []
        for chunk_size_log2 in chunk_size_range_log2():
            chunk_size = 2**chunk_size_log2
            data_size_log2 = best_nominal_data_size_for_bucket_size(chunk_size, bucket_size, n_values)
            data_size = 2**data_size_log2
            n = n_for(data_size, chunk_size, n_values)

            bucket_infos.append("    {{{}, {}, {} /*out of the max of {}*/}}".format(
                chunk_size_log2,
                data_size_log2,
                segments_per_bucket(data_size, chunk_size, n, bucket_size),  # no of segments that fit into the bucket
                no_of_segments(data_size, chunk_size, n)))  # normal no of segments for these sizes
            data_sizes.append(data_size_log2)

        segment_infos = []
        for n_value in n_values:
            if n_value[0] in data_sizes:
                segment_infos.append("    {{{}, {}, {}}}".format(*n_value))

        infos_file.write(file_str.format(
            bucket_size=bucket_size,
            bucket_infos=",\n".join(bucket_infos),
            bucket_infos_size=len(bucket_infos),
            segment_infos=",\n".join(segment_infos),
            segment_infos_size=len(segment_infos)))
