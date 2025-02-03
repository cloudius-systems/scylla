/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/test/unit_test.hpp>

#include <seastar/core/sleep.hh>
#include <seastar/util/noncopyable_function.hh>

#include "seastarx.hh"

inline
void eventually(noncopyable_function<void ()> f, size_t max_attempts = 17) {
    size_t attempts = 0;
    while (true) {
        try {
            f();
            break;
        } catch (...) {
            if (++attempts < max_attempts) {
                sleep(std::chrono::milliseconds(1 << attempts)).get();
            } else {
                throw;
            }
        }
    }
}

inline
bool eventually_true(noncopyable_function<bool ()> f) {
    const unsigned max_attempts = 15;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            seastar::sleep(std::chrono::milliseconds(1 << attempts)).get();
        } else {
            return false;
        }
    }

    return false;
}

// Must be called in a seastar thread
template <typename T>
void REQUIRE_EVENTUALLY_EQUAL(std::function<T()> a, T b) {
    eventually_true([&] { return a() == b; });
    BOOST_REQUIRE_EQUAL(a(), b);
}

// Must be called in a seastar thread
template <typename T>
void CHECK_EVENTUALLY_EQUAL(std::function<T()> a, T b) {
    eventually_true([&] { return a() == b; });
    BOOST_CHECK_EQUAL(a(), b);
}
