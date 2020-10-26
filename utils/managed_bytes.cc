
/*
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "managed_bytes.hh"

managed_bytes::managed_bytes(managed_bytes_view o) : managed_bytes(initialized_later(), o.size()) {
    // FIXME: implement
}

std::unique_ptr<bytes_view::value_type[]>
managed_bytes::do_linearize_pure() const {
    auto b = _u.ptr;
    auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[b->size]);
    auto e = data.get();
    while (b) {
        e = std::copy_n(b->data, b->frag_size, e);
        b = b->next;
    }
    return data;
}

managed_bytes_view::managed_bytes_view(const managed_bytes& mb) {
    if (mb._u.small.size != -1) {
        _current_fragment = bytes_view(mb._u.small.data, mb._u.small.size);
        _size = mb._u.small.size;
    } else {
        auto p = mb._u.ptr;
        _current_fragment = bytes_view(p->data, p->frag_size);
        _next_fragments = p->next;
        _size = p->size;
    }
}

bytes_view::value_type
managed_bytes_view::operator[](size_t idx) const {
    if (idx < _current_fragment.size()) {
        return _current_fragment[idx];
    }
    idx -= _current_fragment.size();
    auto f = _next_fragments;
    while (idx >= f->frag_size) {
        idx -= f->frag_size;
        f = f->next;
    }
    return f->data[idx];
}

bytes
managed_bytes_view::to_bytes() const {
    // FIXME: implement
    return bytes();
}

bytes
to_bytes(managed_bytes_view v) {
    return v.to_bytes();
}

bytes
to_bytes(const managed_bytes& b) {
    return managed_bytes_view(b).to_bytes();
}

void managed_bytes_view::remove_prefix(size_t n) {
    // FIXME: implement
}

managed_bytes_view
managed_bytes_view::substr(size_t offset, size_t len) const {
    // FIXME: implement
    return managed_bytes_view();
}

bool
managed_bytes_view::operator==(const managed_bytes_view& x) const {
    // FIXME: implement
    return true;
}

bool
managed_bytes_view::operator!=(const managed_bytes_view& x) const {
    // FIXME: implement
    return true;
}

int compare_unsigned(const managed_bytes_view v1, const managed_bytes_view v2) {
    // FIXME: implement
    return 0;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes_view& v) {
    // FIXME: implement
    return os;
}

std::ostream& operator<<(std::ostream& os, const managed_bytes& b) {
    return os << managed_bytes_view(b);
}
