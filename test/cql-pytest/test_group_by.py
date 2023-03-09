# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for SELECT's GROUP BY feature
#############################################################################

import pytest
from util import new_test_table
from cassandra.protocol import InvalidRequest

# table1 has some pre-set data which the tests below SELECT on (the tests
# shouldn't write to it).
@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p, c1, c2, v) VALUES (?, ?, ?, ?)')
        cql.execute(stmt, [0, 0, 0, 1])
        cql.execute(stmt, [0, 0, 1, 2])
        cql.execute(stmt, [0, 1, 0, 3])
        cql.execute(stmt, [0, 1, 1, 4])
        cql.execute(stmt, [1, 0, 0, 5])
        cql.execute(stmt, [1, 0, 1, 6])
        cql.execute(stmt, [1, 1, 0, 7])
        cql.execute(stmt, [1, 1, 1, 8])
        # FIXME: I would like to make the table read-only to prevent tests
        # from messing with it accidentally.
        yield table

### GROUP BY without aggregation:

# GROUP BY partition key picks a single row from each partition, the
# first row (in the clustering order).
def test_group_by_partition(cql, table1):
    assert {(0,0,0,1), (1,0,0,5)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p'))

# Similarly, GROUP BY a partition key plus just a prefix of the clustering
# key retrieves the first row in each of the clustering ranges
def test_group_by_clustering_prefix(cql, table1):
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    # Try the same restricting the scan to a single partition instead of
    # a whole-table scan
    assert [(0,0,0,1), (0,1,0,3)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY p,c1'))

# Adding a LIMIT should be honored.
# Reproduces #5362 - fewer results than the limit were generated.
@pytest.mark.xfail(reason="issue #5362")
def test_group_by_clustering_prefix_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# Adding a PER PARTITION LIMIT should be honored
# Reproduces #5363 - fewer results than the limit were generated
@pytest.mark.xfail(reason="issue #5363")
def test_group_by_clustering_prefix_with_pplimit(cql, table1):
    # Without per-partition limit we get 4 results, 2 from each partition:
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    # With per-partition limit of 2, no change:
    # But in issue #5363, we got here fewer results.
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 2'))
    # With per-partition limit of 1, we should get just two of the results -
    # the lower clustering key in each.
    assert {(0,0,0,1), (1,0,0,5)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 1'))

# GROUP BY the entire primary key doesn't really do anything, but is
# allowed
def test_group_by_entire_primary_key(cql, table1):
    assert [(0,0,0,1), (0,0,1,2), (0,1,0,3), (0,1,1,4)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY p,c1,c2'))

# GROUP BY can only be given the primary key columns in order - anything
# else is an error. If some of the key columns are restricted by equality,
# they can be skipped in GROUP BY:
def test_group_bad_columns(cql, table1):
    # non-existent column:
    with pytest.raises(InvalidRequest, match='zzz'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY zzz')
    # Legal primary key prefixes are (p), (p,c1) and (p,c1,c2), already
    # tested above. Other combinations or orders like (c1) or (c1, p) are
    # illegal.
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1,c2')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c2')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1,p')
    # Although we checked "GROUP BY c1" is not allowed, it becomes allowed
    # if p is restricted by equality
    assert [(0,0,0,1), (0,1,0,3)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY c1'))

### GROUP BY with aggregation. For example, when asking for count()
### we get a separate count for each group.
# NOTE: we have additional tests for combining aggregations and GROUP BY
# in test_aggregate.py - e.g., a test for #12477.
def test_group_by_count(cql, table1):
    assert {(0,4), (1,4)} == set(cql.execute(f'SELECT p,count(*) FROM {table1} GROUP BY p'))
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    assert {(0,0,0,1), (0,0,1,1), (0,1,0,1), (0,1,1,1), (1,0,0,1), (1,0,1,1), (1,1,0,1), (1,1,1,1)} == set(cql.execute(f'SELECT p,c1,c2,count(*) FROM {table1} GROUP BY p,c1,c2'))

# Adding a LIMIT should be honored.
# Reproduces #5361 - more results than the limit were generated (seems the
# limit was outright ignored).
@pytest.mark.xfail(reason="issue #5361")
def test_group_by_count_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# Adding a PER PARTITION LIMIT should be honored
# Reproduces #5363 - more results than the limit were generated
@pytest.mark.xfail(reason="issue #5363")
def test_group_by_count_with_pplimit(cql, table1):
    # Without per-partition limit we get 4 results, 2 from each partition:
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    # With per-partition limit of 2, no change:
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 2'))
    # With per-partition limit of 1, we should get just two of the results -
    # the lower clustering key in each.
    # In issue #5363 we wrongly got here all four results (the PER PARTITION
    # LIMIT appears to have been ignored)
    assert {(0,0,2), (1,0,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 1'))

def test_group_by_sum(cql, table1):
    assert {(0,10), (1,26)} == set(cql.execute(f'SELECT p,sum(v) FROM {table1} GROUP BY p'))
    assert {(0,0,3), (0,1,7), (1,0,11), (1,1,15)} == set(cql.execute(f'SELECT p,c1,sum(v) FROM {table1} GROUP BY p,c1'))
    assert {(0,0,0,1), (0,0,1,2), (0,1,0,3), (0,1,1,4), (1,0,0,5), (1,0,1,6), (1,1,0,7), (1,1,1,8)} == set(cql.execute(f'SELECT p,c1,c2,sum(v) FROM {table1} GROUP BY p,c1,c2'))

# If selecting both an aggregation and real columns, we get both the result
# from *one* of the group rows (the first row by clustering order), and the
# aggregation from all of them.
def test_group_by_v_and_sum(cql, table1):
    assert {(0,1,10), (1,5,26)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p'))
    assert {(0,1,3), (0,3,7), (1,5,11), (1,7,15)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1'))
    assert {(0,1,1), (0,2,2), (0,3,3), (0,4,4), (1,5,5), (1,6,6), (1,7,7), (1,8,8)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1,c2'))

# Adding a LIMIT should be honored.
# Reproduces #5361 - more results than the limit were generated (seems the
# limit was outright ignored).
@pytest.mark.xfail(reason="issue #5361")
def test_group_by_v_and_sum_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# NOTE: we have tests for the combination of GROUP BY and SELECT DISTINCT
# in test_distinct.py (reproducing issue #12479).
