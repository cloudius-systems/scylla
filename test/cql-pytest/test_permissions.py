# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for managing permissions
#############################################################################

import pytest
import time
from cassandra.protocol import SyntaxException, InvalidRequest, Unauthorized
from util import new_test_table, new_function, new_user, new_session, new_test_keyspace, unique_name

# Test that granting permissions to various resources works for the default user.
# This case does not include functions, because due to differences in implementation
# the tests will diverge between Scylla and Cassandra (e.g. there's no common language)
# to create a user-defined function in.
# Marked cassandra_bug, because Cassandra allows granting a DESCRIBE permission
# to a specific ROLE, in contradiction to its own documentation:
# https://cassandra.apache.org/doc/latest/cassandra/cql/security.html#cql-permissions
def test_grant_applicable_data_and_role_permissions(cql, test_keyspace, cassandra_bug):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_table(cql, test_keyspace, schema) as table:
        # EXECUTE is not listed, as it only applies to functions, which aren't covered in this test case
        all_permissions = set(['create', 'alter', 'drop', 'select', 'modify', 'authorize', 'describe'])
        applicable_permissions = {
            'all keyspaces': ['create', 'alter', 'drop', 'select', 'modify', 'authorize'],
            f'keyspace {test_keyspace}': ['create', 'alter', 'drop', 'select', 'modify', 'authorize'],
            f'table {table}': ['alter', 'drop', 'select', 'modify', 'authorize'],
            'all roles': ['create', 'alter', 'drop', 'authorize', 'describe'],
            f'role {user}': ['alter', 'drop', 'authorize'],
        }
        for resource, permissions in applicable_permissions.items():
            # Applicable permissions can be legally granted
            for permission in permissions:
                cql.execute(f"GRANT {permission} ON {resource} TO {user}")
            # Only applicable permissions can be granted - nonsensical combinations
            # are refused with an error
            for permission in all_permissions.difference(set(permissions)):
                with pytest.raises((InvalidRequest, SyntaxException), match="support.*permissions"):
                    cql.execute(f"GRANT {permission} ON {resource} TO {user}")


def eventually_authorized(fun, timeout_s=10):
    for i in range(timeout_s * 10):
        try:
            return fun()
        except Unauthorized as e:
            time.sleep(0.1)
    return fun()

def eventually_unauthorized(fun, timeout_s=10):
    for i in range(timeout_s * 10):
        try:
            fun()
            time.sleep(0.1)
        except Unauthorized as e:
            return
    try:
        fun()
        pytest.fail(f"Function {fun} was not refused as unauthorized")
    except Unauthorized as e:
        return

def grant(cql, permission, resource, username):
    cql.execute(f"GRANT {permission} ON {resource} TO {username}")

def revoke(cql, permission, resource, username):
    cql.execute(f"REVOKE {permission} ON {resource} FROM {username}")

# Helper function for checking that given `function` can only be executed
# with given `permission` granted, and returns an unauthorized error
# with the `permission` revoked.
def check_enforced(cql, username, permission, resource, function):
    eventually_unauthorized(function)
    grant(cql, permission, resource, username)
    eventually_authorized(function)
    revoke(cql, permission, resource, username)
    eventually_unauthorized(function)

# Test that data permissions can be granted and revoked, and that they're effective
def test_grant_revoke_data_permissions(cql, test_keyspace):
    with new_user(cql) as username:
        with new_session(cql, username) as user_session:
            ks = unique_name()
            # Permissions on all keyspaces
            def create_keyspace_idempotent():
                user_session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }}")
                user_session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
            check_enforced(cql, username, permission='CREATE', resource='ALL KEYSPACES', function=create_keyspace_idempotent)
            # Permissions for a specific keyspace
            with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as keyspace:
                t = unique_name()
                def create_table_idempotent():
                    user_session.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")
                    cql.execute(f"DROP TABLE IF EXISTS {keyspace}.{t}")
                eventually_unauthorized(create_table_idempotent)
                grant(cql, 'CREATE', f'KEYSPACE {keyspace}', username)
                eventually_authorized(create_table_idempotent)
                cql.execute(f"CREATE TABLE {keyspace}.{t}(id int primary key)")
                # Permissions for a specific table
                check_enforced(cql, username, permission='ALTER', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"ALTER TABLE {keyspace}.{t} WITH comment = 'hey'"))
                check_enforced(cql, username, permission='SELECT', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"SELECT * FROM {keyspace}.{t}"))
                check_enforced(cql, username, permission='MODIFY', resource=f'{keyspace}.{t}',
                        function=lambda: user_session.execute(f"INSERT INTO {keyspace}.{t}(id) VALUES (42)"))
                cql.execute(f"DROP TABLE {keyspace}.{t}")
                revoke(cql, 'CREATE', f'KEYSPACE {keyspace}', username)
                eventually_unauthorized(create_table_idempotent)

                def drop_table_idempotent():
                    user_session.execute(f"DROP TABLE IF EXISTS {keyspace}.{t}")
                    cql.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")

                cql.execute(f"CREATE TABLE {keyspace}.{t}(id int primary key)")
                check_enforced(cql, username, permission='DROP', resource=f'KEYSPACE {keyspace}', function=drop_table_idempotent)
                cql.execute(f"DROP TABLE {keyspace}.{t}")

                # CREATE permission on all keyspaces also implies creating any tables in any keyspace
                check_enforced(cql, username, permission='CREATE', resource='ALL KEYSPACES', function=create_table_idempotent)
                # Same for DROP
                cql.execute(f"CREATE TABLE IF NOT EXISTS {keyspace}.{t}(id int primary key)")
                check_enforced(cql, username, permission='DROP', resource='ALL KEYSPACES', function=drop_table_idempotent)

# Test that permissions for user-defined functions are serialized in a Cassandra-compatible way
def test_udf_permissions_serialization(cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            # Creating a bilingual function makes this test case work for both Scylla and Cassandra
            div_body_lua = "(b bigint, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return b//i'"
            div_body_java = "(b bigint, i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return b/i;'"
            div_body = div_body_lua
            try:
                with new_function(cql, keyspace, div_body) as div_fun:
                    pass
            except:
                div_body = div_body_java
            with new_function(cql, keyspace, div_body) as div_fun:
                applicable_permissions = {
                    'all functions': ['create', 'alter', 'drop', 'authorize', 'execute'],
                    f'all functions in keyspace {keyspace}': ['create', 'alter', 'drop', 'authorize', 'execute'],
                    f'function {keyspace}.{div_fun}(bigint, int)': ['alter', 'drop', 'authorize', 'execute'],
                }
                for resource, permissions in applicable_permissions.items():
                    # Applicable permissions can be legally granted
                    for permission in permissions:
                        cql.execute(f"GRANT {permission} ON {resource} TO {user}")

                permissions = {row.resource: row.permissions for row in cql.execute(f"SELECT * FROM system_auth.role_permissions")}
                assert permissions['functions'] == set(['ALTER', 'AUTHORIZE', 'CREATE', 'DROP', 'EXECUTE'])
                assert permissions[f'functions/{keyspace}'] == set(['ALTER', 'AUTHORIZE', 'CREATE', 'DROP', 'EXECUTE'])
                assert permissions[f'functions/{keyspace}/{div_fun}[org.apache.cassandra.db.marshal.LongType^org.apache.cassandra.db.marshal.Int32Type]'] == set(['ALTER', 'AUTHORIZE', 'DROP', 'EXECUTE'])

                resources_with_execute = [row.resource for row in cql.execute(f"LIST EXECUTE OF {user}")]
                assert '<all functions>' in resources_with_execute
                assert f'<all functions in {keyspace}>' in resources_with_execute
                assert f'<function {keyspace}.{div_fun}(bigint, int)>' in resources_with_execute

# Test that names that require quoting (e.g. due to having nonorthodox characters)
# are properly handled, with right permissions granted.
# Cassandra doesn't quote names properly, so the test fails
def test_udf_permissions_quoted_names(cassandra_bug, cql):
    schema = "a int primary key"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            weird_body_lua = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE lua AS 'return 42;'"
            weird_body_java = "(i int) CALLED ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return 42;'"
            weird_body = weird_body_lua
            try:
                with new_function(cql, keyspace, weird_body) as weird_fun:
                    pass
            except:
                weird_body = weird_body_java
            with new_function(cql, keyspace, weird_body, '"weird[name]"') as weird_fun:
                with new_user(cql) as username:
                    with new_session(cql, username) as user_session:
                        grant(cql, 'EXECUTE', f'FUNCTION {keyspace}.{weird_fun}(int)', username)
                        grant(cql, 'SELECT', table, username)
                        cql.execute(f"INSERT INTO {table}(a) VALUES (7)")
                        assert list([r[0] for r in user_session.execute(f"SELECT {keyspace}.{weird_fun}(a) FROM {table}")]) == [42]

# Test that permissions set for user-defined functions are enforced
# Tests for ALTER are separate, because they are qualified as cassandra_bug
def test_grant_revoke_udf_permissions(cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
            fun_body_java = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body) as fun:
                    pass
            except:
                fun_body = fun_body_java
            with new_user(cql) as username:
                with new_session(cql, username) as user_session:
                    fun = "fun42"

                    def create_function_idempotent():
                        user_session.execute(f"CREATE FUNCTION IF NOT EXISTS {keyspace}.{fun} {fun_body}")
                        cql.execute(f"DROP FUNCTION IF EXISTS {keyspace}.{fun}(int)")
                    check_enforced(cql, username, permission='CREATE', resource=f'all functions in keyspace {keyspace}',
                            function=create_function_idempotent)
                    check_enforced(cql, username, permission='CREATE', resource='all functions',
                            function=create_function_idempotent)

                    def drop_function_idempotent():
                        user_session.execute(f"DROP FUNCTION IF EXISTS {keyspace}.{fun}(int)")
                        cql.execute(f"CREATE FUNCTION IF NOT EXISTS {keyspace}.{fun} {fun_body}")
                    for resource in [f'function {keyspace}.{fun}(int)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='DROP', resource=resource, function=drop_function_idempotent)

                    grant(cql, 'SELECT', table, username)
                    for resource in [f'function {keyspace}.{fun}(int)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='EXECUTE', resource=resource,
                                function=lambda: user_session.execute(f"SELECT {keyspace}.{fun}(a) FROM {table}"))

                    grant(cql, 'EXECUTE', 'ALL FUNCTIONS', username)
                    def grant_idempotent():
                        grant(user_session, 'EXECUTE', f'function {keyspace}.{fun}(int)', 'cassandra')
                        revoke(cql, 'EXECUTE', f'function {keyspace}.{fun}(int)', 'cassandra')
                    for resource in [f'function {keyspace}.{fun}(int)', f'all functions in keyspace {keyspace}', 'all functions']:
                        check_enforced(cql, username, permission='AUTHORIZE', resource=resource, function=grant_idempotent)

# This test case is artificially extracted from the one above,
# because it's qualified as cassandra_bug - the documentation quotes that ALTER is needed on
# functions if the definition is replaced (CREATE OR REPLACE FUNCTION (...)),
# and yet it's not enforced
def test_grant_revoke_alter_udf_permissions(cassandra_bug, cql):
    schema = "a int primary key"
    user = "cassandra"
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }") as keyspace:
        with new_test_table(cql, keyspace, schema) as table:
            fun_body_lua = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE lua AS 'return 42;'"
            fun_body_java = "(i int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 42;'"
            fun_body = fun_body_lua
            try:
                with new_function(cql, keyspace, fun_body) as fun:
                    pass
            except:
                fun_body = fun_body_java
            with new_user(cql) as username:
                with new_session(cql, username) as user_session:
                    fun = "fun42"
                    grant(cql, 'CREATE', 'ALL FUNCTIONS', username)
                    check_enforced(cql, username, permission='ALTER', resource=f'all functions in keyspace {keyspace}',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}"))
                    check_enforced(cql, username, permission='ALTER', resource='all functions',
                            function=lambda: user_session.execute(f"CREATE OR REPLACE FUNCTION {keyspace}.{fun} {fun_body}")) 
