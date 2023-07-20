#!/usr/bin/python3

import os
import sys
import logging
import pytest
import shutil
import tempfile
from dataclasses import dataclass
from contextlib import contextmanager

# use minio_server
sys.path.insert(1, sys.path[0] + '/../..')
from test.pylib.minio_server import MinioServer
from test.pylib.host_registry import HostRegistry
from test.pylib.cql_repl import conftest

hosts = HostRegistry()


def pytest_addoption(parser):
    conftest.pytest_addoption(parser)
    # reserved for tests with real S3
    s3_options = parser.getgroup("s3-server", description="S3 Server settings")
    s3_options.addoption('--s3-server-address')
    s3_options.addoption('--s3-server-port', type=int)
    s3_options.addoption('--s3-server-bucket')


@dataclass
class S3_Server:
    address: str
    port: int
    bucket_name: str

    async def start(self):
        pass

    async def stop(self):
        pass


@pytest.fixture(scope="function")
def ssl(request):
    yield request.config.getoption('--ssl')


@contextmanager
def _maybe_remove_tempdir_on_complete(request, tempdir):
    if pytest.version_tuple > (7, 3, 0):
        yield
        return
    # tmp_path_retention_count was introduced in pytest 7.3.0, so we have
    # to swing this by ourselves when using lower version of pytest. see
    # https://docs.pytest.org/en/7.3.x/changelog.html#pytest-7-3-0-2023-04-08
    tests_failed = request.session.testsfailed
    yield
    if tests_failed == request.session.testsfailed:
        # not tests failed after this seesion, so remove the tempdir
        shutil.rmtree(tempdir)


@pytest.fixture(scope="function")
def test_tempdir(request, tmpdir):
    tempdir = tmpdir.strpath
    with _maybe_remove_tempdir_on_complete(request, tempdir):
        yield tempdir
        with open(os.path.join(tempdir, 'log'), 'rb') as log:
            shutil.copyfileobj(log, sys.stdout.buffer)


@pytest.fixture(scope="function")
async def s3_server(pytestconfig, tmpdir):
    server = None
    s3_server_address = pytestconfig.getoption('--s3-server-address')
    s3_server_port = pytestconfig.getoption('--s3-server-port')
    s3_server_bucket = pytestconfig.getoption('--s3-server-bucket')

    default_address = os.environ.get('S3_SERVER_ADDRESS_FOR_TEST')
    default_port = os.environ.get('S3_SERVER_PORT_FOR_TEST')
    default_bucket = os.environ.get('S3_PUBLIC_BUCKET_FOR_TEST')

    if s3_server_address:
        server = S3_Server(s3_server_address,
                           s3_server_port,
                           s3_server_bucket)
    elif default_address:
        server = S3_Server(default_address,
                           int(default_port),
                           default_bucket)
    else:
        tempdir = tmpdir.strpath
        server = MinioServer(tempdir,
                             hosts,
                             logging.getLogger('minio'))
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
