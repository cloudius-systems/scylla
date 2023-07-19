import logging
import time
import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import read_barrier, wait_for_cql_and_get_hosts


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_old_ip_notification_repro(manager: ManagerClient) -> None:
    """
    Regression test for #14257.
    It starts two nodes. It introduces a sleep in raft_group_registry::on_alive
    (in raft_group_registry.cc) when receiving a gossip notification about
    HOST_ID update from the second node. Then it restarts the second node with
    a different IP. Due to the sleep, the old notification from the old IP arrives
    after the second node has restarted. If the bug is present, this notification
    overrides the address map entry and the second read barrier times out, since
    the first node cannot reach the second node with the old IP.
    """
    s1 = await manager.server_add()
    s2 = await manager.server_add(start=False)
    async with inject_error(manager.api, s1.ip_addr, 'raft_group_registry::on_alive',
                            parameters={ "second_node_ip": s2.ip_addr }) as handler:
        # This injection delays the gossip notification from the initial IP of s2.
        logger.info(f"Starting {s2}")
        await manager.server_start(s2.server_id)
        logger.info(f"Stopping {s2}")
        await manager.server_stop_gracefully(s2.server_id)
        await manager.server_change_ip(s2.server_id)
        logger.info(f"Starting {s2}")
        await manager.server_start(s2.server_id)
        cql = manager.get_cql()
        logger.info(f"Wait for cql")
        h1 = (await wait_for_cql_and_get_hosts(cql, [s1], time.time() + 60))[0]
        logger.info(f"Read barrier")
        await read_barrier(cql, h1) # Wait for s1 to be aware of s2 with the new IP.
        await handler.message() # s1 receives the gossip notification from the initial IP of s2.
        logger.info(f"Read barrier")
        # If IP of s2 is overridden by its initial IP, the read barrier should time out.
        await read_barrier(cql, h1)
        logger.info(f"Done")
