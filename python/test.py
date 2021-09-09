import logging
from urllib.parse import urlparse

from redis import Redis
from redis._compat import nativestr
from redis.connection import UnixDomainSocketConnection
from rediscluster import RedisCluster
from rediscluster.connection import ClusterWithReadReplicasConnectionPool, ClusterParser
from rediscluster.nodemanager import NodeManager

logger = logging.getLogger(__name__)


def redisbetween_socket_path(host, port, db, readonly):
    rb_socket_path = f"/var/tmp/redisbetween-{host}-{port}"
    if db is not None:
        rb_socket_path += f"-{db}"
    if readonly:
        rb_socket_path += "-ro"
    rb_socket_path += ".sock"
    return rb_socket_path


class RedisbetweenReadOnlyNodeManager(NodeManager):
    def get_redis_link(self, host, port, decode_responses=False, read_from_replicas=False):
        return Redis(
            decode_responses=decode_responses,
            unix_socket_path=redisbetween_socket_path(host, port, None, read_from_replicas),
        )


class RedisbetweenReadonlyClusterConnection(UnixDomainSocketConnection):
    def __init__(self, *args, **kwargs):
        host, port, path = (
            kwargs.pop("host", "localhost"),
            kwargs.pop("port", 6379),
            kwargs.pop("db", None),
        )
        kwargs["path"] = redisbetween_socket_path(host, port, path, True)
        self.readonly = kwargs.pop('readonly', False)
        kwargs['parser_class'] = ClusterParser
        super(RedisbetweenReadonlyClusterConnection, self).__init__(*args, **kwargs)

    def on_connect(self):
        """
        Initialize the connection, authenticate and select a database and send READONLY if it is
        set during object initialization. Copied from redis-py-cluster ClusterConnection class.
        """
        super(UnixDomainSocketConnection, self).on_connect()
        if self.readonly:
            self.send_command("READONLY")
            if nativestr(self.read_response()) != "OK":
                raise ConnectionError("READONLY command failed")


class RedisbetweenClusterWithReadReplicasConnectionPool(ClusterWithReadReplicasConnectionPool):
    def __init__(
            self,
            startup_nodes=None,
            init_slot_cache=True,
            connection_class=None,
            max_connections=None,
            max_connections_per_node=False,
            reinitialize_steps=None,
            skip_full_coverage_check=True,
            nodemanager_follow_cluster=False,
            host_port_remap=None,
            **connection_kwargs,
    ):
        read_from_replicas = connection_kwargs.pop("read_from_replicas", False)
        super(RedisbetweenClusterWithReadReplicasConnectionPool, self).__init__(
            startup_nodes=startup_nodes,
            init_slot_cache=False,  # prevent the original NodeManager instance from trying to connect
            skip_full_coverage_check=skip_full_coverage_check,
            connection_class=RedisbetweenReadonlyClusterConnection,
            **connection_kwargs,
        )
        self.nodes = RedisbetweenReadOnlyNodeManager(
            startup_nodes=startup_nodes,
            skip_full_coverage_check=skip_full_coverage_check,
            read_from_replicas=read_from_replicas,
            **connection_kwargs,
        )
        self.nodes.initialize()

    @classmethod
    def from_url(cls, url, db=None, decode_components=False, **kwargs):
        parsed = urlparse(url)
        return RedisbetweenClusterWithReadReplicasConnectionPool(
            startup_nodes=[{"host": parsed.hostname, "port": parsed.port}],
            skip_full_coverage_check=True,
        )


startup_nodes = [{"host": "127.0.0.1", "port": 7000}]
redis_client = RedisCluster(
    startup_nodes=startup_nodes,
    decode_responses=True,
    skip_full_coverage_check=True,
    connection_class=RedisbetweenReadonlyClusterConnection,
    connection_pool=(RedisbetweenClusterWithReadReplicasConnectionPool(
        startup_nodes=startup_nodes,
        skip_full_coverage_check=True,
    ))
)
ro_redis_client = RedisCluster(
    read_from_replicas=True,
    startup_nodes=startup_nodes,
    decode_responses=True,
    skip_full_coverage_check=True,
    connection_class=RedisbetweenReadonlyClusterConnection,
    connection_pool=(RedisbetweenClusterWithReadReplicasConnectionPool(
        startup_nodes=startup_nodes,
        skip_full_coverage_check=True,
        read_from_replicas=True,
    ))
)

for i in range(100):
    redis_client.set(f"hello{i}", i)

for i in range(100):
    first = redis_client.get(f"hello{i}")
    for j in range(5):
        if first != ro_redis_client.get(f"hello{i}"):
            print(f"err on {j} of {i}")
