"""
Distributed locks with Redis
Redis doc: http://redis.io/topics/distlock
"""
from __future__ import division
from datetime import datetime
import random
import time
import uuid

import redis


# Reference:  http://redis.io/topics/distlock
# Section Correct implementation with a single instance
RELEASE_LUA_SCRIPT = """
    if redis.call("get",KEYS[1]) == ARGV[1] then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
"""


class RedLockError(Exception):
    pass


class RedLockFactory(object):
    """
    A Factory class that helps reuse multiple Redis connections.
    """

    def __init__(self, connection_details):
        """

        """
        self.redis_nodes = []

        for conn in connection_details:
            if isinstance(conn, redis.StrictRedis):
                node = conn
            elif 'url' in conn:
                url = conn.pop('url')
                node = redis.StrictRedis.from_url(url, **conn)
            else:
                node = redis.StrictRedis(**conn)
            node._release_script = node.register_script(RELEASE_LUA_SCRIPT)
            self.redis_nodes.append(node)
            self.quorum = len(self.redis_nodes) // 2 + 1

    def create_lock(self, resource, **kwargs):
        """
        Create a new RedLock object and reuse stored Redis clients.
        All the kwargs it received would be passed to the RedLock's __init__
        function.
        """
        lock = RedLock(resource=resource, created_by_factory=True, **kwargs)
        lock.redis_nodes = self.redis_nodes
        lock.quorum = self.quorum
        lock.factory = self
        return lock


class RedLock(object):

    """
    A distributed lock implementation based on Redis.
    It shares a similar API with the `threading.Lock` class in the
    Python Standard Library.
    """

    DEFAULT_RETRY_TIMES = 3
    DEFAULT_RETRY_DELAY = 200
    DEFAULT_TTL = 100000
    CLOCK_DRIFT_FACTOR = 0.01

    def __init__(self, resource, connection_details=None,
                 retry_times=None, retry_delay=None, ttl=None,
                 created_by_factory=False,
                 key=None):

        self.resource = resource
        self.retry_times = retry_times or self.DEFAULT_RETRY_TIMES
        self.retry_delay = retry_delay or self.DEFAULT_RETRY_DELAY
        self.ttl = ttl or self.DEFAULT_TTL
        if not key:
            # lock_key should be random and unique
            self.lock_key = uuid.uuid4().hex
        else:
            # To enable release of an externally stored key
            self.lock_key = key
        # In python3 the lock_key must be a instance bytes (which is
        # in python2 the same as str)
        if isinstance(self.lock_key, str):
            self.lock_key = self.lock_key.encode()

        if created_by_factory:
            self.factory = None
            return

        self.redis_nodes = []
        # If the connection_details parameter is not provided,
        # use redis://127.0.0.1:6379/0
        if connection_details is None:
            connection_details = [{
                'host': 'localhost',
                'port': 6379,
                'db': 0,
            }]

        for conn in connection_details:
            if isinstance(conn, redis.StrictRedis):
                node = conn
            elif 'url' in conn:
                url = conn.pop('url')
                node = redis.StrictRedis.from_url(url, **conn)
            else:
                node = redis.StrictRedis(**conn)
            node._release_script = node.register_script(RELEASE_LUA_SCRIPT)
            self.redis_nodes.append(node)
        self.min_quorum = len(self.redis_nodes) // 2 + 1
        self.max_quorum = len(self.redis_nodes)
        self.quorum = self.min_quorum
        self.lock_acquired = False

    def __enter__(self):
        acquired, validity = self.acquire_with_validity()
        if not acquired:
            raise RedLockError('failed to acquire lock')
        return validity

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def _total_ms(self, delta):
        """
        Get the total number of milliseconds in a timedelta object with
        microsecond precision.
        """
        delta_seconds = delta.seconds + delta.days * 24 * 3600
        return (delta.microseconds + delta_seconds * 10**6) / 10**3

    def set_quorum(self, quorum=None):
        """
        Set the quorum
        """
        if self.lock_acquired:
            raise RedLockError("Cannot set quorum when lock is acquired.")
        self.quorum = min(max(self.max_quorum, quorum), self.max_quorum)

    def acquire_node(self, node):
        """
        acquire a single redis node
        """
        try:
            return node.set(self.resource, self.lock_key, nx=True, px=self.ttl)
        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError):
            return False

    def release_node(self, node):
        """
        release a single redis node
        """
        # use the lua script to release the lock in a safe way
        try:
            node._release_script(keys=[self.resource], args=[self.lock_key])
        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError):
            pass

    def info_node(self, node):
        """
        Get lock info from a single node
        """
        value = ttl = None
        try:
            value = node.get(self.resource)
            ttl = node.pttl(self.resource)
        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError):
            pass
        return (value, ttl)

    def extend_node(self, node, ttl=None):
        """
        Extend lock (set new ttl to ttl or self.ttl)
        """
        if not ttl:
            ttl = self.ttl
        acquired = self.acquire_node(node)
        try:
            if acquired or node.get(self.resource) == self.lock_key:
                if node.pexpire(self.resource, ttl):
                    return True
        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError):
            pass
        if acquired:
            return True
        return False

    def acquire(self):
        acquired, validity = self._acquire()
        return acquired

    def acquire_with_validity(self):
        return self._acquire()

    def _acquire(self):
        """
        acquire a lock on at least quorum redis nodes
        """

        self.lock_acquired = False
        for retry in range(self.retry_times + 1):
            acquired_node_count = 0
            start_time = datetime.utcnow()

            # acquire the lock in all the redis instances sequentially
            for node in self.redis_nodes:
                if self.acquire_node(node):
                    acquired_node_count += 1

            end_time = datetime.utcnow()
            elapsed_milliseconds = self._total_ms(end_time - start_time)

            # Add 2 milliseconds to the drift to account for Redis expires
            # precision, which is 1 milliscond, plus 1 millisecond min drift
            # for small TTLs.
            drift = (self.ttl * self.CLOCK_DRIFT_FACTOR) + 2

            validity = self.ttl - (elapsed_milliseconds + drift)
            if acquired_node_count >= self.quorum and validity > 0:
                self.lock_acquired = True
                return True, validity
            else:
                for node in self.redis_nodes:
                    self.release_node(node)
                time.sleep(random.randint(0, self.retry_delay) / 1000)
        return False, 0

    def release(self):
        """
        Release lock on all redis nodes
        """
        for node in self.redis_nodes:
            self.release_node(node)
        self.lock_acquired = False

    def holding(self):
        """
        Check if this lock is acquired
        """
        acquired_node_count = 0
        for node in self.redis_nodes:
            value, ttl = self.info_node(node)
            if value == self.lock_key:
                acquired_node_count += 1
        if acquired_node_count >= self.quorum:
            self.lock_acquired = True
            return True
        for node in self.redis_nodes:
            self.release_node(node)
        self.lock_acquired = False
        return False

    def info(self):
        """
        Get lock_key and remaining ttl of the lock on all redis nodes
        """
        return list([
            self.info_node(node)
            for node in self.redis_nodes
        ])

    def extend(self, ttl=None):
        acquired, validity = self._extend(ttl)
        return acquired

    def extend_with_validity(self, ttl=None):
        return self._extend(ttl)

    def _extend(self, ttl=None):
        """
        Extend the previously acquired lock on all redis nodes
        """
        acquired_node_count = 0
        start_time = datetime.utcnow()
        # Extend the lock ttls
        for node in self.redis_nodes:
            if self.extend_node(node, ttl):
                acquired_node_count += 1
        end_time = datetime.utcnow()
        elapsed_milliseconds = self._total_ms(end_time - start_time)

        # Add 2 milliseconds to the drift to account for Redis expires
        # precision, which is 1 milliscond, plus 1 millisecond min drift
        # for small TTLs.
        drift = (self.ttl * self.CLOCK_DRIFT_FACTOR) + 2

        validity = self.ttl - (elapsed_milliseconds + drift)
        if acquired_node_count >= self.quorum and validity > 0:
            self.lock_acquired = True
            return True, validity
        for node in self.redis_nodes:
            self.release_node(node)
        self.lock_acquired = False
        return False, 0


class ReentrantRedLock(RedLock):

    def __init__(self, *args, **kwargs):
        super(ReentrantRedLock, self).__init__(*args, **kwargs)
        self._acquired = 0

    def acquire(self):
        if self._acquired == 0:
            result = super(ReentrantRedLock, self).acquire()
            if result:
                self._acquired += 1
            return result
        else:
            self._acquired += 1
            return True

    def release(self):
        if self._acquired > 0:
            self._acquired -= 1
            if self._acquired == 0:
                return super(ReentrantRedLock, self).release()
            return True
        return False

    def extend(self, ttl=None):
        if self._acquired == 0:
            result = super(ReentrantRedLock, self).extend(ttl)
            if result:
                self._acquired += 1
            return result
        else:
            self._acquired += 1
            return True
