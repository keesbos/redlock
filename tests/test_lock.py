from redlock import RedLock, ReentrantRedLock, RedLockError
import mock
import time
import unittest

CLOCK_DRIFT_FACTOR = RedLock.CLOCK_DRIFT_FACTOR


def test_default_connection_details_value():
    """
    Test that RedLock instance could be created with
    default value of `connection_details` argument.
    """
    lock = RedLock("test_simple_lock")  # NOQA


def test_simple_lock():
    """
    Test a RedLock can be acquired.
    """
    lock = RedLock("test_simple_lock", [{"host": "localhost"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True


def test_lock_with_validity():
    """
    Test a RedLock can be acquired and the lock validity is also retruned.
    """
    ttl = 1000
    lock = RedLock("test_simple_lock", [{"host": "localhost"}], ttl=ttl)
    locked, validity = lock.acquire_with_validity()
    lock.release()
    assert locked is True
    assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2


def test_from_url():
    """
    Test a RedLock can be acquired via from_url.
    """
    lock = RedLock("test_from_url", [{"url": "redis://localhost/0"}], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True


def test_context_manager():
    """
    Test a RedLock can be released by the context manager automically.

    """
    ttl = 1000
    with RedLock(
        "test_context_manager", [{"host": "localhost"}], ttl=ttl
    ) as validity:
        assert 0 < validity < ttl - ttl * CLOCK_DRIFT_FACTOR - 2
        lock = RedLock(
            "test_context_manager", [{"host": "localhost"}], ttl=ttl
        )
        locked = lock.acquire()
        assert locked is False

    lock = RedLock("test_context_manager", [{"host": "localhost"}], ttl=ttl)
    locked = lock.acquire()
    assert locked is True

    # try to lock again within a with block
    try:
        with RedLock("test_context_manager", [{"host": "localhost"}]):
            # shouldn't be allowed since someone has the lock already
            assert False
    except RedLockError:
        # we expect this call to error out
        pass

    lock.release()


def test_fail_to_lock_acquired():
    lock1 = RedLock(
        "test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000
    )
    lock2 = RedLock(
        "test_fail_to_lock_acquired", [{"host": "localhost"}], ttl=1000
    )

    lock1_locked = lock1.acquire()
    lock2_locked = lock2.acquire()
    lock1.release()

    assert lock1_locked is True
    assert lock2_locked is False


def test_lock_expire():
    lock1 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=500)
    lock1.acquire()
    time.sleep(1)

    # Now lock1 has expired, we can accquire a lock
    lock2 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock2.acquire()
    assert locked is True

    lock1.release()
    lock3 = RedLock("test_lock_expire", [{"host": "localhost"}], ttl=1000)
    locked = lock3.acquire()
    assert locked is False


def test_relock():
    lock1 = RedLock("test_relock", [{"host": "localhost"}], ttl=500)
    lock1.acquire()
    assert lock1.holding()
    time.sleep(1)
    assert not lock1.holding()

    lock1.extend(500)
    assert lock1.holding()
    time.sleep(1)
    assert not lock1.holding()

    lock2 = RedLock(
        "test_relock", [{"host": "localhost"}], ttl=500, key=lock1.lock_key
    )
    lock2.acquire()
    assert lock2.holding()
    time.sleep(1)
    assert not lock2.holding()

    lock3 = RedLock("test_relock", [{"host": "localhost"}], ttl=2000)
    lock3.acquire()
    assert lock3.holding()
    lock2.extend(500)
    assert not lock2.holding()
    assert lock3.holding()
    assert lock3.info()[0][1] <= 2000
    lock3.extend(3000)
    assert lock3.info()[0][1] > 2000
    lock3.release()


class TestLock(unittest.TestCase):
    def setUp(self):
        super(TestLock, self).setUp()
        self.redlock = mock.patch.object(
            RedLock, '__init__', return_value=None
        ).start()
        self.redlock_acquire = mock.patch.object(RedLock, 'acquire').start()
        self.redlock_release = mock.patch.object(RedLock, 'release').start()
        self.redlock_acquire.return_value = True

    def tearDown(self):
        mock.patch.stopall()

    def test_passthrough(self):
        test_lock = ReentrantRedLock('')
        test_lock.acquire()
        test_lock.release()

        self.redlock.assert_called_once_with('')
        self.redlock_acquire.assert_called_once_with()
        self.redlock_release.assert_called_once_with()

    def test_reentrant(self):
        test_lock = ReentrantRedLock('')
        test_lock.acquire()
        test_lock.acquire()
        test_lock.release()
        test_lock.release()

        self.redlock.assert_called_once_with('')
        self.redlock_acquire.assert_called_once_with()
        self.redlock_release.assert_called_once_with()

    def test_reentrant_n(self):
        test_lock = ReentrantRedLock('')
        for _ in range(10):
            test_lock.acquire()
        for _ in range(10):
            test_lock.release()

        self.redlock.assert_called_once_with('')
        self.redlock_acquire.assert_called_once_with()
        self.redlock_release.assert_called_once_with()

    def test_no_release(self):
        test_lock = ReentrantRedLock('')
        test_lock.acquire()
        test_lock.acquire()
        test_lock.release()

        self.redlock.assert_called_once_with('')
        self.redlock_acquire.assert_called_once_with()
        self.redlock_release.assert_not_called()


def test_lock_with_multi_backend():
    """
    Test a RedLock can be acquired when at least N/2+1 redis instances are
    alive.
    Set redis instance with port 6380 down or debug sleep during test.
    """
    lock = RedLock("test_simple_lock", connection_details=[
        {"host": "localhost", "port": 6379, "db": 0, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6379, "db": 1, "socket_timeout": 0.2},
        {"host": "localhost", "port": 6380, "db": 0, "socket_timeout": 0.2}
        ], ttl=1000)
    locked = lock.acquire()
    lock.release()
    assert locked is True
