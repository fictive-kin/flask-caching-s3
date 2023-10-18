"""
Flask-Caching integration tests

These tests require a localstack container to be running; by default this will
be started by the relevant test fixtures, which read the sibling
``docker-compose.yml`` to determine which services to enable.

"""

from __future__ import annotations

import datetime
import typing as t
import io

import pytest

from flask_caching import Cache
from freezegun import freeze_time


if t.TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Object as S3Object
    from mypy_boto3_s3.service_resource import Bucket as S3Bucket


class CachedItemMaker(t.Protocol):
    def __call__(
        self,
        bucket: S3Bucket,
        key: str,
        value: t.Any,
        expires_at: t.Optional[datetime.datetime] = None,
    ) -> S3Object:
        ...


@pytest.fixture(scope="function")
def cache_item(
    default_cache_prefix: str,
) -> CachedItemMaker:
    """Factory fixture for quickly adding an item to S3"""

    def add_item_to_s3(
        bucket: S3Bucket,
        key: str,
        value: t.Any,
        expires_at: t.Optional[datetime.datetime] = None,
    ) -> S3Object:
        object = bucket.Object(key=f"{default_cache_prefix}{key}")
        metadata = {}

        if expires_at is not None:
            metadata = {"expires_at": str(int(expires_at.timestamp()))}

        object.put(Body=io.BytesIO(bytes(value, "utf-8")), Metadata=metadata)
        return object

    return add_item_to_s3


def test_simple_cache_set(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """
    Set an item in the cache, and verify that it has been added by querying S3
    directly.

    Default expiration time is set.
    """

    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )

    with freeze_time(now):
        result = cache.set("key", "something I'd like to cache")
        assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}key").get()

    # Raw data from S3 is in streaming bytes, so let's read it.
    value = stored["Body"].read()
    assert value.decode() == "something I'd like to cache"

    expiration = datetime.datetime.fromtimestamp(
        int(stored["Metadata"]["expires_at"]), tz=datetime.timezone.utc
    )

    assert now < expiration
    assert expiration == (now + datetime.timedelta(seconds=300))


def test_simple_cache_get(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
):
    """
    Get an item that has already been persisted to the cache.

    For the purposes of this test, no expiration is set.
    """

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    cache_item(default_bucket, key=key, value=value)

    result = cache.get(key)
    assert result == value


def test_simple_cache_get_missing_item(cache: Cache, default_bucket: S3Bucket):
    """
    Attempt to get an item that doesn't exist in the cache.
    """

    key = "missing_key"
    result = cache.get(key)
    assert result is None


def test_simple_cache_get_expired_item(
    cache: Cache, default_bucket: S3Bucket, cache_item: CachedItemMaker
):
    """
    Attempt to get an item that exists in the cache but has expired.
    """

    key = "expiration_test_key"
    value = "This value is no longer valid."

    # compute expiration time that is set in the past
    yesterday = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    ) - datetime.timedelta(days=1)

    object = cache_item(default_bucket, key=key, value=value, expires_at=yesterday)

    result = cache.get(key)
    assert result is None

    # Item should not be purged, by default, so it should still exist in S3
    assert object.get()["Body"].read().decode() == value


def test_simple_cache_get_expired_item_purge(
    purging_cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
    s3: S3ServiceResource,
):
    """
    Attempt to get an item that exists in the cache but has expired, which
    will purge the item from S3.
    """

    key = "expiration_test_key"
    value = "This value is no longer valid."

    # compute expiration time that is set in the past
    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )
    yesterday = now - datetime.timedelta(days=1)
    object = cache_item(default_bucket, key, value, expires_at=yesterday)

    result = purging_cache.get(key)
    assert result is None

    with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
        object.get()
        # default_bucket.Object(key=f"{default_cache_prefix}{key}").get()


def test_simple_cache_add(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """
    Add an item to the cache.

    The difference between ``add`` and ``set`` is that the former (the one
    that is under test here) will _not_ overwrite a key that already exists.

    If the key already exists, ``False`` will be returned, and the value will
    not be updated.
    """

    key = "hamlet_5_2"
    expected_value = "The rest is silence."

    result = cache.add(key, expected_value)
    assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    # Raw data from S3 is in streaming bytes, so let's read it.
    data = stored["Body"].read()
    assert data.decode() == expected_value

    # Now attempt to call cache.add() again with the same key, but
    # a different value

    result = cache.add(key, "Goodnight, sweet prince")
    assert result is False

    # check that the original value hasn't been overwritten.
    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    data = stored["Body"].read()
    assert data.decode() == expected_value


def test_cache_set_with_explicit_expiration(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """Set an item in the cache with an explicit expiration"""

    one_day = 60 * 60 * 24
    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )

    with freeze_time(now):
        result = cache.set("key", "something I'd like to cache", timeout=one_day)
        assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}key").get()
    expiration = datetime.datetime.fromtimestamp(
        int(stored["Metadata"]["expires_at"]), tz=datetime.timezone.utc
    )

    assert now < expiration
    assert expiration == (now + datetime.timedelta(seconds=one_day))


def test_cache_set_with_explicit_forever_timeout(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """Forever actually means 2000 days due to S3 requiring a valid datetime"""

    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )

    with freeze_time(now):
        result = cache.set("key", "something I'd like to cache", timeout=0)
        assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}key").get()
    value = stored["Body"].read()
    assert value.decode() == "something I'd like to cache"

    # No expires_at means never expires
    assert "expires_at" not in stored["Metadata"]


def test_cache_add_with_explicit_expiration(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """
    Add an item in the cache with an explicit expiration

    Remember: adding is different than ``set``, because ``add`` will not
              overwrite existing keys/values.
    """

    key = "hamlet_5_2"
    expected_value = "The rest is silence."

    one_day = 60 * 60 * 24
    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )

    with freeze_time(now):
        result = cache.add(key, expected_value, timeout=one_day)
    assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    data = stored["Body"].read()
    assert data.decode() == expected_value

    # Now attempt to call cache.add() again with the same key, but
    # a different value
    result = cache.add(key, "Goodnight, sweet prince")
    assert result is False

    # check that the original value hasn't been overwritten.
    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    expiration = datetime.datetime.fromtimestamp(
        int(stored["Metadata"]["expires_at"]), tz=datetime.timezone.utc
    )
    data = stored["Body"].read()

    assert data.decode() == expected_value

    assert now < expiration
    assert expiration == (now + datetime.timedelta(seconds=one_day))


def test_cache_add_with_explicit_forever_timeout(
    cache: Cache, default_bucket: S3Bucket, default_cache_prefix: str
):
    """
    Add an item in the cache with a timeout of ``0`` which means that the
    item should never expire.

    Remember: adding is different than ``set``, because ``add`` will not
              overwrite existing keys/values.
    """

    key = "hamlet_5_2"
    expected_value = "The rest is silence."

    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )

    with freeze_time(now):
        result = cache.add(key, expected_value, timeout=0)
    assert result is True

    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    data = stored["Body"].read()
    assert data.decode() == expected_value

    # Now attempt to call cache.add() again with the same key, but
    # a different value
    result = cache.add(key, "Goodnight, sweet prince")
    assert result is False

    # check that the original value hasn't been overwritten.
    stored = default_bucket.Object(key=f"{default_cache_prefix}{key}").get()
    data = stored["Body"].read()

    assert data.decode() == expected_value
    assert "expires_at" not in stored["Metadata"]


def test_cache_delete_single_item(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
    s3: S3ServiceResource,
):
    """Remove an item from the cache that has not expired."""

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    now = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    )
    object = cache_item(
        default_bucket, key, value, expires_at=(now + datetime.timedelta(seconds=300))
    )

    result = cache.delete(key)
    assert result is True

    with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
        object.get()


def test_cache_delete_expired_item(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
    s3: S3ServiceResource,
):
    """Remove an item from the cache that has already expired."""

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    yesterday = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    ) - datetime.timedelta(days=1)
    object = cache_item(default_bucket, key, value, expires_at=yesterday)

    result = cache.delete(key)
    assert result is True

    with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
        object.get()


def test_cache_delete_single_item_not_in_cache(cache: Cache, default_bucket: S3Bucket):
    """
    Attempt to remove an item from the cache that does not exist.

    Note: S3 semantics mean that deleting a key that does not exist will not have
    a response that materially differs from deleting a key that does exist, so we
    return success in all cases unless there is an underlying error in the operation.
    """

    key = "fake_item"
    result = cache.delete(key)
    assert result is True


def test_cache_delete_multiple_items(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
    s3: S3ServiceResource,
):
    """Remove multiple items from the cache."""

    data = [("one", "existing value 1"), ("two", "existing value 2")]
    objects = []
    for item in data:
        objects.append(cache_item(default_bucket, item[0], item[1]))

    result = cache.delete_many("one", "two")
    assert result is True

    # Each item will raise NoSuchKey exception because they have been
    # deleted from S3
    for obj in objects:
        with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
            obj.get()


def test_cache_has_item_that_expired(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
):
    """
    If an item is expired, the check will return False.
    """

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    yesterday = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    ) - datetime.timedelta(days=1)

    # Put object and set expieration to yesterday
    object = cache_item(default_bucket, key, value, expires_at=yesterday)

    result = cache.has(key)
    assert result is False

    # Reading the expired item should _not_ purge it by default, and it should
    # still be in S3
    assert object.get()["Body"].read().decode() == value


def test_cache_has_item_that_expired_purge(
    purging_cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
    s3: S3ServiceResource,
):
    """
    If an item is expired when we perform cache.has(key), and the cache
    is configured to purge expired items, remove the item.
    """

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    yesterday = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    ) - datetime.timedelta(days=1)

    object = cache_item(default_bucket, key, value, expires_at=yesterday)

    result = purging_cache.has(key)
    assert result is False

    # Item should no longer exist in S3 after being purged
    with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
        object.get()


def test_cache_has_item_not_expired(
    cache: Cache,
    default_bucket: S3Bucket,
    cache_item: CachedItemMaker,
):
    """
    Check that an item exists in the cache, and that item has not expired yet.
    """

    key = "polonius_2_2"
    value = "Brevity is the soul of wit."
    tomorrow = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc, microsecond=0
    ) + datetime.timedelta(days=1)

    # Put object and set expieration to yesterday
    cache_item(default_bucket, key, value, expires_at=tomorrow)

    result = cache.has(key)
    assert result is True


def test_cache_clear(
    cache: Cache,
    default_bucket: S3Bucket,
    s3: S3ServiceResource,
    cache_item: CachedItemMaker,
):
    """Clear out all items from the cache that have the configured cache prefix."""

    # put several items into the cache so that we can clear them out
    data = [("one", "existing value 1"), ("two", "existing value 2")]
    objects = []
    for item in data:
        objects.append(cache_item(default_bucket, item[0], item[1]))

    # put an item that doesn't have the common prefix to ensure that we don't
    # mistakenly delete it
    prefixless_key = "whodis"
    prefixless_value = "call me maybe"

    prefixless_object = default_bucket.Object(key=f"{prefixless_key}")
    prefixless_object.put(Body=io.BytesIO(bytes(prefixless_value, "utf-8")))

    result = cache.clear()
    assert result is True

    # Ensure prefixed data was cleared
    for obj in objects:
        with pytest.raises(s3.meta.client.exceptions.NoSuchKey):
            obj.get()

    # Make sure we didn't clear out unprefixed data
    assert prefixless_object.get()["Body"].read().decode() == prefixless_value
