"""
Flask Caching S3 adapter

Inspired by https://github.com/pallets-eco/flask-caching/blob/master/src/flask_caching/contrib/googlecloudstoragecache.py
"""
from __future__ import annotations
import typing as t

import datetime
import logging
import io

import boto3
import botocore
from flask_caching.backends.base import BaseCache

if t.TYPE_CHECKING:
    from flask import Flask
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket as S3Bucket


logger = logging.getLogger(__name__)


class S3Cache(BaseCache):
    """
    Uses an AWS S3 bucket as a cache backend.

    Note: Cache keys must meet S3 criteria for a valid object name; see
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html for
    more details.

    Consider an S3 bucket lifecycle rule for managing long-term expirations.
    See https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
    for more details.

    :param bucket: Required. Name of the bucket to use. It must already exist.
    :param key_prefix: A prefix that should be added to all keys, for
                       namespacing purposes.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`S3Cache.set`. A timeout of
                            ``0`` indicates that the cache never expires.
    """

    class InvalidExpirationError(Exception):
        """
        Raised if there's invalid expiration metadata associated to an item.
        """

        pass

    def __init__(
        self,
        bucket: str,
        key_prefix: t.Optional[str] = None,
        default_timeout: int = 300,
        purge_expired_on_read: bool = False,
        **kwargs,
    ):
        super().__init__(default_timeout)

        # Initialize the s3 resource with the required configuration parameters
        self._client: S3ServiceResource = boto3.resource(
            service_name="s3", endpoint_url=kwargs.get("CACHE_S3_ENDPOINT_URL")
        )
        self.bucket: S3Bucket = self._client.Bucket(bucket)
        self.key_prefix = key_prefix or ""
        self.default_timeout = default_timeout
        self.purge_expired_on_read = purge_expired_on_read

    @classmethod
    def factory(
        cls,
        app: Flask,
        config: t.Mapping,
        args: t.MutableSequence,
        kwargs: t.MutableMapping,
    ):
        """
        Instantiate this object with additional arguments
        """
        if config.get("CACHE_S3_BUCKET") is None:
            raise ValueError("You must specify CACHE_S3_BUCKET in your config.")

        args.insert(0, config["CACHE_S3_BUCKET"])
        key_prefix = config.get("CACHE_KEY_PREFIX")
        if key_prefix:
            kwargs["key_prefix"] = key_prefix
        return cls(*args, **kwargs)

    def _utcnow(self) -> datetime.datetime:
        """
        Return a datetime representing the current time for UTC
        """
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    def _normalize_expires(
        self, expires_at: t.Optional[str]
    ) -> t.Optional[datetime.datetime]:
        """
        Convert metadata expiration to usable datetime object
        """
        if expires_at is None:
            return None
        try:
            converted = datetime.datetime.fromtimestamp(
                int(expires_at), tz=datetime.timezone.utc
            )
        except (OverflowError, TypeError):
            logger.error("invalid expiration of %r" % expires_at)
            raise self.InvalidExpirationError()

        return converted

    def get(self, key) -> t.Optional[str]:
        """
        Get the cache value identified by ``key``.

        If the item being retrieved has expired via the metadata expiration
        time that _could_ be added when the item was added to the cache,
        and the ``purge_expired_on_read`` flag is set to ``True``, then we
        purge the expired item from the cache and return a miss.

        :param key: The unique identifier for the relevant item.
        """
        full_key = self.key_prefix + key
        object = self.bucket.Object(full_key)

        try:
            result = object.get()
        except self._client.meta.client.exceptions.NoSuchKey:
            # Does not exist
            logger.debug("get key %r -> miss", full_key)
            return None
        except self._client.meta.client.exceptions.NoSuchBucket:
            # Unauthorized/invalid bucket
            logger.error("get key %r -> unauthorized", full_key)
            return None
        except self._client.meta.client.exceptions.ClientError:
            # In case of an error we can't handle, log it, and fail
            # gracefully. A cache miss is better than a poisoned cache.
            logger.exception("get key %r -> error", full_key)
            return None

        try:
            expires = self._normalize_expires(result["Metadata"].get("expires_at"))
        except self.InvalidExpirationError:
            logger.error(
                "get key %r -> invalid expiration metadata %r, purging"
                % (full_key, result["Metadata"])
            )
            object.delete()
            logger.debug("get key %r -> purged", full_key)
            return None

        if expires is not None and expires < self._utcnow():
            logger.debug("get key %r -> expired", full_key)
            if self.purge_expired_on_read:
                object.delete()
                logger.debug("get key %r -> purged", full_key)
            return None

        return result["Body"].read().decode()

    def set(self, key: str, value: t.Any, timeout: t.Optional[int] = None) -> bool:
        """
        Put ``value`` into into the cache, identified by ``key``, for ``timeout`` seconds.

        :param key: The unique identifier for the relevant item.
        :param value: The data to be cached.
        :param timeout: When the data should expire, in seconds.
        """
        full_key = self.key_prefix + key
        object = self.bucket.Object(full_key)
        timeout = self._normalize_timeout(timeout)

        expires = (
            (self._utcnow() + datetime.timedelta(seconds=timeout))
            if timeout > 0
            else None
        )
        metadata = {"expires_at": str(int(expires.timestamp()))} if expires else {}

        result = object.put(
            Body=io.BytesIO(bytes(value, "utf-8")),
            Metadata=metadata,
        )

        logger.debug("set key %r -> %s", full_key, result)
        return True

    def add(self, key: str, value: t.Any, timeout: t.Optional[int] = None) -> bool:
        """
        Works identically to :meth:`set`, except this does not overwrite the
        value of already existing keys.

        NOTE: This is not immune to race conditions due to how the S3 API/service
              is implemented. It's possible that the interval between the existence
              check and the underlying :meth:`set` operation

        :param key: The unique identifier for the relevant item.
        :param value: The data to be cached.
        :param timeout: When the data should expire, in seconds.
        """
        full_key = self.key_prefix + key
        if self._has(full_key):
            logger.debug("add key %r -> not added", full_key)
            return False
        else:
            return self.set(key, value, timeout)

    def delete(self, key: str) -> bool:
        """
        Delete a single item identified by ``key``.

        :param key: The unique identifier of the item to remove.
        """
        full_key = self.key_prefix + key
        return self._delete(full_key)

    def delete_many(self, *keys: str) -> bool:
        """
        Delete multiple items.

        :param keys: Variable length argument list of key names to delete.
        """
        return self._delete_many([self.key_prefix + key for key in keys])

    def has(self, key: str) -> bool:
        """
        Is the ``key`` present and non-expired in the cache?

        :param key: The unique identifier for the relevant item.
        """
        full_key = self.key_prefix + key
        return self._has(full_key)

    def clear(self) -> bool:
        """
        Delete all cached items.

        Returns boolean on success/failure of clear operation.
        """
        try:
            self.bucket.objects.filter(Prefix=f"{self.key_prefix}").delete()
        except self._client.meta.client.exceptions.ClientError:
            logger.exception(
                "Could not clear %s cache with prefix %s."
                % (self.bucket.name, self.key_prefix)
            )
            return False

        return True

    def _delete(self, key: str) -> bool:
        """
        Delete a single item identified by ``key``

        Returns boolan on success/failure of the operation.

        :param key: The unique identifier for the relevant item
        """
        return self._delete_many([key])

    def _delete_many(self, keys: t.Sequence) -> bool:
        """
        Delete multiple values based on the ``keys`` passed.

        Due to S3 implementation details, deletion operations will always return
        success, even if the key(s) in question do not exist.

        :param keys: Sequence of keys to be removed.
        """
        if not len(keys):
            logger.debug("delete many -> no keys provided, no-op")
            return True

        keys = [{"Key": k} for k in keys]
        result = self._client.meta.client.delete_objects(
            Bucket=self.bucket.name, Delete={"Objects": keys}
        )

        errors = [msg for msg in result.get("Errors", [])]

        # if we have any errors, return false for the whole operation
        if errors:
            for error in errors:
                logger.error(
                    "Could not delete key %s due to error %s: %s"
                    % (error["Key"], error["Code"], error["Message"])
                )
            return False

        return True

    def _has(self, key: str) -> bool:
        """
        Existence check for ``key`` in the S3 bucket.

        If the key represents an _expired_ item, this will return ``False``.

        :param key: The unique identifier for the relevant item.
        """
        object = self.bucket.Object(key)

        # Only fetches metadata; does not fetch actual object ``Body``.
        try:
            object.load()
        except self._client.meta.client.exceptions.NoSuchKey:
            # Does not exist
            logger.debug("has key %r -> miss", key)
            return False
        except self._client.meta.client.exceptions.NoSuchBucket:
            # Unauthorized/invalid bucket
            logger.error("has key %r -> unauthorized", key)
            return False
        except self._client.meta.client.exceptions.ClientError:
            # In case of an error we can't handle, log it, and fail
            # gracefully. A cache miss is better than a poisoned cache.
            logger.exception("has key %r -> error", key)
            return False

        try:
            expires = self._normalize_expires(object.metadata.get("expires_at"))
        except self.InvalidExpirationError:
            logger.error(
                "has key %r -> invalid expiration metadata %r, purging"
                % (key, object.metadata)
            )
            object.delete()
            logger.debug("get key %r -> purged", key)
            return False

        if expires is not None and expires < self._utcnow():
            logger.debug("has key %r -> expired", key)
            if self.purge_expired_on_read:
                object.delete()
                logger.debug("has key %r -> purged", key)

            return False

        logger.debug("has key %r", key)
        return True
