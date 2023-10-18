"""
Basic test fixtures

"""

from __future__ import annotations
import typing as t
import pytest

from localstack_client.patch import enable_local_endpoints
from flask import Flask
from flask_caching import Cache

import boto3

if t.TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
    from mypy_boto3_s3.service_resource import Bucket as S3Bucket

T = t.TypeVar("T")
YieldFixture = t.Generator[T, None, None]


@pytest.fixture(autouse=True)
def patch_boto3_for_localstack():
    """
    Mock out any boto3 connections to use localstack
    """
    yield enable_local_endpoints()


@pytest.fixture(scope="session")
def docker_localstack(docker_services):
    """Start the localstack service for the integration tests"""

    docker_services.start("localstack")
    public_port = docker_services.wait_for_service("localstack", 4566)
    return f"{docker_services.docker_ip}:{public_port}"


@pytest.fixture
def flask_app() -> YieldFixture[Flask]:
    """Simple ``Flask`` application for integration testing."""

    yield Flask("flask-caching-s3-test")


@pytest.fixture
def default_bucket_name() -> str:
    """Default name of bucket for tests/fixtures to use."""
    return "library-of-alexandria"


@pytest.fixture()
def default_cache_prefix() -> str:
    """Default cache prefix."""
    return "test_prefix_"


@pytest.fixture
def cache(
    flask_app: Flask, default_bucket_name: str, default_cache_prefix: str
) -> YieldFixture[Cache]:
    """Initialize cache"""

    cache = Cache()
    cache.init_app(
        flask_app,
        {
            "CACHE_TYPE": "flask_caching_s3.S3Cache",
            "CACHE_S3_BUCKET": default_bucket_name,
            "CACHE_KEY_PREFIX": default_cache_prefix,
        },
    )
    yield cache


@pytest.fixture
def purging_cache(
    flask_app: Flask, default_bucket_name: str, default_cache_prefix: str
) -> YieldFixture[Cache]:
    """Initialize cache that purges expired items on read"""

    cache = Cache()
    cache.init_app(
        flask_app,
        {
            "CACHE_TYPE": "flask_caching_s3.S3Cache",
            "CACHE_S3_BUCKET": default_bucket_name,
            "CACHE_KEY_PREFIX": default_cache_prefix,
            "CACHE_OPTIONS": {"purge_expired_on_read": True},
        },
    )
    yield cache


@pytest.fixture
def s3(docker_localstack) -> YieldFixture[S3ServiceResource]:
    """A boto3 resource to use for the test suite."""

    yield boto3.resource("s3")


@pytest.fixture(scope="function")
def default_bucket(
    s3: S3ServiceResource, default_bucket_name: str
) -> YieldFixture[S3Bucket]:
    """
    Create the default bucket to be used for tests, and clean up after
    ourselves.
    """
    bucket_handle = s3.create_bucket(Bucket=default_bucket_name)

    yield bucket_handle

    # Clean up
    bucket_handle.objects.all().delete()
    bucket_handle.delete()
