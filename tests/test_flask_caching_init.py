"""
Flask-Caching initialization/configuration tests for S3Cache backend
"""

from __future__ import annotations
import typing as t

from flask_caching import Cache

if t.TYPE_CHECKING:
    from flask import Flask


def test_initialize_cache_extension_with_import_path(flask_app: Flask):
    """
    Initialize Flask-Caching with S3Cache backend.

    This should _not_ trigger any sort of query to AWS S3; it only
    requires that the ``CACHE_S3_BUCKET`` config variable be set.
    """

    flask_app.config.update(
        {
            "CACHE_S3_BUCKET": "library-of-alexandria",
            "CACHE_TYPE": "flask_caching_s3.S3Cache",
        }
    )
    cache = Cache()
    cache.init_app(flask_app)


def test_initialize_cache_extension_with_direct_configuration(flask_app: Flask):
    """
    Initialize ``Flask-Caching`` by passing config options to ``init_app`` directly.
    """
    cache = Cache()
    cache.init_app(
        flask_app,
        {
            "CACHE_TYPE": "flask_caching_s3.S3Cache",
            "CACHE_S3_BUCKET": "library-of-alexandria",
        },
    )

    assert cache.app == flask_app
