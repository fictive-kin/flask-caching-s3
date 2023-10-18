AWS S3 Backend for Flask-Cache
==============================

When installed and configured, this package will allow you to utilize an AWS S3 bucket as the backend for `Flask-Cache <https://flask-caching.readthedocs.io/en/latest/>`_.

Yes, S3 is not a very good cache. But if millisecond performance is not a concern, S3 is a  low-effort and simple to implement alternative that is far less costly than Redis or Memcache.

Installation
------------

Install via ``pip`` directly, or whatever your preferred dependency/package environment manager may be (``poetry``, ``pipenv``, ...):

.. code-block:: shell

    pip install flask-cache-s3

Configuration
-------------

Configuration is straightforward, and is identical to the usual process for configuring ``Flask-Cache``. Note that the `mechanism by which you choose to configure <https://flask.palletsprojects.com/en/3.0.x/config/>`_ is up to you; the example below is simply for illustrative purposes:

.. code-block:: python
    from flask import Flask
    from flask_caching import Cache

    config = {
        # The import path of the S3 cache backend
        "CACHE_TYPE": "flask_caching_s3.S3Cache",
        # The bucket name where you'd like cache artifacts to be stored/queried
        "CACHE_S3_BUCKET": "your-unique-bucket-name",
        # Optional key prefix
        "CACHE_KEY_PREFIX": "cache_",
    }

    app = Flask(__name__)
    app.config.from_mapping(config)
    cache = Cache(app)

Of course, an application-factory is also supported:

.. code-block:: python


    from flask import Flask
    from flask_caching import Cache

    configuration = {
        # The import path of the S3 cache backend
        "CACHE_TYPE": "flask_caching_s3.S3Cache",
        # The bucket name where you'd like cache artifacts to be stored/queried
        "CACHE_S3_BUCKET": "your-unique-bucket-name",
        # Optional key prefix
        "CACHE_KEY_PREFIX": "cache_",
    }

    # Initialize the extension without the application object present,
    # as is typical in the Flask application factory pattern
    cache = Cache()

    def create_app():
        """Create and configure an application instance."""

        app = Flask("factory-app")
        app.config.from_mapping(configuration)

        cache.init_app(app)

        return app

And in situations where you may want to have _multiple_ caching strategies with different backends, a per-object configuration is supported:

.. code-block:: python

    from flask import Flask
    from flask_caching import Cache

    s3_cache = Cache()
    redis_cache = Cache()

    def create_app():
        """Create and configure an application instance."""

        app = Flask("factory-app")

        # Setup our s3 cache
        s3_cache.init_app(
            app,
            config={
                "CACHE_TYPE": "flask_caching_s3.S3Cache",
                "CACHE_S3_BUCKET": "the-tholian-initiative"
            }
        )

        # And now, separately, we can setup our redis cache,
        # with redis-specific configuration options.
        redis_cache.init_app(
            app,
            config={
                "CACHE_TYPE": "RedisCache",
                "CACHE_REDIS_HOST": "example.com"
            }
        )

        return app

Configuration Options
---------------------

Required
~~~~~~~~

- ``CACHE_S3_BUCKET``: There's only one required configuration, and that's the S3 bucket name. Your bucket must already exist in S3, and you must set the correct ACLs/permissions for your application to read and write from it. This backend will do none of that work for you.

Optional
~~~~~~~~

The following options can be provided to the S3Cache, but are entirely optional:

- ``CACHE_KEY_PREFIX``: A string that will be prepended to /every/ cache key, for both reads and writes. Useful if you want to use the same bucket for non-cache related things and avoid disaster when you call ``cache.clear()`` and wonder where all your S3 bucket contents have gone.
- ``CACHE_DEFAULT_TIMEOUT``: The number of seconds that an item in the cache is valid for. After this time has elapsed, the item is considered expired, and even if the item is still in the S3 bucket, a cache miss will occur.
- ``CACHE_S3_ENDPOINT_URL``: The endpoint for the S3 service. Typically this is only utilized when using something like `localstack <https://localstack.cloud/>`_ for local development/testing.
- ``CACHE_OPTIONS``: A dictionary of key/value pairs for more fine-grained configuration of how the cache will behave.

    .. code-block:: python
        s3_cache.init_app(
            app,
            config={
                "CACHE_TYPE": "flask_caching_s3.S3Cache",
                "CACHE_S3_BUCKET": "the-tholian-initiative",
                "CACHE_OPTIONS": {"purge_expired_on_read": True}
            }
        )

  The only key currently supported in ``CACHE_OPTIONS`` is the boolean ``purge_expired_on_read``, which defaults to ``False``. If set to ``True``, items will be evicted from S3 if ``Flask-Cache```` attempts to read them (via ``cache.get()`` or ``cache.has()``, for example) and they have expired.

S3 Object Lifecycle Management
-------------------------------

The use of ``purge_expired_on_read`` *does* incur a performance penalty since the eviction/deletion is performed in the same operation, and it also means that if some items are never accessed, they will continue to exist in the bucket far beyond their expiration.

The proper solution to this is to create an `S3 object lifecycle rule
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html>`_
which can clean up objects for you. Care must be taken here, though, since you cannot rely on the ``CACHE_DEFAULT_TIMEOUT`` value as the boundary value for object lifetimes; users of ``Flask-Cache`` can always override the default timeout at call-time with e.g.:

.. code-block:: python

    @app.route("/")
    @cache.cached(timeout=50)
    def index():
        return render_template('index.html')

Where ``cache.cached(timeout=3600)`` indicates that the cached object is valid for 3600 seconds, even though our default may be set to 300.

Thus, if you do choose to go with an Object Lifecycle Management rule, pick an Expiration policy that is beyond whatever maximum timeout value that you would conceivably apply.


Testing
~~~~~~~

0. Have Docker installed and running
1. Clone this repository
2. Ensure you have `poetry` available on your system
3. `poetry run pytest`

The test suite will spin up an ephemeral Docker container; it may take a few seconds for it to load. The relevant test fixtures will handle creating objects and their values in the Localstack S3 service.
