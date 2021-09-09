# -*- coding: utf-8 -*-

from elasticsearch.client import _normalize_hosts
from six import PY3

if PY3:
    from unittest.mock import patch

    from contextlib import ExitStack, contextmanager

    @contextmanager
    def nested(*contexts):
        with ExitStack() as stack:
            for ctx in contexts:
                stack.enter_context(ctx)
            yield contexts


else:
    from mock import patch
    from contextlib import nested


from elasticmock.fake_elasticsearch import FakeElasticsearch

ELASTIC_INSTANCES = {}
# if you needed to patch elasticsearch6, elasticsearch7 as well
DEFAULT_ELASTIC_CLASSES_TO_PATCH = {
    "elasticsearch.Elasticsearch",
}


def _get_elasticmock(hosts=None, *args, **kwargs):
    host = _normalize_hosts(hosts)[0]
    elastic_key = "{0}:{1}".format(
        host.get("host", "localhost"), host.get("port", 9200)
    )

    if elastic_key in ELASTIC_INSTANCES:
        connection = ELASTIC_INSTANCES.get(elastic_key)
    else:
        connection = FakeElasticsearch()
        ELASTIC_INSTANCES[elastic_key] = connection
    return connection


def elasticmock(f=None, klasses_to_patch=None):
    klasses_to_mock = klasses_to_patch or DEFAULT_ELASTIC_CLASSES_TO_PATCH

    def decorator(func):
        def inner(*args, **kwargs):
            ELASTIC_INSTANCES.clear()
            with nested(*(patch(klass, _get_elasticmock) for klass in klasses_to_mock)):
                result = func(*args, **kwargs)
            return result

        return inner

    if f is not None:
        return decorator(f)
    return decorator
