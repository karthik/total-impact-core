import os, unittest, time, logging
from totalimpact.providers.provider import Provider, ProviderFactory
from totalimpact import dao, tiredis, backend
from nose.tools import raises, assert_equals, nottest


from test.mocks import ProviderMock, QueueMock

def slow(f):
    f.slow = True
    return f

CWD, _ = os.path.split(__file__)

TIME_SCALE = 0.0005 #multiplier to run the tests as fast as possible
BACKEND_POLL_INTERVAL = 0.5 #seconds

class ProviderNotImplemented(Provider):
    def __init__(self):
        Provider.__init__(self, None)
        self.provider_name = 'not_implemented'
    def aliases(self, item, provider_url_template=None, cache_enabled=True):
        raise NotImplementedError()
    def metrics(self, item, provider_url_template=None, cache_enabled=True):
        raise NotImplementedError()


def save_and_unqueue_mock(self, item):
    pass
    
def get_providers_mock(cls, config):
    return [ProviderMock("1"), ProviderMock("2"), ProviderMock("3")]



class TestBackend():
    
    def setUp(self):
        self.config = None #placeholder
        TEST_PROVIDER_CONFIG = [
            ("wikipedia", {})
        ]
        # hacky way to delete the "ti" db, then make it fresh again for each test.
        temp_dao = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))
        temp_dao.delete_db(os.getenv("CLOUDANT_DB"))
        self.d = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))

        # do the same thing for the redis db
        self.r = tiredis.from_url("redis://localhost:6379")
        self.r.flushdb()


        self.providers = [ProviderMock("1"), ProviderMock("2"), ProviderMock("3")]
        self.item_with_aliases = {
            "_id": "1",
            "num_providers_still_updating":1,
            "aliases":{"doi":["10.1"]},
            "biblio": {"title": "fake item"},
            "metrics": {}
        }
        
        
    def teardown(self):
        self.d.delete_db(os.environ["CLOUDANT_DB"])

#    def test_init_metric_worker(self):
#        provider = self.providers[0]
#        provider.metrics
#        au = backend.MetricWorker(self.providers[0])
#        au.update()
#        assert_equals(
#            len(self.item_with_aliases["metrics"]["mock:views"]),
#            1)



class TestMetricWorker(TestBackend):

    def test_add_metrics_to_item(self):
        au = backend.MetricsWorker("fake providerwrapper")
        metrics = self.providers[0].metrics([("doi", "10.1")])
        new_item = au.add_metrics_to_item(metrics, self.item_with_aliases)
        print new_item

        # adds drilldown urls for each metric
        assert_equals(
            new_item["metrics"]["mock:pdf"]["drilldown_url"],
            "http://drilldownurl.org"
        )
        assert_equals(
            new_item["metrics"]["mock:html"]["drilldown_url"],
            "http://drilldownurl.org"
        )

        # adds values for each metric
        assert_equals(
            new_item["metrics"]["mock:html"]["values"].values()[0],
            2
        )
        assert_equals(
            new_item["metrics"]["mock:pdf"]["values"].values()[0],
            1
        )

    def test_update(self):
        sample_metrics = self.providers[0].metrics([("doi", "10.1")])
        backend.ProviderWrapper.process_item_for_provider = lambda self, item: (1, sample_metrics)

        au = backend.MetricsWorker(backend.ProviderWrapper(1,2))
        au.update(self.item_with_aliases)

        # adds values for each metric
        assert_equals(
            self.item_with_aliases["metrics"]["mock:html"]["values"].values()[0],
            2
        )
        assert_equals(
            self.item_with_aliases["metrics"]["mock:pdf"]["values"].values()[0],
            1
        )

        # decrements the num_providers_still_updating
        assert_equals(
            self.item_with_aliases["num_providers_still_updating"],
            0
        )

class TestAliasWorker(TestBackend):
    pass
