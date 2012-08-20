import os, unittest, time, logging
from totalimpact.providers.provider import Provider, ProviderTimeout
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
            "aliases":{"pmid":["111"]},
            "biblio": {},
            "metrics": {}
        }
        
        
    def teardown(self):
        self.d.delete_db(os.environ["CLOUDANT_DB"])



class TestMetricWorker(TestBackend):

    def test_converts_classname_to_provider_method_name(self):
        mw = backend.MetricsWorker(self.providers[0])
        assert_equals(mw.provider_name, "ProviderMock")
        assert_equals(mw.method_name, "metrics")
        assert_equals(mw.method, self.providers[0].metrics)

    def test_add_metrics_to_item(self):
        mw = backend.MetricsWorker(self.providers[0])
        mw.dao = self.d

        #just using the test provider here to create the test metrics dict...
        metrics = self.providers[0].metrics([("doi", "10.1")])

        new_item = mw.add_metrics_to_item(metrics, self.item_with_aliases)
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

        # metrics will always run where an item already exists
        self.d.save(self.item_with_aliases)
        item = self.d.get(self.item_with_aliases["_id"])

        mw = backend.MetricsWorker(self.providers[0])
        mw.dao = self.d
        mw.update(item)

        updated_item = self.d.get(item["_id"])

        # adds values for each metric
        assert_equals(
            updated_item["metrics"]["mock:html"]["values"].values()[0],
            2
        )
        assert_equals(
            updated_item["metrics"]["mock:pdf"]["values"].values()[0],
            1
        )

        # decrements the num_providers_still_updating
        assert_equals(
            updated_item["num_providers_still_updating"],
            0
        )



class TestAliasesWorker(TestBackend):

    def test_update(self):
        aw = backend.AliasesWorker(self.providers[0])
        new_item = aw.update(self.item_with_aliases)
        print new_item
        assert_equals(
            new_item["aliases"]["doi"][0],
            "10.1"
        )

    def test_update_with_another_doi(self):
        # put a doi in the item
        aw = backend.AliasesWorker(self.providers[0])
        item_with_doi = aw.update(self.item_with_aliases)

        # another provider also gets dois...
        self.providers[1].aliases_returns = [("doi", "10.2")]
        aw = backend.AliasesWorker(self.providers[1])
        item_with_two_dois = aw.update(item_with_doi)

        print item_with_two_dois
        assert_equals(
            item_with_two_dois["aliases"]["doi"],
            ["10.1", "10.2"]
        )

    def test_update_with_provider_timeout(self):
        self.providers[0].exception_to_raise = ProviderTimeout
        aw = backend.AliasesWorker(self.providers[0])
        new_item = aw.update(self.item_with_aliases)
        print new_item

        assert_equals(len(new_item["aliases"]), 1) # new alias not added



class TestAliasWorker(TestBackend):

    def test_update(self):
        bw = backend.BiblioWorker(self.providers[0])
        new_item = bw.update(self.item_with_aliases)
        print new_item
        assert_equals(
            new_item["biblio"]["title"],
            "fake item"
        )


