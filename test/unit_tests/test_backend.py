import json, os, Queue, datetime

from totalimpact import dao, tiredis, backend, default_settings
from totalimpact.providers.provider import Provider, ProviderTimeout, ProviderFactory
from nose.tools import raises, assert_equals, nottest
from test.utils import slow
from test import mocks


class TestBackend():
    
    def setUp(self):
        self.config = None #placeholder
        self.TEST_PROVIDER_CONFIG = [
            ("wikipedia", {})
        ]
        # hacky way to delete the "ti" db, then make it fresh again for each test.
        temp_dao = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))
        temp_dao.delete_db(os.getenv("CLOUDANT_DB"))
        self.d = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))

        # do the same thing for the redis db, set up the test redis database.  We're using DB Number 8
        self.r = tiredis.from_url("redis://localhost:6379", db=8)
        self.r.flushdb()

        provider_queues = {}
        providers = ProviderFactory.get_providers(self.TEST_PROVIDER_CONFIG)
        for provider in providers:
            provider_queues[provider.provider_name] = backend.RedisQueue(provider.provider_name+"_queue", self.r)

        self.b = backend.Backend(
            backend.RedisQueue("alias-unittest", self.r), 
            provider_queues, 
            [backend.RedisQueue("couch_queue", self.r)], 
            self.r)

        self.fake_item = {
            "_id": "1",
            "type": "item",
            "num_providers_still_updating":1,
            "aliases":{"pmid":["111"]},
            "biblio": {},
            "metrics": {}
        }
        self.fake_aliases_dict = {"pmid":["222"]}
        self.tiid = "abcd"

    def teardown(self):
        self.d.delete_db(os.environ["CLOUDANT_DB"])
        self.r.flushdb()


class TestProviderWorker(TestBackend):
    # warning: calls live provider right now
    def test_add_to_couch_queue_if_nonzero(self):    
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        provider_worker = backend.ProviderWorker(mocks.ProviderMock("myfakeprovider"), 
                                        None, None, None, {"a": test_couch_queue}, None, self.r)  
        response = provider_worker.add_to_couch_queue_if_nonzero("aaatiid", #start fake tiid with "a" so in first couch queue
                {"doi":["10.5061/dryad.3td2f"]}, 
                "aliases", 
                "dummy")

        # test that it put it on the queue
        in_queue = test_couch_queue.pop()
        expected = ['aaatiid', {'doi': ['10.5061/dryad.3td2f']}, 'aliases']
        assert_equals(in_queue, expected)

    def test_add_to_couch_queue_if_nonzero_given_metrics(self):    
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        provider_worker = backend.ProviderWorker(mocks.ProviderMock("myfakeprovider"), 
                                        None, None, None, {"a": test_couch_queue}, None, self.r)  
        metrics_method_response = {'dryad:package_views': [361, 'http://dx.doi.org/10.5061/dryad.7898'], 
                    'dryad:total_downloads': [176, 'http://dx.doi.org/10.5061/dryad.7898'], 
                    'dryad:most_downloaded_file': [65, 'http://dx.doi.org/10.5061/dryad.7898']}        
        response = provider_worker.add_to_couch_queue_if_nonzero("aaatiid", #start fake tiid with "a" so in first couch queue
                metrics_method_response,
                "metrics", 
                "dummy")

        # test that it put it on the queue
        in_queue = test_couch_queue.pop()
        expected = ['aaatiid', metrics_method_response, "metrics"]
        print in_queue
        assert_equals(in_queue, expected)        

        # check nothing in redis since it had a value
        response = num_left = self.r.get_num_providers_left("aaatiid")
        assert_equals(response, None)

    def test_add_to_couch_queue_if_nonzero_given_empty_metrics_response(self):    
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        provider_worker = backend.ProviderWorker(mocks.ProviderMock("myfakeprovider"), 
                                        None, None, None, {"a": test_couch_queue}, None, self.r)  
        metrics_method_response = {}
        response = provider_worker.add_to_couch_queue_if_nonzero("aaatiid", #start fake tiid with "a" so in first couch queue
                metrics_method_response,
                "metrics", 
                "dummy")

        # test that it did not put it on the queue
        in_queue = test_couch_queue.pop()
        expected = None
        assert_equals(in_queue, expected)        

        # check decremented in redis since the payload was null
        response = num_left = self.r.get_num_providers_left("aaatiid")
        assert_equals(response, -1)

    def test_wrapper(self):     
        def fake_callback(tiid, new_content, method_name, aliases_providers_run):
            pass

        response = backend.ProviderWorker.wrapper("123", 
                {'url': ['http://somewhere'], 'doi': ['10.123']}, 
                mocks.ProviderMock("myfakeprovider"), 
                "aliases", 
                [], # aliases previously run
                fake_callback)
        print response
        expected = {'url': ['http://somewhere'], 'doi': ['10.1', '10.123']}
        assert_equals(response, expected)

class TestCouchWorker(TestBackend):
    def test_update_item_with_new_aliases(self):
        response = backend.CouchWorker.update_item_with_new_aliases(self.fake_aliases_dict, self.fake_item)
        expected = {'metrics': {}, 'num_providers_still_updating': 1, 'biblio': {}, '_id': '1', 'type': 'item', 
            'aliases': {'pmid': ['222', '111']}}
        assert_equals(response, expected)

    def test_update_item_with_new_aliases_using_dup_alias(self):
        dup_alias_dict = self.fake_item["aliases"]
        response = backend.CouchWorker.update_item_with_new_aliases(dup_alias_dict, self.fake_item)
        expected = None # don't return the item if it already has all the aliases in it
        assert_equals(response, expected)

    def test_update_item_with_new_biblio(self):
        new_biblio_dict = {"title":"A very good paper", "authors":"Smith, Lee, Khun"}
        response = backend.CouchWorker.update_item_with_new_biblio(new_biblio_dict, self.fake_item)
        expected = new_biblio_dict
        assert_equals(response["biblio"], expected)

    def test_update_item_with_new_biblio_existing_biblio(self):
        item_with_some_biblio = self.fake_item
        item_with_some_biblio["biblio"] = {"title":"Different title"}
        new_biblio_dict = {"title":"A very good paper", "authors":"Smith, Lee, Khun"}
        response = backend.CouchWorker.update_item_with_new_biblio(new_biblio_dict, item_with_some_biblio)
        expected = None # return None if item already has aliases in it
        assert_equals(response, expected)

    def test_update_item_with_new_metrics(self):
        response = backend.CouchWorker.update_item_with_new_metrics("mendeley:groups", (3, "http://provenance"), self.fake_item)
        expected = {'mendeley:groups': {'provenance_url': 'http://provenance', 'values': {'raw': 3, 'raw_history': {'2012-09-15T21:39:39.563710': 3}}}}
        print response["metrics"]        
        assert_equals(response["metrics"]['mendeley:groups']["provenance_url"], 'http://provenance')
        assert_equals(response["metrics"]['mendeley:groups']["values"]["raw"], 3)
        assert_equals(response["metrics"]['mendeley:groups']["values"]["raw_history"].values(), [3])
        # check year starts with 20
        assert_equals(response["metrics"]['mendeley:groups']["values"]["raw_history"].keys()[0][0:2], "20")

    def test_run_nothing_in_queue(self):
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        couch_worker = backend.CouchWorker(test_couch_queue, self.r, self.d)
        response = couch_worker.run()
        expected = None
        assert_equals(response, expected)

    def test_run_aliases_in_queue(self):
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        test_couch_queue_dict = {self.fake_item["_id"][0]:test_couch_queue}
        provider_worker = backend.ProviderWorker(mocks.ProviderMock("myfakeprovider"), 
                                        None, None, None, test_couch_queue_dict, None, self.r)  
        response = provider_worker.add_to_couch_queue_if_nonzero(self.fake_item["_id"], 
                {"doi":["10.5061/dryad.3td2f"]}, 
                "aliases", 
                "dummy")

        # save basic item beforehand
        self.d.save(self.fake_item)

        # run
        couch_worker = backend.CouchWorker(test_couch_queue, self.r, self.d)
        response = couch_worker.run()
        expected = None
        assert_equals(response, expected)

        # check couch_queue has value after
        couch_response = self.d.get(self.fake_item["_id"])
        print couch_response
        expected = {'pmid': ['111'], 'doi': ['10.5061/dryad.3td2f']}
        assert_equals(couch_response["aliases"], expected)

        # check has updated last_modified time
        now = datetime.datetime.now().isoformat()
        assert_equals(couch_response["last_modified"][0:10], now[0:10])

    def test_run_metrics_in_queue(self):
        test_couch_queue = backend.RedisQueue("test_couch_queue", self.r)
        test_couch_queue_dict = {self.fake_item["_id"][0]:test_couch_queue}
        provider_worker = backend.ProviderWorker(mocks.ProviderMock("myfakeprovider"), 
                                        None, None, None, test_couch_queue_dict, None, self.r) 
        metrics_method_response = {'dryad:package_views': (361, 'http://dx.doi.org/10.5061/dryad.7898'), 
                            'dryad:total_downloads': (176, 'http://dx.doi.org/10.5061/dryad.7898'), 
                            'dryad:most_downloaded_file': (65, 'http://dx.doi.org/10.5061/dryad.7898')}                                         
        response = provider_worker.add_to_couch_queue_if_nonzero(self.fake_item["_id"], 
                metrics_method_response,
                "metrics", 
                "dummy")

        # save basic item beforehand
        self.d.save(self.fake_item)

        # run
        couch_worker = backend.CouchWorker(test_couch_queue, self.r, self.d)    
        couch_worker.run()
            
        # check couch_queue has value after
        couch_response = self.d.get(self.fake_item["_id"])
        print couch_response
        expected = 361
        assert_equals(couch_response["metrics"]['dryad:package_views']['values']["raw"], expected)

        # check has updated last_modified time
        now = datetime.datetime.now().isoformat()
        assert_equals(couch_response["last_modified"][0:10], now[0:10])


class TestBackendClass(TestBackend):

    def test_decide_who_to_call_next_unknown(self):
        aliases_dict = {"unknownnamespace":["111"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect blanks
        expected = {'metrics': [], 'biblio': [], 'aliases': ['webpage']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_unknown_after_webpage(self):
        aliases_dict = {"unknownnamespace":["111"]}
        prev_aliases = ["webpage"]
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect blanks
        expected = {'metrics': ["wikipedia"], 'biblio': ["webpage"], 'aliases': []}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_webpage_no_title(self):
        aliases_dict = {"url":["http://a"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect all metrics and lookup the biblio
        expected = {'metrics': ['wikipedia'], 'biblio': ['webpage'], 'aliases': []}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_webpage_with_title(self):
        aliases_dict = {"url":["http://a"], "title":["A Great Paper"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect all metrics, no need to look up biblio
        expected = {'metrics': ['wikipedia'], 'biblio': ['webpage'], 'aliases': []}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_slideshare_no_title(self):
        aliases_dict = {"url":["http://abc.slideshare.net/def"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect all metrics and look up the biblio
        expected = {'metrics': ['wikipedia'], 'biblio': ['slideshare'], 'aliases': []}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_dryad_no_url(self):
        aliases_dict = {"doi":["10.5061/dryad.3td2f"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to resolve the dryad doi before can go get metrics
        expected = {'metrics': [], 'biblio': [], 'aliases': ['dryad']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_dryad_with_url(self):
        aliases_dict = {   "doi":["10.5061/dryad.3td2f"],
                                    "url":["http://dryadsomewhere"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # still need the dx.doi.org url
        expected = {'metrics': [], 'biblio': [], 'aliases': ['dryad']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_dryad_with_doi_url(self):
        aliases_dict = {   "doi":["10.5061/dryad.3td2f"],
                                    "url":["http://dx.doi.org/10.dryadsomewhere"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # have url so now can go get all the metrics
        expected = {'aliases': [], 'biblio': ['dryad'], 'metrics': ['wikipedia']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_pmid_not_run(self):
        aliases_dict = {"pmid":["111"]}
        prev_aliases = []
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get more aliases
        expected = {'metrics': [], 'biblio': [], 'aliases': ['pubmed']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_pmid_prev_run(self):
        aliases_dict = {  "pmid":["1111"],
                         "url":["http://pubmedsomewhere"]}
        prev_aliases = ["pubmed"]
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get metrics and biblio
        expected = {'metrics': [], 'biblio': [], 'aliases': ['crossref']}
        assert_equals(response, expected)

    def test_decide_who_to_call_next_doi_with_urls(self):
        aliases_dict = {  "doi":["10.234/345345"],
                                "url":["http://journalsomewhere"]}
        prev_aliases = ["pubmed"]
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get metrics, biblio from crossref
        expected = {'metrics': [], 'biblio': [], 'aliases': ['crossref']}
        assert_equals(response, expected)     

    def test_decide_who_to_call_next_doi_crossref_prev_called(self):
        aliases_dict = { "doi":["10.234/345345"],
                        "url":["http://journalsomewhere"]}
        prev_aliases = ["crossref"]                        
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get metrics, no biblio
        expected = {'metrics': [], 'biblio': [], 'aliases': ['pubmed']}
        assert_equals(response, expected)   

    def test_decide_who_to_call_next_doi_crossref_pubmed_prev_called(self):
        aliases_dict = { "doi":["10.234/345345"],
                        "url":["http://journalsomewhere"]}
        prev_aliases = ["crossref", "pubmed"]                        
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get metrics, no biblio
        expected = {'metrics': ["wikipedia"], 'biblio': ['pubmed', 'crossref'], 'aliases': []}
        assert_equals(response, expected)   

    def test_decide_who_to_call_next_pmid_crossref_pubmed_prev_called(self):
        aliases_dict = { "pmid":["1111"],
                        "url":["http://journalsomewhere"]}
        prev_aliases = ["crossref", "pubmed"]                        
        response = backend.Backend.sniffer(aliases_dict, prev_aliases, self.TEST_PROVIDER_CONFIG)
        print response
        # expect need to get metrics, no biblio
        expected = {'metrics': ["wikipedia"], 'biblio': ['pubmed', 'crossref'], 'aliases': []}
        assert_equals(response, expected)   


