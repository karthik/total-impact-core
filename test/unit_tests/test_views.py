import unittest, json, uuid
from copy import deepcopy
from urllib import quote_plus
from nose.tools import assert_equals

from totalimpact import app, dao, views
from totalimpact.providers.dryad import Dryad
import os


TEST_DRYAD_DOI = "10.5061/dryad.7898"
PLOS_TEST_DOI = "10.1371/journal.pone.0004803"
GOLD_MEMBER_ITEM_CONTENT = ["MEMBERITEM CONTENT"]
TEST_COLLECTION_ID = "TestCollectionId"
TEST_COLLECTION_TIID_LIST = ["tiid1", "tiid2"]
TEST_COLLECTION_TIID_LIST_MODIFIED = ["tiid1", "tiid_different"]

COLLECTION_SEED = json.loads("""{
    "id": "uuid-goes-here",
    "collection_name": "My Collection",
    "owner": "abcdef",
    "created": 1328569452.406,
    "last_modified": 1328569492.406,
    "item_tiids": ["origtiid1", "origtiid2"] 
}""")
COLLECTION_SEED_MODIFIED = deepcopy(COLLECTION_SEED)
COLLECTION_SEED_MODIFIED["item_tiids"] = TEST_COLLECTION_TIID_LIST_MODIFIED


api_items_loc = os.path.join(
    os.path.split(__file__)[0],
    '../data/items.json')
API_ITEMS_JSON = json.loads(open(api_items_loc, "r").read())

def MOCK_member_items(self, query_string, url=None, cache_enabled=True):
    return(GOLD_MEMBER_ITEM_CONTENT)

# ensures that all the functions in the views.py module will use a local db,
# which we can in turn use for these unit tests.
mydao = views.set_db("http://localhost:5984", os.getenv("CLOUDANT_DB"))


class ViewsTester(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        # hacky way to delete the "ti" db, then make it fresh again for each test.
        temp_dao = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))
        temp_dao.delete_db(os.getenv("CLOUDANT_DB"))
        self.d = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))

        #setup api test client
        self.app = app
        self.app.testing = True
        self.client = self.app.test_client()

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

class DaoTester(unittest.TestCase):
    def test_dao(self):
        assert_equals(mydao.db.name, os.getenv("CLOUDANT_DB"))


class TestMemberItems(ViewsTester):

    def setUp(self): 
        super(TestMemberItems, self).setUp()
        # Mock out relevant methods of the Dryad provider
        self.orig_Dryad_member_items = Dryad.member_items
        Dryad.member_items = MOCK_member_items

    def tearDown(self):
        Dryad.member_items = self.orig_Dryad_member_items

    def test_memberitems_get(self):
        response = self.client.get('/provider/dryad/memberitems/Otto%2C%20Sarah%20P.?method=sync')
        print response
        print response.data
        assert_equals(response.status_code, 200)
        assert_equals(json.loads(response.data)["memberitems"], GOLD_MEMBER_ITEM_CONTENT)
        assert_equals(response.mimetype, "application/json")


class TestProvider(ViewsTester):

        def test_exists(self):
            resp = self.client.get("/provider")
            assert resp

        def test_gets_delicious_static_meta(self):
            resp = self.client.get("/provider")
            md = json.loads(resp.data)
            print md["delicious"]
            assert md["delicious"]['metrics']["bookmarks"]["description"]



class TestItem(ViewsTester):

    def test_item_post_unknown_tiid(self):
        response = self.client.post('/item/doi/AnIdOfSomeKind/')
        print response
        print response.data
        assert_equals(response.status_code, 201)  #Created
        assert_equals(len(json.loads(response.data)), 24)
        assert_equals(response.mimetype, "application/json")

    def test_item_post_success(self):
        resp = self.client.post('/item/doi/' + quote_plus(TEST_DRYAD_DOI))
        tiid = json.loads(resp.data)

        response = self.client.get('/item/' + tiid)
        assert_equals(response.status_code, 210) # 210 created, but not done updating...
        assert_equals(response.mimetype, "application/json")
        saved_item = json.loads(response.data)

        assert_equals([unicode(TEST_DRYAD_DOI)], saved_item["aliases"]["doi"])

    def test_item_get_success_realid(self):
        # First put something in
        response = self.client.get('/item/doi/' + quote_plus(TEST_DRYAD_DOI))
        tiid = response.data
        print response
        print tiid

    def test_item_post_unknown_namespace(self):
        response = self.client.post('/item/AnUnknownNamespace/AnIdOfSomeKind/')
        # cheerfully creates items whether we know their namespaces or not.
        assert_equals(response.status_code, 201)


class TestItems(ViewsTester):
    def test_post_with_aliases_already_in_db(self):
        items = [
            ["doi", "10.123"],
            ["doi", "10.124"],
            ["doi", "10.125"]
        ]
        resp = self.client.post(
            '/items',
            data=json.dumps(items),
            content_type="application/json"
        )
        tiids = json.loads(resp.data)

        new_items = [
            ["doi", "10.123"], # duplicate
            ["doi", "10.124"], # duplicate
            ["doi", "10.999"]  # new
        ]

        resp2 = self.client.post(
            '/items',
            data=json.dumps(new_items),
            content_type="application/json"
        )
        new_tiids = json.loads(resp2.data)

        # 3 unique tiids in first set + 1 unique in second set
        assert_equals(len(set(tiids + new_tiids)), 4)

        # 3 new items + 1 new item + 1 design doc
        assert_equals(self.d.db.info()["doc_count"], 5)


class TestCollection(ViewsTester):

    def test_collection_post_new_collection(self):
        response = self.client.post(
            '/collection',
            data=json.dumps({"items": TEST_COLLECTION_TIID_LIST, "title":"My Title"}),
            content_type="application/json")

        print response
        print response.data
        assert_equals(response.status_code, 201)  #Created
        assert_equals(response.mimetype, "application/json")
        response_loaded = json.loads(response.data)
        assert_equals(
                set(response_loaded.keys()),
                set([u'created', u'item_tiids', u'last_modified', u'ip_address', u'title', u'type', u'_id', u'_rev']))
        assert_equals(len(response_loaded["_id"]), 6)
        assert_equals(response_loaded["item_tiids"], [u'tiid1', u'tiid2'])

    def test_collection_get_with_no_id(self):
        response = self.client.get('/collection/notgoingtofindthis')
        assert_equals(response.status_code, 404)  #Not found




    def test_collection_update_puts_items_on_update_queue(self):
        # put some stuff in the collection:
        # put some items in the db
        for doc in mydao.db.update([
                {"_id":"larry"},
                {"_id":"curly"},
                {"_id":"moe"}
        ]):
            pass # no need to do anything, just put 'em in couch.

        collection = {
            "_id":"123",
            "item_tiids":["larry", "curly", "moe"]
            }
        mydao.save(collection)
        resp = self.client.post(
            "/collection/123"
        )
        assert_equals(resp.data, "true")

        larry = mydao.get("larry")
        # super hacky way to test for iso date string
        assert_equals(larry["needs_aliases"][0:4], "2012")


class TestApi(ViewsTester):

    def setUp(self):
        super(TestApi, self).setUp()

    def tearDown(self):
        pass

    def test_clean_id(self):
        nid = u"10.1000/\u200bna\tture "
        response = views.clean_id(nid)
        assert_equals(response, u'10.1000/nature')

    def test_tiid_get_with_unknown_alias(self):
        # try to retrieve tiid id for something that doesn't exist yet
        plos_no_tiid_resp = self.client.get('/tiid/doi/' +
                quote_plus(PLOS_TEST_DOI))
        assert_equals(plos_no_tiid_resp.status_code, 404)  # Not Found


    def test_tiid_get_with_known_alias(self):
        # create new plos item from a doi
        plos_create_tiid_resp = self.client.post('/item/doi/' +
                quote_plus(PLOS_TEST_DOI))
        plos_create_tiid = json.loads(plos_create_tiid_resp.data)

        # retrieve the plos tiid using tiid api
        plos_lookup_tiid_resp = self.client.get('/tiid/doi/' +
                quote_plus(PLOS_TEST_DOI))
        assert_equals(plos_lookup_tiid_resp.status_code, 303)
        plos_lookup_tiids = json.loads(plos_lookup_tiid_resp.data)

        # check that the tiids are the same
        assert_equals(plos_create_tiid, plos_lookup_tiids)

    def test_tiid_get_tiids_for_multiple_known_aliases(self):
        # create two new items with the same plos alias
        first_plos_create_tiid_resp = self.client.post('/item/doi/' +
                quote_plus(PLOS_TEST_DOI))
        first_plos_create_tiid = json.loads(first_plos_create_tiid_resp.data)

        second_plos_create_tiid_resp = self.client.post('/item/doi/' +
                quote_plus(PLOS_TEST_DOI))
        second_plos_create_tiid = json.loads(second_plos_create_tiid_resp.data)

        # check that the tiid lists are the same
        assert_equals(first_plos_create_tiid, second_plos_create_tiid)



class TestTiid(ViewsTester):

    def test_tiid_post(self):
        # POST isn't supported
        response = self.client.post('/tiid/Dryad/NotARealId')
        assert_equals(response.status_code, 405)  # Method Not Allowed

    def test_tiid_post(self):
        # POST isn't supported
        response = self.client.post('/tiid/Dryad/NotARealId')
        assert_equals(response.status_code, 405)  # Method Not Allowed

    def test_item_get_unknown_tiid(self):
        # pick a random ID, very unlikely to already be something with this ID
        response = self.client.get('/item/' + str(uuid.uuid1()))
        assert_equals(response.status_code, 404)  # Not Found

    def test_item_post_known_tiid(self):
        response = self.client.post('/item/doi/IdThatAlreadyExists/')
        print response
        print "here is the response data: " + response.data

        # FIXME should check and if already exists return 200
        # right now this makes a new item every time, creating many dups
        assert_equals(response.status_code, 201)
        assert_equals(len(json.loads(response.data)), 24)
        assert_equals(response.mimetype, "application/json")

    def test_item_get_unknown_tiid(self):
        # pick a random ID, very unlikely to already be something with this ID
        response = self.client.get('/item/' + str(uuid.uuid1()))
        assert_equals(response.status_code, 404)  # Not Found
