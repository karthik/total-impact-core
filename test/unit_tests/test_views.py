import unittest, json, uuid
from copy import deepcopy
from urllib import quote_plus
from nose.tools import nottest, assert_equals
from BeautifulSoup import BeautifulSoup

from totalimpact import app, dao
from totalimpact.providers.dryad import Dryad
import os, yaml


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

sample_item_loc = os.path.join(
    os.path.split(__file__)[0],
    '../data/couch_docs/article.yml')
f = open(sample_item_loc, "r")
ARTICLE_ITEM = yaml.load(f.read())


def MOCK_member_items(self, query_string, url=None, cache_enabled=True):
    return(GOLD_MEMBER_ITEM_CONTENT) 


class ViewsTester(unittest.TestCase):

    def setUp(self):
        #setup api test client
        self.app = app
        self.app.testing = True
        self.client = self.app.test_client()
        # setup the database
        self.testing_db_name = "api_test"
        self.app.config["DB_NAME"] = self.testing_db_name
        self.d = dao.Dao(os.environ["CLOUDANT_URL"], os.environ["CLOUDANT_DB"])

    def tearDown(self):
        self.d.delete_db( os.environ["CLOUDANT_DB"]) 

class TestMemberItems(ViewsTester):

    def setUp(self): 
        super(TestMemberItems, self).setUp()
        # Mock out relevant methods of the Dryad provider
        self.orig_Dryad_member_items = Dryad.member_items
        Dryad.member_items = MOCK_member_items

    def tearDown(self):
        Dryad.member_items = self.orig_Dryad_member_items

    def test_memberitems_get(self):
        response = self.client.get('/provider/dryad/memberitems?query=Otto%2C%20Sarah%20P.&type=author')
        print response
        print response.data
        assert_equals(response.status_code, 200)
        assert_equals(json.loads(response.data), GOLD_MEMBER_ITEM_CONTENT)
        assert_equals(response.mimetype, "application/json")

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
        print response.data

        # FIXME should check and if already exists return 200
        # right now this makes a new item every time, creating many dups
        assert_equals(response.status_code, 201)
        assert_equals(len(json.loads(response.data)), 32)
        assert_equals(response.mimetype, "application/json")

    def test_item_get_unknown_tiid(self):
        # pick a random ID, very unlikely to already be something with this ID
        response = self.client.get('/item/' + str(uuid.uuid1()))
        assert_equals(response.status_code, 404)  # Not Found

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
    '''

    def test_item_post_unknown_tiid(self):
        response = self.client.post('/item/doi/AnIdOfSomeKind/')
        print response
        print response.data
        assert_equals(response.status_code, 201)  #Created
        assert_equals(len(json.loads(response.data)), 32)
        assert_equals(response.mimetype, "application/json")

    def test_item_post_success(self):
        resp = self.client.post('/item/doi/' + quote_plus(TEST_DRYAD_DOI))
        tiid = json.loads(resp.data)

        response = self.client.get('/item/' + tiid)
        assert_equals(response.status_code, 200)
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

    def test_post_with_multiple_items(self):
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
        dois = []
        for tiid in json.loads(resp.data):
            doc = self.d.get(tiid)
            dois.append(doc['aliases']['doi'][0])

        expected_dois = [i[1] for i in items]
        assert_equals(set(expected_dois), set(dois))

    def test_get_csv(self):

        # put some items in the db
        items = [
            ["url", "http://google.com"],
            ["url", "http://nescent.org"],
            ["url", "http://total-impact.org"]
        ]
        resp = self.client.post(
            '/items',
            data=json.dumps(items),
            content_type="application/json"
        )
        tiids = json.loads(resp.data)
        tiids_str = ','.join(tiids)
        resp = self.client.get('/items/'+tiids_str+'.csv')
        rows = resp.data.split("\n")
        print rows
        assert_equals(len(rows), 4) # header plus 3 items





class TestCollection(ViewsTester):

    def test_collection_post_already_exists(self):
        response = self.client.post('/collection/' + TEST_COLLECTION_ID)
        assert_equals(response.status_code, 405)  # Method Not Allowed

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
                set([u'created', u'item_tiids', u'last_modified', u'title', u'type', u'id']))
        assert_equals(len(response_loaded["id"]), 6)

    def test_collection_put_updated_collection(self):

        # Put in an item.  Could mock this out in the future.
        response = self.client.post('/collection',
                data=json.dumps({"items": TEST_COLLECTION_TIID_LIST, "title":"My Title"}),
                content_type="application/json")
        response_loaded = json.loads(response.data)
        new_collection_id = response_loaded["id"]

        # put the new collection stuff
        response = self.client.put('/collection/' + new_collection_id,
                data=json.dumps(COLLECTION_SEED_MODIFIED),
                content_type="application/json")
        print response
        print response.data
        assert_equals(response.status_code, 200)  #updated
        assert_equals(response.mimetype, "application/json")
        response_loaded = json.loads(response.data)
        assert_equals(
                set(response_loaded.keys()),
                set([u'created', u'collection_name', u'item_tiids', u'last_modified',
                    u'owner', u'id'])
                )
        assert_equals(response_loaded["item_tiids"],
            COLLECTION_SEED_MODIFIED["item_tiids"])

    def test_collection_put_empty_payload(self):
        response = self.client.put('/collection/' + TEST_COLLECTION_ID)
        assert_equals(response.status_code, 404)  #Not found

    def test_collection_delete_with_no_id(self):
        response = self.client.delete('/collection/')
        assert_equals(response.status_code, 404)  #Not found




    def test_collection_get_with_no_id(self):
        response = self.client.get('/collection/')
        assert_equals(response.status_code, 404)  #Not found

class TestApi(ViewsTester):

    def setUp(self):

        super(TestApi, self).setUp()

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
        assert_equals(plos_create_tiid, plos_lookup_tiids)'''

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
