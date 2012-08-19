from nose.tools import raises, assert_equals, nottest
import os, unittest, hashlib, json
from time import sleep

from totalimpact import models, dao, tiredis
from totalimpact.providers import bibtex, github
from totalimpact.providers.provider import ProviderTimeout



class TestItemFactory():

    def setUp(self):
        # hacky way to delete the "ti" db, then make it fresh again for each test.
        temp_dao = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))
        temp_dao.delete_db(os.getenv("CLOUDANT_DB"))
        self.d = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))


    def test_get_metric_names(self):
        test_provider_config = [("wikipedia", {"workers":3})]
        response = models.ItemFactory.get_metric_names(test_provider_config)
        assert_equals(response, ['wikipedia:mentions'])


    def test_decide_genre_article_doi(self):
        aliases = {"doi":["10:123", "10:456"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "article")

    def test_decide_genre_article_pmid(self):
        aliases = {"pmid":["12345678"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "article")

    def test_decide_genre_slides(self):
        aliases = {"url":["http://www.slideshare.net/jason/my-slides"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "slides")

    def test_decide_genre_software(self):
        aliases = {"url":["http://www.github.com/jasonpriem/my-sofware"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "software")

    def test_decide_genre_dataset(self):
        aliases = {"doi":["10.5061/dryad.18"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "dataset")

    def test_decide_genre_webpage(self):
        aliases = {"url":["http://www.google.com"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "webpage")

    def test_decide_genre_unknown(self):
        aliases = {"unknown_namespace":["myname"]}
        genre = models.ItemFactory.decide_genre(aliases)
        assert_equals(genre, "unknown")

class TestMemberItems():

    def setUp(self):
        # setup a clean new redis instance
        self.r = tiredis.from_url("redis://localhost:6379")
        self.r.flushdb()

        bibtex.Bibtex.paginate = lambda self, x: [1,2,3,4]
        bibtex.Bibtex.member_items = lambda self, x: ("doi", str(x))
        self.memberitems_resp = [
            ["doi", "1"],
            ["doi", "2"],
            ["doi", "3"],
            ["doi", "4"],
        ]

        self.mi = models.MemberItems(bibtex.Bibtex(), self.r)

    def test_init(self):
        assert_equals(self.mi.__class__.__name__, "MemberItems")
        assert_equals(self.mi.provider.__class__.__name__, "Bibtex")

    def test_start_update(self):
        ret = self.mi.start_update("1234")
        input_hash = hashlib.md5("1234").hexdigest()
        assert_equals(input_hash, ret)

        sleep(.1) # give the thread a chance to finish.
        status = json.loads(self.r.get(input_hash))

        assert_equals(status["memberitems"], self.memberitems_resp )
        assert_equals(status["complete"], 4 )

    def test_get_sync(self):

        github.Github.member_items = lambda self, x: \
                [("github", name) for name in ["project1", "project2", "project3"]]
        synch_mi = models.MemberItems(github.Github(), self.r)

        # we haven't put q in redis with MemberItems.start_update(q),
        # so this should update while we wait.
        ret = synch_mi.get_sync("jasonpriem")
        assert_equals(ret["pages"], 1)
        assert_equals(ret["complete"], 1)
        assert_equals(ret["memberitems"],
            [
                ("github", "project1"),
                ("github", "project2"),
                ("github", "project3")
            ]
        )


    def test_get_async(self):
        ret = self.mi.start_update("1234")
        sleep(.1)
        res = self.mi.get_async(ret)
        print res
        assert_equals(res["complete"], 4)
        assert_equals(res["memberitems"], self.memberitems_resp)




class TestCollectionFactory():

    def setUp(self):
        # hacky way to delete the "ti" db, then make it fresh again for each test.
        temp_dao = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))
        temp_dao.delete_db(os.getenv("CLOUDANT_DB"))
        self.d = dao.Dao("http://localhost:5984", os.getenv("CLOUDANT_DB"))

        # setup a clean new redis instance
        self.r = tiredis.from_url("redis://localhost:6379")
        self.r.flushdb()

        items = [
            {"_id": "1", "type": "item", "aliases": {"title": ["title 1"], "doi":["d1"]}},
            {"_id": "2", "type": "item", "aliases": {"title": ["title 2"], "doi":["d2"]}},
            {"_id": "3", "type": "item", "aliases": {"title": ["title 3"], "doi":["d3"]}},
            {"_id": "4", "type": "item", "aliases": {"title": ["title 4"], "doi":["d4"]}}
        ]
        snaps = [
            {"_id": "s1", "tiid": "1", "type": "metric_snap", "metric_name": "views:html", "created":"now", "value": 100},
            {"_id": "s2", "tiid": "1", "type": "metric_snap", "metric_name": "views:pdf", "created":"now", "value": 10},
            {"_id": "s3", "tiid": "2", "type": "metric_snap", "metric_name": "views:html", "created":"now", "value": 200},
            {"_id": "s4", "tiid": "3", "type": "metric_snap", "metric_name": "views:html", "created":"now", "value": 300}
        ]
        colls = [
            {"_id": "c1", "type": "collection", "item_tiids":["1", "2", "3"]}
        ]

        for doc in items+snaps+colls:
            self.d.save(doc)

    def test_make_creates_identifier(self):
        coll = models.CollectionFactory.make()
        assert_equals(len(coll["_id"]), 6)

    def test_get_includes_items(self):
        coll = models.CollectionFactory.get(self.d, self.r, "c1")
        assert_equals(coll["_id"], "c1")
        assert_equals(len(coll["items"]), 3)

    def test_get_includes_full_items(self):
        coll = models.CollectionFactory.get(self.d, self.r, "c1")
        assert_equals(coll["items"][0]["aliases"]["title"][0], "title 1")

    def test_get_includes_items_with_metrics(self):
        coll = models.CollectionFactory.get(self.d, self.r, "c1")
        assert_equals(coll["items"][0]["metrics"]["views:html"]["values"]["now"], 100)





    #    def test_get_json(self):
#        res_json, still_updating = models.CollectionFactory.get_json(self.d, self.r, "c1")
#        res = json.loads(res_json)
#        print res["items"]
#        assert_equals(len(res["items"]), 3)
#        assert_equals(res["items"][0]["currently_updating"], 0)
#
#    def test_get_csv(self):
#        csv, still_updating = models.CollectionFactory.get_csv(self.d, self.r, "c1")
#        print csv
#        rows = csv.splitlines()
#        assert_equals(len(rows), 4) #header plus three items
#        assert_equals(rows[1].split(",")[1], '"title 1"') # title (in quotes) in 2nd column

    @raises(KeyError)
    def test_get_raises_KeyError_if_cid_not_in_db(self):
        ret, still_updating = models.CollectionFactory.get_json(self.d, self.r, "not_here")
        assert True