import os, requests, json, datetime, time, sys, re, random, string
from time import sleep
from requests import Timeout
import logging

logger = logging.getLogger("ti.fakes")
logger.setLevel(logging.INFO)

requests_log = logging.getLogger("requests").setLevel(
    logging.WARNING) # Requests' logging is too noisy


# setup external services
webapp_url = "http://" + os.getenv("WEBAPP_ROOT")
api_url = "http://" + os.getenv("API_ROOT")

''' Test classes
*****************************************************************************'''

class Importer:
    '''Emulates a single importer on the create_collection page;

    feed it a provider name like "github" at instantiation, then run run a query
    like jasonpriem to get all the aliases associated with that account
    '''
    query = None

    def __init__(self, provider_name):
        self.provider_name = provider_name

    def get_aliases(self, query):
        query_url = "{api_url}/provider/{provider_name}/memberitems?query={query}".format(
            api_url=api_url,
            provider_name=self.provider_name,
            query=query
        )
        start = time.time()
        logger.info(
            "getting aliases from the {provider} importer, using url '{url}'".format(
                provider=self.provider_name,
                url=query_url
            ))
        r = requests.get(query_url)

        try:
            aliases = json.loads(r.text)
            logger.debug("got some aliases from the http call: " + str(aliases))
        except ValueError:
            logger.warning(
                "{provider} importer returned no json for {query}".format(
                    provider=self.provider_name,
                    query="query"
                ))
            aliases = []

        # anoyingly, some providers return lists-as-IDs, which must be joined with a comma
        aliases = [(namespace, id) if isinstance(id, str)
                   else (namespace, ",".join(id)) for namespace, id in aliases]

        logger.info(
            "{provider} importer got {num_aliases} aliases with username '{q}' in {elapsed} seconds.".format(
                provider=self.provider_name,
                num_aliases=len(aliases),
                q=query,
                elapsed=round(time.time() - start, 2),
                ))

        return aliases


class ReportPage:
    def __init__(self, collection_id):
        self.collection_id = collection_id
        start = time.time()
        logger.info(
            "loading the report page for collection '{collection_id}'.".format(
                collection_id=collection_id
            ))
        request_url = "{webapp_url}/collection/{collection_id}".format(
            webapp_url=webapp_url,
            collection_id=collection_id
        )
        resp = requests.get(request_url)
        if resp.status_code == 200:
            self.collectionId = self._get_collectionId(resp.text)
            elapsed = time.time() - start
            logger.info(
                "loaded the report page for '{collection_id}' in {elapsed} seconds.".format(
                    collection_id=collection_id,
                    elapsed=elapsed
                ))
        else:
            logger.warning(
                "report page for '{collection_id}' failed to load! ({url})".format(
                    collection_id=collection_id,
                    url=request_url
                ))


    def _get_collectionId(self, text):
        """gets id of the collection to poll. in the real report page, this is done
        via a 'collectionId' javascript var that's placed there when the view constructs
        the page. this method parses the page to get them...it's brittle, though,
        since if the report page changes this breaks."""

        m = re.search('var collectionId = "([^"]+)"', text)
        collectionId = m.group(1)
        return collectionId


    def poll(self, max_time=60):
        logger.info(
            "polling collection '{collection_id}'".format(
                collection_id=self.collection_id
            ))

        still_updating = True
        tries = 0
        start = time.time()
        while still_updating:
            url = api_url + "/collection/" + self.collectionId
            resp = requests.get(url, config={'verbose': None})
            try:
                coll = json.loads(resp.text)
                items = coll["items"]
            except ValueError:
                logger.warning(
                    "POSTing '{url}' returned no json, only '{resp}') ".format(
                        url=url,
                        resp=resp.text
                    ))

            tries += 1

            num_finished_updating = len(items) - coll["num_items_updating"]

            logger.info(
                "{num_done} of {num_total} items done updating after {tries} requests.".format(
                    num_done=num_finished_updating,
                    num_total=len(items),
                    tries=tries
                ))
            logger.debug("got these items back: " + str(items))

            elapsed = time.time() - start
            if resp.status_code == 200:
                logger.info(
                    "collection '{id}' with {num_items} items finished updating in {elapsed} seconds.".format(
                        id=self.collection_id,
                        num_items=len(items),
                        elapsed=round(elapsed, 2)
                    ))
                return True
            elif elapsed > max_time:
                raise Exception(
                    "max polling time ({max} secs) exceeded for collection {id}. These items didn't update: {item_ids}".format(
                        max=max_time,
                        id=self.collection_id,
                        item_ids=", ".join([item["_id"] for item in items if
                                            item["currently_updating"]])
                    ))
                return False

            sleep(0.5)


class CreateCollectionPage:
    def __init__(self):
        self.reload()

    def reload(self):
        start = time.time()
        logger.info("loading the create-collection page")
        resp = requests.get(webapp_url + "/create")
        if resp.status_code == 200:
            elapsed = time.time() - start
            logger.info(
                "loaded the create-collection page in {elapsed} seconds.".format(
                    elapsed=elapsed
                ))
        else:
            logger.warning(
                "create-collection page for '{collection_id}' failed to load!".format(
                    collection_id=collection_id
                ))
        self.aliases = []
        self.collection_name = "My collection"

    def set_collection_name(self, collection_name):
        self.collection_name = collection_name

    def enter_aliases_directly(self, aliases):
        self.aliases = self.aliases + aliases
        return self.aliases

    def get_aliases_with_importers(self, provider_name, query):
        importer = Importer(provider_name)
        aliases_from_this_importer = importer.get_aliases(query)
        self.aliases = self.aliases + aliases_from_this_importer
        return self.aliases

    def press_go_button(self):
        logger.info(
            "user has pressed the 'go' button on the create-collection page.")
        if len(self.aliases) == 0:
            raise ValueError("Trying to create a collection with no aliases.")

        tiids = self._create_items()
        collection_id = self._create_collection(tiids)
        report_page = ReportPage(collection_id)
        report_page.poll()
        return collection_id

    def _create_items(self):
        start = time.time()
        logger.info("trying to create {num_aliases} new items.".format(
            num_aliases=len(self.aliases)
        ))
        query = api_url + '/items'
        data = json.dumps(self.aliases)
        resp = requests.post(
            query,
            data=data,
            headers={'Content-type': 'application/json'}
        )

        try:
            tiids = json.loads(resp.text)
        except ValueError:
            logger.warning(
                "POSTing {query} endpoint with data '{data}' returned no json, only '{resp}') ".format(
                    query=query,
                    data=data,
                    resp=resp.text
                ))
            raise ValueError

        logger.info("created {num_items} items in {elapsed} seconds.".format(
            num_items=len(self.aliases),
            elapsed=round(time.time() - start, 2)
        ))

        logger.debug("created these new items: " + str(tiids))

        return tiids

    def _create_collection(self, tiids):
        start = time.time()
        url = api_url + "/collection"
        collection_name = "[ti test] " + self.collection_name

        logger.info("creating collection with {num_tiids} tiids".format(
            num_tiids=len(tiids)
        ))
        logger.debug("creating collection with these tiids: " + str(tiids))

        resp = requests.post(
            url,
            data=json.dumps({
                "items": tiids,
                "title": collection_name
            }),
            headers={'Content-type': 'application/json'}
        )
        collection_id = json.loads(resp.text)["_id"]

        logger.info(
            "created collection '{id}' with {num_items} items in {elapsed} seconds.".format(
                id=collection_id,
                num_items=len(self.aliases),
                elapsed=round(time.time() - start, 2)
            ))

        return collection_id

    def clean_db(self):
        pass


class IdSampler(object):
    def get_dois(self, num=1):
        start = time.time()
        dois = []
        url = "http://random.labs.crossref.org/dois?from=2000&count=" + str(num)
        logger.info(
            "getting {num} random dois with IdSampler, using {url}".format(
                num=num,
                url=url
            ))
        try:
            r = requests.get(url, timeout=10)
        except Timeout:
            logger.warning(
                "the random doi service isn't working right now (timed out); sending back an empty list.")
            return dois

        if r.status_code == 200:
            dois = json.loads(r.text)
            logger.info(
                "IdSampler got {count} random dois back in {elapsed} seconds".format(
                    count=len(dois),
                    elapsed=round(time.time() - start, 2)
                ))
            logger.debug("IdSampler got these dois back: " + str(dois))
        else:
            logger.warning(
                "the random doi service isn't working right now (got error code); sending back an empty list.")

        return dois

    def get_github_username(self):
        start = time.time()
        db_url = "http://total-impact.cloudant.com/github_usernames"
        rand_hex_string = hex(random.getrandbits(128))[
                          2:-1] # courtesy http://stackoverflow.com/a/976607/226013
        req_url = db_url + '/_all_docs?include_docs=true&limit=1&startkey="{startkey}"'.format(
            startkey=rand_hex_string
        )
        logger.info(
            "getting a random github username with IdSampler, using {url}".format(
                url=req_url
            ))
        r = requests.get(req_url)
        json_resp = json.loads(r.text)

        username = json_resp["rows"][0]["doc"]["actor"]
        logger.info(
            "IdSampler got random github username '{username}' in {elapsed} seconds".format(
                username=username,
                elapsed=round(time.time() - start, 2)
            ))

        return username

