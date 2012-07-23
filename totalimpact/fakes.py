import os, requests, json, datetime, time, sys, re, random, string
from time import sleep
import logging


logger = logging.getLogger("ti.fakes")

#quiet Requests' noisy logging:
requests_log = logging.getLogger("requests").setLevel(logging.WARNING)

# Don't get db from env variable because don't want to kill production by mistake
base_db_url = os.getenv("CLOUDANT_URL")
base_db = os.getenv("CLOUDANT_DB")
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
            logger.warning("{provider} importer returned no json for {query}".format(
                provider=self.provider_name,
                query="query"
            ))
            aliases = []

        # anoyingly, some providers return lists-as-IDs, which must be joined with a comma
        aliases = [(namespace, id) if isinstance(id, str)
            else (namespace, ",".join(id)) for namespace, id in aliases]

        logger.info("{provider} importer got {num_aliases} aliases with username '{q}' in {elapsed} seconds.".format(
            provider = self.provider_name,
            num_aliases = len(aliases),
            q = query,
            elapsed = round(time.time() - start, 2),
        ))

        return aliases



class ReportPage:
    def __init__(self, collection_id):
        self.collection_id = collection_id
        start = time.time()
        logger.info("loading the report page for collection '{collection_id}'.".format(
            collection_id=collection_id
        ))
        request_url = "{webapp_url}/collection/{collection_id}".format(
            webapp_url=webapp_url,
            collection_id=collection_id
        )
        resp = requests.get(request_url)
        if resp.status_code == 200:
            self.tiids = self._get_tiids(resp.text)
            elapsed = time.time() - start
            logger.info("loaded the report page for '{collection_id}' in {elapsed} seconds.".format(
                collection_id=collection_id,
                elapsed=elapsed
            ))
        else:
            logger.warning("report page for '{collection_id}' failed to load! ({url})".format(
                collection_id = collection_id,
                url=request_url
            ))


    def _get_tiids(self, text):
        """gets the list of tiids to poll. in the real report page, this is done
        via a 'tiids' javascript var that's placed there when the view constructs
        the page. this method parses the page to get them...it's brittle, though,
        since if the report page changes this breaks."""

        m = re.search("var tiids = (\[[^\]]+\])", text)
        tiids = json.loads(m.group(1))
        return tiids


    def poll(self, max_time=50):

        logger.info("polling the {num_tiids} tiids of collection '{collection_id}'".format(
            num_tiids = len(self.tiids),
            collection_id = self.collection_id
        ))

        tiids_str = ",".join(self.tiids)
        still_updating = True
        tries = 0
        start = time.time()
        while still_updating:

            url = api_url+"/items/"+tiids_str
            resp = requests.get(url, config={'verbose': None})
            items = json.loads(resp.text)
            tries += 1

            currently_updating_flags = [True for item in items if item["currently_updating"]]
            num_currently_updating = len(currently_updating_flags)
            num_finished_updating = len(self.tiids) - num_currently_updating

            logger.info("{num_done} of {num_total} items done updating after {tries} requests.".format(
                num_done=num_finished_updating,
                num_total=len(self.tiids),
                tries=tries
            ))
            logger.debug("got these items back: " + str(items))

            elapsed = time.time() - start
            if resp.status_code == 200:
                logger.info("collection '{id}' finished updating in {elapsed} seconds.".format(
                    id=self.collection_id,
                    elapsed=elapsed
                ))
                return True
            elif elapsed > max_time:
                logger.error("max polling time ({max} secs) exceeded for collection '{id}'; giving up.".format(
                    max=max_time,
                    id=self.collection_id
                ))
                logger.error("these items in collection '{id}' didn't update: {item_ids}".format(
                    id=self.collection_id,
                    item_ids=", ".join([item["id"] for item in items if item["currently_updating"]])
                ))
                return False

            sleep(0.5)





class CreateCollectionPage:

    def __init__(self):
        self.reload()

    def reload(self):
        start = time.time()
        logger.info("loading the create-collection page")
        resp = requests.get(webapp_url+"/create")
        if resp.status_code == 200:
            elapsed = time.time() - start
            logger.info("loaded the create-collection page in {elapsed} seconds.".format(
                elapsed=elapsed
            ))
        else:
            logger.warning("create-collection page for '{collection_id}' failed to load!".format(
                collection_id = collection_id
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
        logger.info("user has pressed the 'go' button on the create-collection page.")
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
            num_aliases = len(self.aliases)
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
            logger.warning("POSTing {query} endpoint with data '{data}' returned no json, only '{resp}') ".format(
                query=query,
                data=data,
                resp=resp
            ))
            raise ValueError

        logger.info("created {num_items} items in {elapsed} seconds.".format(
            num_items = len(self.aliases),
            elapsed = round(time.time() - start, 2)
            ))

        logger.debug("created these new items: " + str(tiids))

        return tiids

    def _create_collection(self, tiids):
        start = time.time()
        url = api_url+"/collection"
        collection_name = "[ti test] " + self.collection_name

        logger.info("creating collection with {num_tiids} tiids".format(
            num_tiids = len(tiids)
        ))
        logger.debug("creating collection with these tiids: " + str(tiids))

        resp = requests.post(
            url,
            data = json.dumps({
                "items": tiids,
                "title": collection_name
            }),
            headers={'Content-type': 'application/json'}
        )
        collection_id = json.loads(resp.text)["id"]

        logger.info("created collection '{id}' with {num_items} items in {elapsed} seconds.".format(
            id=collection_id,
            num_items = len(self.aliases),
            elapsed = round(time.time() - start, 2)
            ))

        return collection_id

    def clean_db(self):
        pass


class IdSampler(object):

    def get_dois(self, num=1):
        start = time.time()
        url = "http://random.labs.crossref.org/dois?count="+str(num)
        logger.info("getting {num} random dois with IdSampler, using {url}".format(
            num=num,
            url=url
        ))
        r = requests.get(url)
        if r.status_code == 200:
            dois = json.loads(r.text)
            logger.info("IdSampler got {count} random dois back in {elapsed} seconds".format(
                count=len(dois),
                elapsed=round(time.time() - start, 2)
            ))
            logger.debug("IdSampler got these dois back: " + str(dois))
        else:
            logger.warning("the random doi service isn't working right now; sending back an empty list.")
            dois = []

        return dois

    def get_github_username(self):
        start = time.time()
        db_url = "http://total-impact.cloudant.com/github_usernames"
        rand_hex_string = hex(random.getrandbits(128))[2:-1] # courtesy http://stackoverflow.com/a/976607/226013
        req_url = db_url + '/_all_docs?include_docs=true&limit=1&startkey="{startkey}"'.format(
            startkey=rand_hex_string
        )
        logger.info("getting a random github username with IdSampler, using {url}".format(
            url=req_url
        ))
        r = requests.get(req_url)
        json_resp = json.loads(r.text)

        username = json_resp["rows"][0]["doc"]["actor"]
        logger.info("IdSampler got random github username '{username}' in {elapsed} seconds".format(
            username=username,
            elapsed=round(time.time() - start, 2)
        ))

        return username


class Person(object):

    def do(self, action_type):
        start = time.time()
        interaction_name = ''.join(random.choice(string.ascii_lowercase) for x in range(5))
        logger.info("Fakes.user.{action_type} interaction '{interaction_name}' starting now".format(
            action_type=action_type,
            interaction_name=interaction_name
        ))

        try:
            error_str = None
            result = getattr(self, action_type)(interaction_name)
        except Exception, e:
            error_str = e.__repr__()
            logger.exception("Fakes.user.{action_type} '{interaction_name}' threw an error:'".format(
            action_type=action_type,
            interaction_name=interaction_name,
            error=e.__repr__()
            ))
            result = None

        end = time.time()
        elapsed = end - start
        logger.info("Fakes.user finished {name} interaction in {elapsed} seconds.".format(
        name=action_type,
        elapsed=elapsed
        ))

        # this is a dumb way to do the times; should be using time objects, not stamps
        report = {
            "start": datetime.datetime.fromtimestamp(start).strftime('%m-%d %H:%M:%S'),
            "end": datetime.datetime.fromtimestamp(end).strftime('%m-%d %H:%M:%S'),
            "elapsed": round(elapsed, 2),
            "action": action_type,
            "name": interaction_name,
            "result":result,
            "error_str": error_str
        }
        logger.info("finished doing '{action_type}' interaction test. Here's the report: {report}".format(
            action_type=action_type,
            report=str(report)
        ))
        return report


    def make_collection(self, interaction_name):
        logger.info("starting make_collection interaction script.")
        ccp = CreateCollectionPage()

        sampler = IdSampler()
        ccp.enter_aliases_directly([["doi", x] for x in sampler.get_dois(5)])
        ccp.get_aliases_with_importers("github", sampler.get_github_username())
        ccp.set_collection_name(interaction_name)
        return ccp.press_go_button()

    def upate_collection(self, interaction_name):
         pass

    def check_collection(self, interaction_name):
         pass
