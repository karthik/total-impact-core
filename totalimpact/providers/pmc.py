import hashlib, simplejson, os, couchdb, collections

from totalimpact.providers import provider
from totalimpact.providers.provider import Provider, ProviderContentMalformedError

import logging
logger = logging.getLogger('ti.providers.pmc')

batch_data = None

class Pmc(Provider):  

    example_id = ("pmid", "23066504")
    provenance_url_template = None
    url = "http://www.ncbi.nlm.nih.gov/pmc"
    descr = "a free archive of biomedical and life sciences journal literature at the NIH/NLM"
    static_meta_dict = {
        "pdf_downloads": {
            "display_name": "PDF downloads",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of times the PDF has been downloaded from PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        },
        "abstract_views": {
            "display_name": "abstract views",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of times the abstract has been viewed on PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        },        
        "fulltext_views": {
            "display_name": "fulltext views",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of times the full text has been viewed on PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        },        
        "unique_ip_views": {
            "display_name": "unique IP views",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of unique IP addresses that have viewed this on PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        },         
        "figure_views": {
            "display_name": "figure views",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of times the figures have been viewed on PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        },        
        "suppdata_views": {
            "display_name": "suppdata views",
            "provider": "PMC",
            "provider_url": "http://www.ncbi.nlm.nih.gov/pmc",
            "description": "Number of times the supplementary data has been viewed on PMC",
            "icon": "http://www.ncbi.nlm.nih.gov/favicon.ico"
        }               
    }

    def __init__(self, mydao=None):
        global batch_data
        if not batch_data:
            batch_data = self.build_batch_data_dict(mydao)
        super(Pmc, self).__init__()

    def is_relevant_alias(self, alias):
        (namespace, nid) = alias
        return("pmid" == namespace)

    def build_batch_data_dict(self, mydao=None):
        if not mydao:
            from totalimpact import dao
            mydao = dao.Dao(os.environ["CLOUDANT_URL"], os.environ["CLOUDANT_DB"])

        results = mydao.db.view('provider_batch_data/by_alias_provider_batch_data', include_docs=True)

        batch_data = collections.defaultdict(list)

        try:
            rows = results.rows
        except couchdb.ResourceNotFound:
            return None

        for row in results.rows:
            [provider, [namespace, nid]] = row.key
            if provider == "pmc":
                pmid_alias = (namespace, nid)
                batch_data[pmid_alias] += [{"raw": row.doc["raw"], "max_event_date":row.value}]

        return batch_data

    def has_applicable_batch_data(self, namespace, nid, mydao):
        has_applicable_batch_data = False

        res = mydao.view('provider_batch_data/by_alias_provider_batch_data')
        # for expl of notation, see http://packages.python.org/CouchDB/client.html#viewresults
        matches = res["pmc", [namespace, nid]] 

        if matches.rows:
            if len(matches.rows) > 0:
                has_applicable_batch_data = True

        return has_applicable_batch_data

    def _extract_metrics(self, page, status_code=200, id=None): 
        if status_code != 200:
            if status_code == 404:
                return {}
            else:
                raise(self._get_error(status_code))

        if "<pmc-web-stat>" not in page:
            raise ProviderContentMalformedError

        (doc, lookup_function) = provider._get_doc_from_xml(page)
        if not doc:
            return {}
        try:
            articles = doc.getElementsByTagName("article")
            for article in articles:
                metrics_dict = {}            
                meta_data = article.getElementsByTagName("meta-data")[0]
                pmid = meta_data.getAttribute("pubmed-id")
                if id == pmid:
                    metrics = article.getElementsByTagName("usage")[0]
                    pdf_downloads = int(metrics.getAttribute("pdf"))
                    if pdf_downloads:
                        metrics_dict.update({'pmc:pdf_downloads': pdf_downloads})

                    abstract_views = int(metrics.getAttribute("abstract"))
                    if abstract_views:
                        metrics_dict.update({'pmc:abstract_views': abstract_views})

                    fulltext_views = int(metrics.getAttribute("full-text"))
                    if fulltext_views:
                        metrics_dict.update({'pmc:fulltext_views': fulltext_views})

                    unique_ip_views = int(metrics.getAttribute("unique-ip"))
                    if unique_ip_views:
                        metrics_dict.update({'pmc:unique_ip_views': unique_ip_views})

                    figure_views = int(metrics.getAttribute("figure"))
                    if figure_views:
                        metrics_dict.update({'pmc:figure_views': figure_views})

                    suppdata_views = int(metrics.getAttribute("supp-data"))
                    if suppdata_views:
                        metrics_dict.update({'pmc:suppdata_views': suppdata_views})

                    return metrics_dict

        except (KeyError, IndexError, TypeError):
            pass

        return {}

    def _get_metrics_and_drilldown(self, pages, pmid):
        metrics_dict = {}
        for page in pages:
            one_month_metrics_dict = self._extract_metrics(page, id=pmid)
            print one_month_metrics_dict
            for metric in one_month_metrics_dict:
                try:
                    metrics_dict[metric] += one_month_metrics_dict[metric]
                except KeyError:
                    metrics_dict[metric] = one_month_metrics_dict[metric]
        metrics_and_drilldown = {}
        for metric_name in metrics_dict:
            drilldown_url = ""
            metrics_and_drilldown[metric_name] = (metrics_dict[metric_name], drilldown_url)
        return metrics_and_drilldown

    def metrics(self, 
            aliases,
            provider_url_template=None, # ignore this because multiple url steps
            cache_enabled=True):

        metrics_and_drilldown = {}

        # Only lookup metrics for items with appropriate ids
        from totalimpact import item
        aliases_dict = item.alias_dict_from_tuples(aliases)
        try:
            pmid = aliases_dict["pmid"][0]
        except KeyError:
            return {}
            
        pmid_alias = ("pmid", pmid)
        page = ""

        global batch_data
        if pmid_alias in batch_data:
            pages = [page["raw"] for page in batch_data[pmid_alias]]
        if page:
            metrics_and_drilldown = self._get_metrics_and_drilldown(pages, pmid)

        return metrics_and_drilldown


