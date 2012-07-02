from totalimpact.providers import provider
from totalimpact.providers.provider import Provider, ProviderContentMalformedError

import simplejson
import re

import logging
logger = logging.getLogger('providers.peerev')

class Peerev(Provider):  

    example_id = ("doi", "10.1215/00182702-2008-041")

    url = "http://peerevaluation.org/"
    descr = "PeerEvaluation.  Empowering Scholars."
    
    metrics_url_template = "http://peerevaluation.org/api/libraryID:%s"
    provenance_url_template = "%s"

    static_meta_dict =  {
        "views": {
            "display_name": "views",
            "provider": "Peer Evaluation",
            "provider_url": "http://peerevaluation.org/",
            "description": "Number of views on peerev.org.",
            "icon": "http://test.peerevaluation.org/img/peerev.gif" ,
        },   
        "bookmarks": {
            "display_name": "bookmarks",
            "provider": "Peer Evaluation",
            "provider_url": "http://peerevaluation.org/",
            "description": "Number of bookmarks on peerev.org.",
            "icon": "http://test.peerevaluation.org/img/peerev.gif" ,
        },  
        "comments": {
            "display_name": "comments",
            "provider": "Peer Evaluation",
            "provider_url": "http://peerevaluation.org/",
            "description": "Number of comments on peerev.org.",
            "icon": "http://test.peerevaluation.org/img/peerev.gif" ,
        }, 
        "downloads": {
            "display_name": "downloads",
            "provider": "Peer Evaluation",
            "provider_url": "http://peerevaluation.org/",
            "description": "Number of downloads on peerev.org.",
            "icon": "http://test.peerevaluation.org/img/peerev.gif" ,
        }  
    }
    

    def __init__(self):
        super(Peerev, self).__init__()

    def is_relevant_alias(self, alias):
        (namespace, nid) = alias
        return("doi" == namespace)


    def _extract_metrics(self, page, status_code=200, id=None):
        if status_code != 200:
            if status_code == 404:
                return {}
            else:
                raise(self._get_error(status_code))

        try:
            page_json = provider._load_json(page)
            page_section = page_json["items"][0]["metrics"]

            metrics_dict = {}
            for peerev_metric_dict in page_section:
                print peerev_metric_dict
                metric_name = peerev_metric_dict["display_name"]
                metrics_dict[metric_name] = peerev_metric_dict["value"]

        except KeyError:
            raise ProviderContentMalformedError


        return metrics_dict


    def provenance_url(self, metric_name, aliases):
        id = self.get_best_id(aliases)   
        print id  
        if not id:
            # not relevant
            return None

        url = self.metrics_url_template % id
        response = self.http_get(url, method="provenance_url")
        if response.status_code != 200:
            return ""

        try:
            page_json = provider._load_json(response.text)
            provenance_url = page_json["items"][0]["drilldown_url"]
        except KeyError:
            raise ProviderContentMalformedError

        return provenance_url


