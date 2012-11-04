from totalimpact.providers import provider
from totalimpact.providers.provider import Provider, ProviderContentMalformedError, ProviderServerError

import simplejson, re, os, random, string

import logging
logger = logging.getLogger('ti.providers.microsoft')

class Microsoft(Provider):  

    example_id = ("doi", "10.1371/journal.pone.0000308")

    url = "http://academic.research.microsoft.com/"
    descr = "Improving the discoverability of scholarly materials.."
    # template urls below because they need a freshly-minted random string
    metrics_url_template = "http://academic.research.microsoft.com/json.svc/search?AppID=" + os.environ["MICROSOFT_KEY"] + '&ResultObjects=Publication&FullTextQuery="%s"&OrderBy=Rank&startidx=1&endidx=100'

    provenance_url_template = "http://academic.research.microsoft.com/Publication/%s"

    static_meta_dict =  { 
        "citations": {
            "display_name": "citations",
            "provider": "Microsoft",
            "provider_url": "http://academic.research.microsoft.com/",
            "description": "Number of times the item has been cited",
            "icon": "http://academic.research.microsoft.com/favicon.ico" ,
            
        }
    }
    

    def __init__(self):
        super(Microsoft, self).__init__()

    def is_relevant_alias(self, alias):
        (namespace, nid) = alias
        # right now restricted to doi because we check the title lookup matches doi
        ## to keep precision high.  Later could experiment with opening this up.
        relevant = (namespace=="doi")
        return(relevant)

    def _extract_metrics(self, page, status_code=200, id=None):
        if not "identifiers" in page:
            raise ProviderContentMalformedError()

        dict_of_keylists = {"mendeley:readers": ["stats", "readers"], 
                            "mendeley:discipline": ["stats", "discipline"],
                            "mendeley:career_stage": ["stats", "status"],
                            "mendeley:country": ["stats", "country"],
                            "mendeley:groups" : ["groups"]}

        metrics_dict = provider._extract_from_json(page, dict_of_keylists)

        # get count of groups
        try:
            metrics_dict["mendeley:groups"] = len(metrics_dict["mendeley:groups"])
        except (TypeError, KeyError):
            # don't add null or zero metrics
            pass

        return metrics_dict


    def _extract_provenance_url(self, page, status_code=200, id=None):
        data = provider._load_json(page)
        try:
            provenance_url = data['mendeley_url']
        except KeyError:
            provenance_url = ""
        return provenance_url        

    def _get_page(self, url):
        response = self.http_get(url)
        if response.status_code != 200:
            if response.status_code == 404:
                return None
            else:
                raise(self._get_error(response.status_code))
        return response.text
         
    def _get_mas_id_lookup_page(self, title):
        uuid_from_title_url = self.uuid_from_title_template % title     
        page = self._get_page(uuid_from_title_url)
        if not page:
            raise ProviderContentMalformedError()            
        if not "documents" in page:
            raise ProviderContentMalformedError()
        return page

    def _get_metrics_lookup_page(self, template, id):
        double_encoded_id = urllib.quote(urllib.quote(id, safe=""), safe="")
        metrics_url = template %double_encoded_id
        page = self._get_page(metrics_url)
        if page:
            if not "identifiers" in page:
                page = None
        return page

    @classmethod
    def remove_punctuation(cls, input):
        # from http://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
        no_punc = input
        if input:
            no_punc = "".join(e for e in input if (e.isalnum() or e.isspace()))
        return no_punc

    def _get_masid_from_title(self, aliases_dict, page):
        data = provider._load_json(page)
        doi = aliases_dict["doi"][0]
        biblio = aliases_dict["biblio"][0]
        for mendeley_record in data["documents"]:
            if mendeley_record["doi"] == doi:
                uuid = mendeley_record["uuid"]
                return uuid
            else:
                # more complicated.  Try to match title and year.
                try:
                    mendeley_title = self.remove_punctuation(mendeley_record["title"]).lower()
                    aliases_title = self.remove_punctuation(biblio["title"]).lower()
                except (TypeError, KeyError, AttributeError):
                    continue  # nothing to see here.  Skip to next record

                if mendeley_title == aliases_title:
                    if mendeley_record["year"] == biblio["year"]:
                        # check if author name in common. if not, yell, but continue anyway
                        first_mendeley_surname = mendeley_record["authors"][0]["surname"]
                        has_matching_authors = first_mendeley_surname.lower() in biblio["authors"].lower()
                        if not has_matching_authors:
                            logger.warning("Mendeley: NO MATCHING AUTHORS between %s and %s" %(
                                first_mendeley_surname, biblio["authors"]))
                        # but return it anyway
                        uuid = mendeley_record["uuid"]
                        return uuid
                    else:
                        logger.debug("Mendeley: years don't match %s and %s" %(
                            str(mendeley_record["year"]), str(biblio["year"])))
                else:
                    logger.debug("Mendeley: titles don't match %s and %s" %(
                        self.remove_punctuation(mendeley_record["title"]), self.remove_punctuation(biblio["title"])))
        # no joy
        return None

    def _get_metrics_and_drilldown_from_metrics_section(self, section):
        metrics_dict = self._extract_metrics(section)
        metrics_and_drilldown = {}
        for metric_name in metrics_dict:
            drilldown_url = self._extract_provenance_url(page)
            metrics_and_drilldown[metric_name] = (metrics_dict[metric_name], drilldown_url)
        return metrics_and_drilldown  


    # default method; providers can override
    def metrics(self, 
            aliases,
            provider_url_template=None, # ignore this because multiple url steps
            cache_enabled=True):

        # Only lookup metrics for items with appropriate ids
        from totalimpact.models import ItemFactory
        aliases_dict = ItemFactory.alias_dict_from_tuples(aliases)
        print aliases_dict

        metrics_section = None    
        # lookup by title
        try:
            page = self._get_mas_id_lookup_page(aliases_dict["biblio"][0]["title"])
            if not page:
                raise ProviderServerError()
            if not "research.microsoft.com" in page:
                raise ProviderServerError()
            if page:
                uuid = self._get_uuid_from_title(aliases_dict, page)
                if uuid:
                    logger.debug("Mendeley: uuid is %s for %s" %(uuid, aliases_dict["biblio"][0]["title"]))
                    metrics_section = self._get_metrics_lookup_page(self.metrics_from_uuid_template, uuid)
                else:
                    logger.debug("Mendeley: couldn't find uuid for %s" %(aliases_dict["biblio"][0]["title"]))
        except (KeyError, TypeError):
            raise ProviderServerError()
        # give up!
        if not metrics_section:
            return {}

        metrics_and_drilldown = self._get_metrics_and_drilldown_from_metrics_section(metrics_section)

        return metrics_and_drilldown

