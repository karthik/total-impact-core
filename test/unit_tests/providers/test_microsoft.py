from test.unit_tests.providers import common
from test.unit_tests.providers.common import ProviderTestCase
from totalimpact.providers.provider import Provider, ProviderContentMalformedError, ProviderClientError, ProviderServerError
from test.utils import http

import os
import collections
from nose.tools import assert_equals, raises, nottest

datadir = os.path.join(os.path.split(__file__)[0], "../../../extras/sample_provider_pages/microsoft")
SAMPLE_EXTRACT_METRICS_PAGE = os.path.join(datadir, "metrics")

TEST_DOI = "10.1371/journal.pone.0000308"

class TestMicrosoft(ProviderTestCase):

    provider_name = "microsoft"

    testitem_aliases = ("doi", TEST_DOI)
    testitem_alias_tuples = [('doi', ['10.1371/journal.pone.0000308']), ('biblio', [{'title': 'Mutations causing syndromic autism define an axis of synaptic pathophysiology', 'authors': 'sdf', 'year': 2011}])]
    testitem_metrics = ("doi", TEST_DOI)
    testitem_metrics_dict = {"biblio":[{"year":2011, "authors":"sdf", "title": "Mutations causing syndromic autism define an axis of synaptic pathophysiology"}],"doi":[TEST_DOI]}

    def setUp(self):
        ProviderTestCase.setUp(self)

    def test_is_relevant_alias(self):
        # ensure that it matches an appropriate ids
        assert_equals(self.provider.is_relevant_alias(self.testitem_aliases), True)

    def test_extract_metrics_success(self):
        f = open(SAMPLE_EXTRACT_METRICS_PAGE, "r")
        good_page = f.read()
        metrics_dict = self.provider._extract_metrics(good_page)
        print metrics_dict
        expected = {'scienceseeker:blog_posts': 1}
        assert_equals(metrics_dict, expected)

    def test_provenance_url(self):
        provenance_url = self.provider.provenance_url("blog_posts", 
            [self.testitem_aliases])
        expected = 'http://academic.research.microsoft.com/Publication/10.1371/journal.pone.0000308'
        assert_equals(provenance_url, expected)

    def test_get_mas_id_from_title(self):
        f = open(SAMPLE_EXTRACT_METRICS_PAGE, "r")
        uuid = self.provider._get_mas_id_from_title(self.testitem_metrics_dict, f.read())
        expected = "1f471f70-1e4f-11e1-b17d-0024e8453de6"
        assert_equals(uuid, expected)

    # override common tests
    @raises(ProviderClientError, ProviderServerError)
    def test_provider_metrics_400(self):
        if not self.provider.provides_metrics:
            raise SkipTest
        Provider.http_get = common.get_400
        metrics = self.provider.metrics(self.testitem_alias_tuples)

    @raises(ProviderServerError)
    def test_provider_metrics_500(self):
        if not self.provider.provides_metrics:
            raise SkipTest
        Provider.http_get = common.get_500
        metrics = self.provider.metrics(self.testitem_alias_tuples)

    @raises(ProviderContentMalformedError)
    def test_provider_metrics_empty(self):
        if not self.provider.provides_metrics:
            raise SkipTest
        Provider.http_get = common.get_empty
        metrics = self.provider.metrics(self.testitem_alias_tuples)

    @raises(ProviderContentMalformedError)
    def test_provider_metrics_nonsense_txt(self):
        if not self.provider.provides_metrics:
            raise SkipTest
        Provider.http_get = common.get_nonsense_txt
        metrics = self.provider.metrics(self.testitem_alias_tuples)

    @raises(ProviderContentMalformedError)
    def test_provider_metrics_nonsense_xml(self):
        if not self.provider.provides_metrics:
            raise SkipTest
        Provider.http_get = common.get_nonsense_xml
        metrics = self.provider.metrics(self.testitem_alias_tuples)

"""    
    @http
    def test_metrics(self):
        metrics_dict = self.provider.metrics([self.testitem_metrics])
        expected = {'scienceseeker:blog_posts': (1, 'http://scienceseeker.org/displayfeed/?type=post&filter0=citation&modifier0=doi&value0=10.1016/j.cbpa.2010.06.169')}
        print metrics_dict
        for key in expected:
            assert metrics_dict[key][0] >= expected[key][0], [key, metrics_dict[key], expected[key]]
            assert metrics_dict[key][1] == expected[key][1], [key, metrics_dict[key], expected[key]]

    @http
    def test_metrics_another(self):
        metrics_dict = self.provider.metrics([("doi", "10.1371/journal.pone.0035769")])
        expected = {'scienceseeker:blog_posts': (1, 'http://scienceseeker.org/displayfeed/?type=post&filter0=citation&modifier0=doi&value0=10.1371/journal.pone.0035769')}
        print metrics_dict
        for key in expected:
            assert metrics_dict[key][0] >= expected[key][0], [key, metrics_dict[key], expected[key]]
            assert metrics_dict[key][1] == expected[key][1], [key, metrics_dict[key], expected[key]]
"""