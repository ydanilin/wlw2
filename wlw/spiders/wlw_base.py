# -*- coding: utf-8 -*-
import logging
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from scrapy.shell import inspect_response
from ..items import WlwItem, WlwLoader

logger = logging.getLogger(__name__)


class WlwBaseSpider(CrawlSpider):
    def __init__(self, *args, **kwargs):
        super(WlwBaseSpider, self).__init__(*args, *kwargs)
        self.jobState = None

    name = 'wlw_base'
    allowed_domains = ['wlw.de']
    start_urls = []

    def addNameInUrl(request):
        part = request.url.rsplit('?', 1)[0]
        nameInUrl = part.rsplit('/', 1)[1]
        request.meta['job_dat'] = dict(nameInUrl=nameInUrl)
        return request

    def addFirmaId(request):
        part = request.url.split('?')[0]
        firm = part.rsplit('/', 1)[1]
        grp = re.search(r'\d+', firm)
        if grp:
            output = int(grp.group())
        else:
            output = 0
            logger.error('cannot parse firmaId for url %s' % request.url)
        request.meta['job'] = {'firmaId': output}
        return request

    def setPage(request):
        grp = re.search(r'page=(\d+)', request.url)
        if grp:
            page = int(grp.group())
        else:
            page = 0
            logger.error('cannot parse page No for url %s' % request.url)
        request.meta['job'] = {'page': page}
        return request

    rules = (
        # 0. go to next page
        Rule(LinkExtractor(restrict_xpaths=('//ul[@class="pagination"]/'
                                            'li[not(@class)]/'
                                            'a[text()[contains(.,"chste")]]')
                           ),
             process_request=setPage),
        # 1. from firms list to specific firm
        Rule(LinkExtractor(
            restrict_xpaths='//a[@data-track-type="click_serp_company_name"]'),
            callback='parse_group', process_request=addFirmaId)
    )

    def start_requests(self):
        self.start_urls = self.jobState.getStartUrls()
        for url in self.start_urls:
            fullUrl = 'https://www.wlw.de/de/firmen/' + url
            req = self.make_requests_from_url(fullUrl)
            req.meta['job'] = {'page': 1}
            req.meta['job']['nameInUrl'] = url
            req.meta['rule'] = -1
            yield req

    def parse_group(self, response):
        l = WlwLoader(item=WlwItem(), response=response)
        l.add_xpath('firmaId', '(.//*[@data-company-id]/@data-company-id)[1]')
        #  The given field_name can be None, in which case values for multiple
        #  fields may be added
        l.add_value(None, self.responseMetaDict(response))
        vcard = l.nested_css('div.profile-vcard')
        nameAddr = vcard.nested_css('div.vcard-details')
        nameAddr.add_xpath('name', 'h1//text()')
        nameAddr.add_xpath('full_addr', 'p//text()')
        svgSelector = vcard.nested_xpath('.//svg').selector
        vcard.add_value('site', svgSelector)
        vcard.add_value('email', svgSelector)
        vcard.add_value('phone', svgSelector)

        angebotSel = l.nested_xpath(
            '//div[@id="products-content"]//article').selector
        vcard.add_value('angebots', angebotSel)

        facts = l.nested_xpath('.//div[@id="data-and-facts-content"]/article')
        l.add_value('delivery', facts.selector)
        l.add_value('facts', facts.selector)
        l.add_value('certificates', facts.selector)

        container = l.load_item()

        # inspect_response(response, self)
        return container

    def responseMetaDict(self, response):
        return dict(query=response.meta['job_dat']['initial_term'],
                    category=response.meta['job_dat']['category'],
                    total_firms=response.meta['job_dat']['total'],
                    page=response.meta['job_dat']['page'],
                    totalOnPage=response.meta['job_dat']['linksGot'],
                    queryCat=response.meta['job_dat']['initial_term'] + '/' + \
                             response.meta['job_dat']['category'])
    # TODO: how to combine multivalue in exporter

    def _requests_to_follow(self, response):
        if not isinstance(response, HtmlResponse):
            return
        seen = set()
        # added functionality - check if rule deactivated
        switchOff = response.meta.get('switchedOffRule', -1)
        for n, rule in enumerate(self._rules):
            if n != switchOff:
                links = [lnk for lnk in rule.link_extractor.extract_links(response)
                         if lnk not in seen]
                if links and rule.process_links:
                    links = rule.process_links(links)
                # added functionality - set amount of links got in the response
                linksGot = len(links)
                response.meta['job']['linksGot'] = linksGot
                # added functionality - if no "next page links" found, means
                # that response's page is the last one
                respRule = response.meta.get('rule', -32)
                if (n == 0) and (respRule in [-1, 0]):
                    if not links:
                        name = response.meta['job']['nameInUrl']
                        pg = response.meta['job']['page']
                        self.jobState.thisIsTheLastPage(name, pg)

                for link in links:
                    seen.add(link)
                    r = self._build_request(n, link)
                    yield rule.process_request(r)
