# -*- coding: utf-8 -*-
import logging
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse
from scrapy.shell import inspect_response
from ..items import WlwItem, WlwLoader, AngebotItem, AngebotLoader

logger = logging.getLogger(__name__)


class WlwBaseSpider(CrawlSpider):
    def __init__(self, *args, **kwargs):
        super(WlwBaseSpider, self).__init__(*args, *kwargs)
        self.jobState = None

    name = 'wlw_base'
    allowed_domains = ['wlw.de']
    start_urls = []

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
            page = int(grp.group(grp.lastindex))
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
        it = response.meta.get('item')
        if not it:  # bypass if debugging page (scrapy parse)
            it = WlwItem()
        l = WlwLoader(item=it, response=response)
        # 1. Name and address data
        vcard = l.nested_css('div.profile-vcard')
        nameAddr = vcard.nested_css('div.vcard-details')
        nameAddr.add_xpath('name', 'h1//text()')
        nameAddr.add_xpath('full_addr', 'p//text()')
        svgSelector = vcard.nested_xpath('.//svg').selector
        vcard.add_value('site', svgSelector)
        vcard.add_value('email', svgSelector)
        vcard.add_value('phone', svgSelector)
        # 2. Delivery, daten und fakten, zertificates
        facts = l.nested_xpath('.//div[@id="data-and-facts-content"]/article')
        l.add_value('delivery', facts.selector)
        l.add_value('facts', facts.selector)
        l.add_value('certificates', facts.selector)
        # inspect_response(response, self)
        # 3. Uber uns section
        uberUns = l.nested_xpath('.//section[@id="about-us"]//article')
        l.add_value('about', uberUns.selector)
        l.add_value('key_people', uberUns.selector)
        # 4. Ansprechpartner
        sprech = l.nested_xpath(
            './/section[@id="contacts"]//article[1]/div[2]')
        l.add_value('common_person', sprech.selector)
        # 5. Categories, bilatt
        angebotSel = l.nested_xpath(
            '//div[@id="products-content"]//article').selector
        angebots = []
        for angebot in self.get_angebots(angebotSel):
            angebots.append(dict(angebot))
        l.add_value('angebots', angebots)

        container = l.load_item()
        # inspect_response(response, self)
        return container

    # TODO: how to combine multivalue in exporter

    def get_angebots(self, selector):
        for angebot in selector:
            l = AngebotLoader(item=AngebotItem(), selector=selector)
            yield l.load_item()


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
