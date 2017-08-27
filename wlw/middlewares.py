# -*- coding: utf-8 -*-
import logging
import re
from scrapy import signals
from scrapy.http import Request
from .items import WlwItem


logger = logging.getLogger(__name__)

class WlwSpiderMiddleware(object):
    def __init__(self, crawler):
        self.spider = None
        self.stats = crawler.stats
        self.jobState = None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        rule = response.meta.get('rule')
        if rule:  # bypass if debugging page (scrapy parse)
            nm = response.meta['job']['nameInUrl']
            if rule == -1:
                total = self.extractAmount(response)
                self.jobState.storeItemsReported(nm, total)
            seen = self.jobState.ifPageSeen(nm, response.meta['job']['page'])
            if seen:
                response.meta['switchedOffRule'] = 1
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            if isinstance(i, Request):
                i.meta['job'].update(response.meta['job'])
                willRequestByRule = i.meta.get('rule')
                if willRequestByRule == 1:
                    firmaId = i.meta['job']['firmaId']
                    bi = WlwItem(dict(firmaId=firmaId,
                                      nameInUrl=i.meta['job']['nameInUrl'],
                                      page=i.meta['job']['page'],
                                      linksGot=i.meta['job']['linksGot'],
                                      isDuplicate=0)
                                 )
                    if self.jobState.ifItemExists(firmaId):
                        bi['isDuplicate'] = 1
                        i.meta['discard'] = 1
                        yield bi
                    else:
                        i.meta['item'] = bi
                elif willRequestByRule == 0:
                    pg = i.meta['job']['page']
                    pg += 1
                    i.meta['job']['page'] = pg
                if not i.meta.get('discard'):
                    yield i
            else:
                yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.
        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.
        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        self.spider = spider
        self.jobState = spider.jobState

    def assignPage(self, spawnedByRule, willRequestByRule, resp, req):
        # if (not spawnedByRule) and (willRequestByRule == 0):
        #     req.meta['job']['page'] = 1
        # if spawnedByRule in [0, 2]:
        if spawnedByRule in [None, 2]:
            if willRequestByRule == 1:
                pg = resp.meta['job']['page']
                req.meta['job']['page'] = pg
            if willRequestByRule == 2:
                pg = resp.meta['job']['page']
                req.meta['job']['page'] = pg + 1

    def logPacket(self, packet, spider, supress_scraped=False):
        nameInUrl = packet.meta['job']['nameInUrl']
        page = packet.meta['job']['page']
        record = self.stats.get_value(nameInUrl)
        pg = record['pages'].get(page, 0) + 1
        record['pages'][page] = pg
        # if not supress_scraped:
        #     scr = record['scraped'] + 1
        # else:
        #     scr = record['scraped']
        # record['scraped'] = scr
        self.stats.set_value(nameInUrl, record)

        if not supress_scraped:
            # spider.dbms.updateScraped(nameInUrl, scr)

            if pg == packet.meta['job']['linksGot']:
                spider.dbms.addPageSeen(nameInUrl, page)

            # if scr == packet.meta['job']['total']:
            #     # signal when all firms for sysnonym are fetched
            #     msg = ('For category %(c)s'
            #            ' all firms fetched (%(a)d).')
            #     query = packet.meta['job']['initial_term']
            #     classif = packet.meta['job']['category']
            #     log_args = {'c': query + '/' + classif,
            #                 'a': packet.meta['job']['total']}
            #     logger.info(msg, log_args)

    def openCategory(self, nameInUrl, request, spider):
        lastPage = -1
        total = 0
        catRecord = spider.dbms.getCategory(nameInUrl)
        if catRecord:
            category = catRecord['caption']
            lastPage = catRecord['last_page']
            total = catRecord['total']
        else:  # create new category in db
            txt = request.meta.get('link_text', '')
            catDetails = re.split(r'(\d+) Anbieter', txt)
            if len(catDetails) == 3:
                category, total, dummy = catDetails
                total = int(total)
                spider.dbms.addCategory(nameInUrl, category, total)
            else:
                category = txt
                # i.meta['job']['discard'] = True
                msg = ('cannot parse name & amounts for category: {0}.'
                       ' Not recorded.')
                logger.error(msg.format(txt))
        return category, lastPage, total

    def extractAmount(self, response):
        txt = response.xpath('//h1[@class="lead"]//text()').extract_first()
        grp = re.search(r'[\d\.]+', txt)
        if grp:
            amt = int(grp.group().replace('.', ''))
        else:
            amt = 0
        return amt
