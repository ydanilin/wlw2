# -*- coding: utf-8 -*-
import logging
import re
from scrapy import signals
from scrapy.http import Request


logger = logging.getLogger(__name__)

class WlwSpiderMiddleware(object):
    def __init__(self, stats):
        self.stats = stats

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler.stats)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        rule = response.meta.get('rule', 77)
        # patch block begins
        if rule == 77:
            part = response.url.rsplit('?', 1)[0]
            nameInUrl = part.rsplit('/', 1)[1]
            response.meta['job_dat']['nameInUrl'] = nameInUrl
            response.meta['job_dat']['page'] = 1
        # patch block ends
        if rule == 1:
            # means one firm already processed:
            self.logPacket(response, spider)
        # elif rule in [0, 2]:
        elif rule in [77, 0, 2]:  # patch block. assume cat already in db
            pageSeen = spider.dbms.getPageSeen(
                response.meta['job_dat']['nameInUrl'])
            if response.meta['job_dat']['page'] in pageSeen:
                response.meta['switchedOffRule'] = 1
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            if isinstance(i, Request):
                # i.meta['job_dat'] = response.meta['job_dat'].copy()
                # ahuj1 = response.meta.get('job_dat')
                # ahuj2 = i.meta.get('job_dat')
                i.meta['job_dat'].update(response.meta['job_dat'])

                spawnedByRule = response.meta.get('rule')
                willRequestByRule = i.meta.get('rule')

                self.assignPage(spawnedByRule, willRequestByRule, response, i)

                # if (not spawnedByRule) and (willRequestByRule == 0):
                if not spawnedByRule:
                    nameInUrl = i.meta['job_dat']['nameInUrl']
                    category, lastPage, total = self.openCategory(nameInUrl, i,
                                                                 spider)
                    pageSeen = spider.dbms.getPageSeen(nameInUrl)
                    if len(pageSeen) == lastPage:
                        i.meta['job_dat']['discard'] = True
                    else:
                        dic = dict(total=total, pages={}, caption=category)
                        self.stats.set_value(nameInUrl, dic)
                    i.meta['job_dat']['category'] = category
                    i.meta['job_dat']['total'] = int(total)

                # if (spawnedByRule in [0, 2]) and (willRequestByRule == 1):
                if (spawnedByRule in [None, 2]) and (willRequestByRule == 1):
                    # pageSeen = spider.dbms.getPageSeen(nameInUrl)
                    # if i.meta['job_dat']['page'] in pageSeen:
                    #     i.meta['job_dat']['discard'] = True
                    fid = int(i.meta['firmaId'])
                    if fid in spider.ids_seen:
                        i.meta['job_dat']['discard'] = True
                        # requests passed the filter logged in spider_input
                        self.logPacket(i, spider, supress_scraped=True)
                        idd = i.meta['firmaId']
                        n = i.meta['job_dat']['nameInUrl']
                        p = i.meta['job_dat']['page']
                        l = i.meta['job_dat']['linksGot']
                        msg = 'Duplicate req for {0}, page {1} w/links {2}, cat: {3}'
                        logger.warning(msg.format(idd, p, l, n))
                        self.stats.inc_value('Duplicated_requests')

                # final decision
                discard = i.meta.get('job_dat', {}).get('discard')
                if not discard:
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

    def assignPage(self, spawnedByRule, willRequestByRule, resp, req):
        # if (not spawnedByRule) and (willRequestByRule == 0):
        #     req.meta['job_dat']['page'] = 1
        # if spawnedByRule in [0, 2]:
        if spawnedByRule in [None, 2]:
            if willRequestByRule == 1:
                pg = resp.meta['job_dat']['page']
                req.meta['job_dat']['page'] = pg
            if willRequestByRule == 2:
                pg = resp.meta['job_dat']['page']
                req.meta['job_dat']['page'] = pg + 1

    def logPacket(self, packet, spider, supress_scraped=False):
        nameInUrl = packet.meta['job_dat']['nameInUrl']
        page = packet.meta['job_dat']['page']
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

            if pg == packet.meta['job_dat']['linksGot']:
                spider.dbms.addPageSeen(nameInUrl, page)

            # if scr == packet.meta['job_dat']['total']:
            #     # signal when all firms for sysnonym are fetched
            #     msg = ('For category %(c)s'
            #            ' all firms fetched (%(a)d).')
            #     query = packet.meta['job_dat']['initial_term']
            #     classif = packet.meta['job_dat']['category']
            #     log_args = {'c': query + '/' + classif,
            #                 'a': packet.meta['job_dat']['total']}
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
                # i.meta['job_dat']['discard'] = True
                msg = ('cannot parse name & amounts for category: {0}.'
                       ' Not recorded.')
                logger.error(msg.format(txt))
        return category, lastPage, total
