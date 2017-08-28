# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import re
from datetime import date
from scrapy import signals, logformatter
from scrapy.exceptions import DropItem

logger = logging.getLogger(__name__)


class WlwPipeline(object):

    def process_item(self, item, spider):
        item['source'] = 'wlw.de'
        item['timestamp'] = date.today().strftime("%d/%m/%Y")
        firmaId = item['firmaId']
        # process address
        addrSplitted = re.split(r',\s+', item['full_addr'])
        if len(addrSplitted) == 2:
            stHaus, indStadt = addrSplitted
            indSplitted = re.split(r'(DE-\d+)\s?', indStadt)
            if len(indSplitted) == 3:
                dummy, index, stadt = indSplitted
                item['zip'] = index
                item['city'] = stadt
            else:
                logger.error(
                    'when re.split index, city for {0}'.format(firmaId))
            streetSplitted = stHaus.rsplit(' ', 1)
            if len(streetSplitted) == 2:
                street, house = streetSplitted
                item['street'] = street
                item['building'] = house
            else:
                logger.error('when re.split street, for {0}'.format(firmaId))
        else:
            logger.error('when re.split full address for {0}'.format(firmaId))
        # process item's phone
        ph = item.get('phone')
        if ph:
            item['phone'] = self.extractPhone(ph, firmaId)
        # process angebot's phone
        angebots = item.get('angebots', [])
        for angebot in angebots:
            ph = angebot.get('phone')
            li = angebot.get('listing_id', 'no listing')
            if ph:
                angebot['phone'] = self.extractPhone(ph, li)
        return item

    def extractPhone(self, phstr, firmaId):
        phoneRe = re.search(r'<a [^>]+>([^<]+)<\/a>', phstr)
        if phoneRe.lastindex == 1:
            return phoneRe.group(1).strip()
        else:
            logger.error('when parsing phone tag for {0}'.format(firmaId))
            return phstr


class DuplicatesPipeline(object):

    def __init__(self, crawler):
        self.stats = crawler.stats
        self.jobState = None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_item(self, item, spider):
        fm = item['firmaId']
        nm = item['nameInUrl']
        pg = item['page']
        self.stats.inc_value('Items:_total')
        # register item in pages seen
        firmasOnPage = self.jobState.increaseOnPageCounter(nm, pg)
        if firmasOnPage == item['linksGot']:
            self.jobState.addPageSeen(nm, pg)
        if item['isDuplicate'] or self.jobState.ifItemExists(fm):
            # handle duplicates here

            self.stats.inc_value('Items:_duplicated')
            logger.warning("Duplicate item found for: %s" % fm)
            raise DropItem("Duplicate item found for: %s" % fm)
        else:
            # register item in totals
            self.stats.inc_value('Items:_stored')
            self.jobState.registerInTotals(nm)
            self.jobState.addItemToSeen(fm)
            return item

    def spider_opened(self, spider):
        self.jobState = spider.jobState

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass
    # TODO study deferred


class DBMSPipeline(object):

    def __init__(self, crawler):
        self.stats = crawler.stats
        self.jobState = None

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_item(self, item, spider):
        i = dict(item)
        self.jobState.storeItem(i)
        self.jobState.storeCategories(i)

    def spider_opened(self, spider):
        self.jobState = spider.jobState


class PoliteLogFormatter(logformatter.LogFormatter):
    def dropped(self, item, exception, response, spider):
        return {
            'level': logging.DEBUG,
            'msg': logformatter.DROPPEDMSG,
            'args': {
                'exception': exception,
                'item': item,
            }
        }
