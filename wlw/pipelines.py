# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import logging
import re
from scrapy import logformatter
from scrapy.exceptions import DropItem
from .dbms import DBMS

logger = logging.getLogger(__name__)


class WlwPipeline(object):

    def process_item(self, item, spider):
        firmaId = item['firmaId']
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
        phoneRe = re.search(r'<a [^>]+>([^<]+)<\/a>', item['phone'])
        if phoneRe.lastindex == 1:
            item['phone'] = phoneRe.group(1).strip()
        else:
            logger.error('when parsing phone tag for {0}'.format(firmaId))
        return item


class DuplicatesPipeline(object):

    def __init__(self, crawler):
        # self.ids_seen = set()
        self.dbms = None
        self.ids_seen = None
        self.stats = crawler.stats

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler)
        # crawler.signals.connect(s.engine_stopped, signals.spider_closed)
        return s

    def process_item(self, item, spider):
        self.stats.inc_value('aa_proc_item_called')
        call = self.stats.get_value('aa_proc_item_called')
        itId = int(item['firmaId'])
        if itId in spider.ids_seen:
            self.stats.inc_value('Duplicated_items')
            logger.warning("Duplicate item found for: %s" % item['firmaId'])
            raise DropItem("Duplicate item found for: %s" % item['firmaId'])
        else:
            # self.ids_seen.add(itId)
            spider.ids_seen.add(itId)
            page = int(item['page'])
            tOnPage = int(item['totalOnPage'])
            qry = item['queryCat']
            self.dbms.addIdSeen(itId, page, tOnPage, qry)
            return item

    def open_spider(self, spider):
        full = os.path.dirname(os.path.abspath(__file__))
        dbPath = os.path.join(full, spider.name + '.db')
        self.dbms = DBMS(dbPath)
        spider.dbms = self.dbms
        spider.ids_seen = self.dbms.loadIdsSeen()

    def close_spider(self, spider):
        self.dbms.terminateDbms()
        logger.warning('base closed (close_spider)')

    # def spider_closed(self, spider):
    #     spider.dbms.terminateDbms()

    # def __del__(self):
    #     self.dbms.terminateDbms()
    # TODO study deferred

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
