# coding=utf-8
import os
import logging
from scrapy import signals
from .dbms import DBMS

logger = logging.getLogger(__name__)


class JobState(object):
    def __init__(self, crawler):
        # self.spider = crawler.spider
        self.stats = crawler.stats
        self.dbms = None
        """
        {nameInUrl: {pageSeen: [], pages: {1:4, 2:46, ...}, last: 2, total: 50},
         nameInUrl: {pageSeen: [], pages: {5:8, 3:77, ...}, last: 3, total: 88},
         .....
        }
        """
        self.jobState = {}
        self.ids_seen = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        spider.jobState = self
        self.dbms = self.openDBMS(spider)
        self.jobState = self.loadJobState()
        self.ids_seen = self.loadIdsSeen()

    def spider_closed(self, spider):
        # print(self.jobState)
        for nm, content in self.jobState.items():
            total = content['total']
            if total != 0:
                # print('store total ', total)
                self.storeItemsReported(nm, total)
            ps = content['pageSeen']
            # if ps:
            #     self
        self.closeDBMS(self.dbms)

# *********** service functions ***********************
    def getStartUrls(self):
        return [i for i in self.jobState.keys()]

    def ifPageSeen(self, nameInUrl, page):
        if page in self.jobState[nameInUrl]['pageSeen']:
            return True
        else:
            return False

    def ifItemExists(self, firmaId):
        if firmaId in self.ids_seen:
            return True
        else:
            return False

    def storeItemsReported(self, nameInUrl, amount):
        self.dbms.storeItemsReported(nameInUrl, amount)

    def registerInTotals(self, nameInUrl):
        actual = self.jobState[nameInUrl]['total']
        actual += 1
        self.jobState[nameInUrl]['total'] = actual
        # logger.warning(actual)

    def thisIsTheLastPage(self, nameInUrl, page):
        self.dbms.updateLastPage(nameInUrl, page)
        self.jobState[nameInUrl]['last'] = page

    def addItemToSeen(self, firmaId):
        self.ids_seen.add(firmaId)

    def increaseOnPageCounter(self, nameInUrl, page):
        """returns increased firms on page counter"""
        entry = self.jobState[nameInUrl]['pages']
        counter = entry.get(page, 0)
        counter += 1
        entry[page] = counter
        return counter

    def loadJobState(self):
        """returns the whole jobState structure"""
        rows = self.dbms.loadJobState()
        output = {}
        for row in rows:
            name = row['name_in_url']
            lsp = row['last_page']
            pgs = row['page_seen']
            stor = row['items_stored']
            if pgs:
                pglist = list(map(lambda x: int(x), pgs.split(',')))
            else:
                pglist = []
            if len(pglist) != lsp:
                output[name] = {'pageSeen': pglist,
                                'pages': {},
                                'last': lsp,
                                'total': stor}
        return output

    def loadIdsSeen(self):
        sett = self.dbms.loadIdsSeen()
        if sett:
            output = [x['id_'] for x in sett]
        else:
            output = []
        return set(output)

    def addPageSeen(self, nameInUrl, page):
        entry = self.jobState[nameInUrl]['pageSeen']
        entry.append(page)
        entry.sort()
        self.jobState[nameInUrl]['pageSeen'] = entry
        output = ','.join(map(lambda x: str(x), entry))
        self.dbms.addPageSeen(nameInUrl, output)
        msg = 'Page {0} for category "{1}" completed'.format(page, nameInUrl)
        logger.info(msg)
        # log and commit when all pagess
        if len(entry) == self.jobState[nameInUrl]['last']:
            self.dbms.updateItemsStored(nameInUrl,
                                        self.jobState[nameInUrl]['total'])
            logger.info('Category "{0}" completed'.format(nameInUrl))

    def storeItem(self, item):
        self.dbms.storeItem(item)

    def storeCategories(self, item):
        firmaId = item['firmaId']
        a = item.get('angebots', [])
        for angebot in a:
            self.dbms.storeCategory(angebot)
            self.dbms.storeAngebot(firmaId, angebot)

    def openDBMS(self, spider):
        full = os.path.dirname(os.path.abspath(__file__))
        dbPath = os.path.join(full, spider.name + '.db')
        return DBMS(dbPath)

    def closeDBMS(self, dbms):
        dbms.terminateDbms()
