# coding=utf-8
import os
from scrapy import signals
from .dbms import DBMS


class JobState(object):
    def __init__(self, crawler):
        # self.spider = crawler.spider
        self.stats = crawler.stats
        self.dbms = None
        """
        {nameInUrl: {pageSeen: [], pages = {1:4, 2:46, ...}},
         nameInUrl: {pageSeen: [], pages = {5:18, 3:77, ...}},
         .....
        }
        """
        self.jobState = {}

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        spider.jobState = self
        self.dbms = self.openDBMS()
        self.jobState = self.loadJobState()

    def spider_closed(self, spider):
        self.closeDBMS(self.dbms)

# *********** service functions ***********************
    def getStartUrls(self):
        return [i for i in self.jobState.keys()]

    def ifPageSeen(self, nameInUrl, page):
        if page in self.jobState[nameInUrl]['pageSeen']:
            return True
        else:
            return False

    def thisIsTheLastPage(self, nameInUrl, page):
        self.dbms.updateLastPage(nameInUrl, page)

    def increaseOnPageCounter(self, nameInUrl, page):
        """returns increased firms on page counter"""
        entry = self.jobState[nameInUrl]['pages']
        counter = entry.get(page, 0)
        counter += 1
        entry[page] = counter
        return counter

    def addPageSeen(self, nameInUrl, page):
        entry = self.jobState[nameInUrl]['pageSeen']
        entry.append(page)
        entry.sort()
        self.jobState[nameInUrl]['pageSeen'] = entry
        output = ','.join(map(lambda x: str(x), entry))
        self.dbms.addPageSeen(nameInUrl, output)

    def loadJobState(self):
        """returns the whole jobState structure"""
        rows = self.dbms.loadJobState()
        output = {}
        for row in rows:
            name = row['name_in_url']
            lsp = row['last_page']
            pgs = row['page_seen']
            if pgs:
                pglist = list(map(lambda x: int(x), pgs.split(',')))
            else:
                pglist = []
            if len(pglist) != lsp:
                output[name] = {'pageSeen': pglist, 'pages': {}}
        return output

    def openDBMS(self):
        full = os.path.dirname(os.path.abspath(__file__))
        dbPath = os.path.join(full, self.spider.name + '.db')
        return DBMS(dbPath)

    def closeDBMS(self, dbms):
        dbms.terminateDbms()
