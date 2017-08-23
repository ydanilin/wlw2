# coding=utf-8
import os
from scrapy import signals
from .dbms import DBMS


class JobState(object):

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        full = os.path.dirname(os.path.abspath(__file__))
        dbPath = os.path.join(full, spider.name + '.db')
        self.dbms = DBMS(dbPath)
        spider.dbms = self.dbms

    def spider_closed(self, spider):
        pass