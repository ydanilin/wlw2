# -*- coding: utf-8 -*-
import sqlite3


class DBMS:
    def __init__(self, dbName):
        # Database connection
        self.conn = sqlite3.connect(dbName)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cur = self.conn.cursor()
        self.cur.row_factory = sqlite3.Row
        self.dumpCount = 0  # commit counter
        self.threshold = 20  # threshold when to commit
        self.sqlLoadJobState = """
                               SELECT name_in_url,
                                      last_page,
                                      page_seen
                               FROM job_state
                               """

        self.sqlUpdateLastPage = """UPDATE job_state SET last_page = :pg
                                    WHERE name_in_url = :name"""
        self.sqlAddPageSeen = """UPDATE job_state SET page_seen = :pages
                                         WHERE name_in_url = :nameInUrl"""
        self.sqlLoadIdsSeen = """SELECT id_ FROM firmas"""

    def loadJobState(self):
        return self.cur.execute(self.sqlLoadJobState).fetchall()

    def loadIdsSeen(self):
        return self.cur.execute(self.sqlLoadIdsSeen).fetchall()

    def updateLastPage(self, nameInUrl, page):
        self.conn.execute(self.sqlUpdateLastPage, dict(name=nameInUrl,
                                                       pg=int(page))
                          )
        self.conn.commit()

    def addPageSeen(self, nameInUrl, pageSeenStr):
        self.conn.execute(self.sqlAddPageSeen, dict(nameInUrl=nameInUrl,
                                                    pages=pageSeenStr))
        self.conn.commit()

    def bulkCommit(self):
        self.dumpCount += 1
        if self.dumpCount == self.threshold:
            self.conn.commit()
            self.dumpCount = 0

    # ----------- terminate
    def terminateDbms(self):
        self.conn.commit()
        self.conn.close()
