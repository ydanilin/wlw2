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

    def loadJobState(self):
        output = self.cur.execute("SELECT name_in_url FROM job_state")
        return output.fetchall()

    def bulkCommit(self):
        self.dumpCount += 1
        if self.dumpCount == self.threshold:
            self.conn.commit()
            self.dumpCount = 0

    # ----------- terminate
    def terminateDbms(self):
        self.conn.commit()
        self.conn.close()
