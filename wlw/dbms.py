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
                                      page_seen,
                                      items_stored
                               FROM job_state
                               """

        self.sqlUpdateLastPage = """UPDATE job_state SET last_page = :pg
                                    WHERE name_in_url = :name"""
        self.sqlAddPageSeen = """UPDATE job_state SET page_seen = :pages
                                         WHERE name_in_url = :nameInUrl"""
        self.sqlStoreItemsReported = """
                                     UPDATE job_state SET items_reported = :amt
                                            WHERE name_in_url = :nameInUrl"""
        self.sqlUpdateItemsStored = """
                                    UPDATE job_state SET items_stored = :amt
                                           WHERE name_in_url = :nameInUrl"""
        self.sqlLoadIdsSeen = """SELECT id_ FROM firmas"""
        self.sqlGetCategory = """SELECT name_in_url FROM categories
                                 WHERE id_ = ?"""
        self.sqlStoreItem = """
            INSERT INTO firmas (id_,
                                name,
                                site,
                                email,
                                phone,
                                full_addr,
                                street,
                                building,
                                zip,
                                city,
                                delivery,
                                facts,
                                certificates,
                                about,
                                key_people,
                                common_person,
                                timestamp,
                                source)
                        VALUES (:fid,
                                :name,
                                :site,
                                :email,
                                :phone,
                                :fa,
                                :street,
                                :building,
                                :zip,
                                :city,
                                :dlv,
                                :facts,
                                :certs,
                                :about,
                                :kp,
                                :comperson,
                                :ts,
                                :sr)
        """
        self.sqlStoreCategory = """INSERT INTO categories (id_,
                                                           name_in_url,
                                                           caption)
                                                   VALUES (:cid,
                                                           :nm,
                                                           :cp)
                                """
        self.sqlStoreAngebot = """INSERT INTO cat_per_firm (firma_id,
                                                            cat_id,
                                                            is_producer,
                                                            is_service,
                                                            is_distrib,
                                                            is_wholesaler,
                                                            offer_text,
                                                            contact_person,
                                                            phone,
                                                            email,
                                                            listing_id)
                                                    VALUES (:fid,
                                                            :cid,
                                                            :prod,
                                                            :srv,
                                                            :distr,
                                                            :whs,
                                                            :offer,
                                                            :cp,
                                                            :phone,
                                                            :email,
                                                            :lid)"""

    def loadJobState(self):
        return self.cur.execute(self.sqlLoadJobState).fetchall()

    def loadIdsSeen(self):
        return self.cur.execute(self.sqlLoadIdsSeen).fetchall()

    def updateLastPage(self, nameInUrl, page):
        self.conn.execute(self.sqlUpdateLastPage, dict(name=nameInUrl,
                                                       pg=int(page))
                          )
        self.conn.commit()

    def storeItemsReported(self, nameInUrl, amount):
        self.conn.execute(self.sqlStoreItemsReported, dict(nameInUrl=nameInUrl,
                                                           amt=amount))
        self.conn.commit()

    def updateItemsStored(self, nameInUrl, items):
        self.conn.execute(self.sqlUpdateItemsStored, dict(nameInUrl=nameInUrl,
                                                           amt=items))
        self.conn.commit()

    def addPageSeen(self, nameInUrl, pageSeenStr):
        self.conn.execute(self.sqlAddPageSeen, dict(nameInUrl=nameInUrl,
                                                    pages=pageSeenStr))
        self.conn.commit()

    def storeItem(self, it):
        output = dict(ts=it['timestamp'],
                      sr=it['source'],
                      fid=it['firmaId'],
                      name=it['name'],
                      site=it.get('site', ''),
                      email=it.get('email', ''),
                      phone=it.get('phone', ''),
                      street=it.get('street', ''),
                      building=it.get('building', ''),
                      zip=it.get('zip', ''),
                      city=it.get('city', ''),
                      fa=it.get('full_addr', ''),
                      dlv=it.get('delivery', ''),
                      facts=it.get('facts', ''),
                      certs=it.get('certificates', ''),
                      about=it.get('about', ''),
                      kp=it.get('key_people', ''),
                      comperson=it.get('common_person', '')
                      )
        self.conn.execute(self.sqlStoreItem, output)
        self.bulkCommit()

    def storeCategory(self, angebot):
        cid = angebot['cat_id']
        nm = angebot['nameinurl']
        cp = angebot['caption']
        tryCat = self.cur.execute(self.sqlGetCategory, (cid,)).fetchall()
        if not tryCat:
            self.conn.execute(self.sqlStoreCategory, dict(cid=cid,
                                                          nm=nm,
                                                          cp=cp))
            self.bulkCommit()

    def storeAngebot(self, firmaId, angebot):
        output = dict(fid=firmaId,
                      cid=angebot['cat_id'],
                      prod=angebot['is_producer'],
                      srv=angebot['is_service'],
                      distr=angebot['is_distrib'],
                      whs=angebot['is_wholesaler'],
                      offer=angebot.get('offer_text'),
                      cp=angebot.get('contact_person'),
                      phone=angebot.get('phone'),
                      email=angebot.get('email'),
                      lid=angebot['listing_id'])
        self.conn.execute(self.sqlStoreAngebot, output)
        self.bulkCommit()

    def bulkCommit(self):
        self.dumpCount += 1
        if self.dumpCount == self.threshold:
            self.conn.commit()
            self.dumpCount = 0

    # ----------- terminate
    def terminateDbms(self):
        self.conn.commit()
        self.conn.close()
