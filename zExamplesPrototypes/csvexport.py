# -*- coding: utf-8 -*-
import os
import csv
import sqlite3


class DBMS:
    def __init__(self, dbName):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        # Database connection
        self.conn = sqlite3.connect(dbName)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.cur = self.conn.cursor()
        self.cur.row_factory = dict_factory
        self.sqlTotalFirms = """
            -- amounts based on job_state categories, no duplicates
            SELECT name_in_url,
                   count(firmaId) AS total_firms
            FROM (
                    SELECT j.cat_id      AS cid,
                           c.name_in_url AS name_in_url,
                           f.id_         AS firmaId,
                           f.name        AS name
                    FROM
                    job_state j INNER JOIN categories c
                                    ON j.name_in_url = c.name_in_url
                                INNER JOIN cat_per_firm s
                                    ON c.id_ = s.cat_id
                                INNER JOIN firmas f
                                    ON s.firma_id = f.id_
                    GROUP BY f.name
                    ORDER BY j.cat_id
                 )
            GROUP BY name_in_url
            ORDER BY cid
        """
        self.sqlRecordset = """
            -- based on job_state categories, no duplicates
            SELECT f.source,
                   f.timestamp AS research_ts,
                   group_concat(c.caption, ";") AS category,
                   c.name_in_url,
                   f.id_ AS firmaId,
                   f.name AS name,
                   f.full_addr,
                   f.street,
                   f.building,
                   f.zip,
                   f.city,
                   f.phone,
                   f.email,
                   f.site,
                   f.delivery,
                   f.facts AS akquisition_info,
                   f.certificates,
                   f.about AS company_info,
                   f.key_people,
                   f.common_person AS main_contact
            FROM
            job_state j INNER JOIN categories c ON j.name_in_url = c.name_in_url
                        INNER JOIN cat_per_firm s ON c.id_ = s.cat_id
                        INNER JOIN firmas f ON s.firma_id = f.id_
            GROUP BY f.name
            ORDER BY j.cat_id
        """
        self.sqlAngebots = """
            SELECT f.name,
       group_concat(
           (c.caption
            || " ("
            || case when s.is_producer = 1 then 'producer ' else '' end
            || case when s.is_service = 1 then 'service ' else '' end
            || case when s.is_distrib = 1 then 'distrib ' else '' end
            || case when s.is_wholesaler = 1 then 'wholesaler' else '' end
            || ")"
            || case when typeof(s.offer_text) != 'null'
                    then ', Text:' || s.offer_text else '' end
            || case when typeof(s.contact_person) != 'null'
                    then ', Contact:' || s.contact_person else '' end
            || case when typeof(s.phone) != 'null'
                    then ', phone:' || s.phone else '' end
            || case when typeof(s.email) != 'null'
                    then ', email:' || s.email else '' end
             ),
        "; ") AS det
FROM
firmas f INNER JOIN cat_per_firm s ON f.id_ = s.firma_id
         INNER JOIN categories c ON s.cat_id = c.id_
WHERE f.id_ = ?"""

    def countTotals(self):
        out = self.cur.execute(self.sqlTotalFirms).fetchall()
        newdata = {}
        for entry in out:
            name = entry.pop('name_in_url')  # remove and return the name field
            newdata[name] = entry
        return newdata

    def recordset(self):
        return self.cur.execute(self.sqlRecordset).fetchall()
        # return self.cur.execute(self.sqlRecordset).fetchmany(100)

    def composeAngebots(self, firmaId):
        return self.cur.execute(self.sqlAngebots, (firmaId,)).fetchone()

    def terminateDbms(self):
        self.conn.close()


if __name__ == '__main__':
    # discover database file placement
    rootDir = 'wlw2'
    curDir = os.path.dirname(os.path.abspath(__file__))
    dir = ''
    while dir != rootDir:
        curDir, dir = os.path.split(curDir)
    projectPath = os.path.join(curDir, dir)
    dbPath = os.path.join(projectPath, 'wlw', 'wlw_base.db')
    dbms = DBMS(dbPath)
    # init csv
    fn = ['source',
          'research_ts',
          'category',
          'total_firms',
          'firmaId',
          'name',
          'full_addr',
          'street',
          'building',
          'zip',
          'city',
          'phone',
          'email',
          'site',
          'delivery',
          'akquisition_info',
          'certificates',
          'company_info',
          'key_people',
          'main_contact',
          'full_search'
          ]
    f = open('wlw_aug.csv', 'w', newline='', encoding='utf-8')
    wrt = csv.DictWriter(f, fieldnames=fn, extrasaction='ignore')
    wrt.writeheader()
    # process
    print('Doing queries...')
    totals = dbms.countTotals()
    recordset = dbms.recordset()
    max_ = len(recordset)
    acc = 0
    print('Starting the process...')
    for i, rec in enumerate(recordset):
        rec['total_firms'] = totals[rec['name_in_url']]['total_firms']
        fid = rec['firmaId']
        ang = dbms.composeAngebots(fid)
        rec['full_search'] = ang['det']
        wrt.writerow(rec)
        acc += 1
        if acc >= 20:
            f.flush()
            print('processed {0} of {1} records'.format(i+1, max_))
            acc = 0
    print('Completed.')
    dbms.terminateDbms()
    f.close()





















