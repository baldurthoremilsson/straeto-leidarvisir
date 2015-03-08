# -*- coding: utf-8 -*-

import psycopg2


class Provider(object):
    def __init__(self, **params):
        self.connection = psycopg2.connect(**params)

    def leidir(self):
        with self.connection.cursor() as curs:
            curs.execute('SELECT lid, num, leid FROM leidir')
            for row in curs:
                yield row

    def stodvar(self):
        with self.connection.cursor() as curs:
            curs.execute('SELECT stod, lon, lat, nafn FROM stodvar')
            for row in curs:
                yield row

    def dagar(self):
        with self.connection.cursor() as curs:
            curs.execute('SELECT dag, variant FROM dagar')
            for row in curs:
                yield row

    def _lids_for_stod(self, id):
        with self.connection.cursor() as curs:
            curs.execute('''
                SELECT DISTINCT ferdir.lid
                FROM stops
                LEFT JOIN ferdir ON stops.ferd = ferdir.id
                WHERE stops.stod = (%s)
            ''', (id,))
            for lid, in curs:
                yield lid

    def stod(self, ids, datetime, stops):
        date = datetime.date()
        time = datetime.time()
        queries = []
        values = []
        for id in ids:
            lids = self._lids_for_stod(id)
            for lid in lids:
                queries.append('''
                (
                    SELECT stops.stod, ferdir.lid, dagar.dag, stops.timi
                    FROM stops
                    LEFT JOIN ferdir ON stops.ferd = ferdir.id
                    LEFT JOIN dagar ON ferdir.variant = dagar.variant
                    WHERE stops.stod = %s
                    AND ferdir.lid = %s
                    AND (dagar.dag = %s AND stops.timi >= %s
                         OR dagar.dag > %s)
                    ORDER BY dagar.dag, stops.timi
                    LIMIT %s
                )
                ''')
                values.extend([id, lid, date, time, date, stops])
        query = ' UNION '.join(queries)
        with self.connection.cursor() as curs:
            curs.execute(query, values)
            for row in curs:
                yield row

    def _ferdir_for_lid(self, lid, datetime, count):
        date = datetime.date()
        time = datetime.time()

        with self.connection.cursor() as curs:
            curs.execute('''
                SELECT dagar.dag, ferdir.id
                FROM ferdir
                LEFT JOIN dagar ON ferdir.variant = dagar.variant
                WHERE ferdir.lid = %s
                AND (dagar.dag = %s AND ferdir.stop >= %s
                     OR dagar.dag > %s)
                ORDER BY dagar.dag, ferdir.start
                LIMIT %s
            ''', (lid, date, time, date, count))
            for row in curs:
                yield row

    def leid(self, lids, datetime, count):
        date = datetime.date()
        time = datetime.time()
        queries = []
        values = []

        for lid in lids:
            for ferd_date, ferd_id in self._ferdir_for_lid(lid, datetime, count):
                queries.append('''
                (
                    SELECT
                      %s as lid,
                      stops.ferd as ferd_id,
                      ferdir.start as ferd_start,
                      stops.stod as stod,
                      stops.timi as timi,
                      %s as dag,
                      stops.stnum as stnum
                    FROM stops
                    LEFT JOIN ferdir ON stops.ferd = ferdir.id
                    WHERE stops.ferd = %s
                    ORDER BY stops.stnum
                )
                ''')
                values.extend([lid, ferd_date, ferd_id])

        query = ' UNION '.join(queries)
        query = 'SELECT lid, ferd_id, ferd_start, stod, timi, dag, stnum FROM ('\
                + query +\
                ') AS tt ORDER BY lid, dag, ferd_start, stnum'
        with self.connection.cursor() as curs:
            curs.execute(query, values)
            for row in curs:
                yield row

