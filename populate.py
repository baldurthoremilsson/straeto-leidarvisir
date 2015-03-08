#!/usr/bin/env python

import os
import psycopg2
from lxml.etree import parse


FILEPATHS = {
    'dagar': 'V2015B/dagar.xml',
    'leidir': 'V2015B/leidir.xml',
    'stodvar': 'V2015B/stodvar.xml',
    'ferlar': 'V2015B/ferlar.xml'
}

def main():
    conn = connect()
    with conn.cursor() as curs:
        dagar(FILEPATHS['dagar'], curs)
        leidir(FILEPATHS['leidir'], curs)
        stodvar(FILEPATHS['stodvar'], curs)
        ferlar(FILEPATHS['ferlar'], curs)
    conn.commit()


def connect():
    return psycopg2.connect(
        host=os.environ['STRAETO_HOST'],
        port=os.environ['STRAETO_PORT'],
        database=os.environ['STRAETO_DATABASE'],
        user=os.environ['STRAETO_DATABASE'],
        password=os.environ['STRAETO_PASSWORD']
    )

def dagar(filepath, curs):
    print filepath
    tree = parse(filepath)
    dagar = tree.getroot()
    for dagur in dagar:
        dag = dagur.attrib['dag']
        variant = dagur.attrib['variant']
        curs.execute('INSERT INTO dagar(dag, variant) VALUES(%s, %s)', (dag, variant))

def leidir(filepath, curs):
    print filepath
    tree = parse(filepath)
    leidir = tree.getroot()
    for leid in leidir:
        lid = leid.attrib['lid']
        num = leid.attrib['num']
        _leid = leid.attrib['leid']
        curs.execute('INSERT INTO leidir(lid, num, leid) VALUES(%s, %s, %s)', (lid, num, _leid))

def stodvar(filepath, curs):
    print filepath
    tree = parse(filepath)
    stodvar = tree.getroot()
    for stod in stodvar:
        id = stod.attrib['id']
        lon = stod.attrib['lon']
        lat = stod.attrib['lat']
        nafn = stod.attrib['nafn']
        curs.execute('INSERT INTO stodvar(stod, lon, lat, nafn) VALUES(%s, %s, %s, %s)', (id, lon, lat, nafn))

def ferlar(filepath, curs):
    print filepath
    tree = parse(filepath)
    ferlar = tree.getroot()
    ferd = EmptyFerd()
    force_store = False
    count = 0
    for ferill in ferlar:
        for variant in ferill:
            variants = variant.attrib['var'].split(',')
            force_store = True
            for stop in variant:
                count += 1
                if count % 1000 == 0:
                    print count

                stod = stop.attrib['stod']
                lid = stop.attrib['lid']
                timi = stop.attrib['timi']
                stnum = stop.attrib['stnum']

                if stnum == '1' or force_store:
                    ferd.store(curs)
                    ferd = Ferd(lid, variants)
                    force_store = False

                ferd.add_stop(stod, timi, stnum)
            ferd.store(curs)
            ferd = EmptyFerd()
    print 'stops', count


class Ferd(object):
    def __init__(self, lid, variants):
        self.lid = lid
        self.variants = variants
        self.stops = []

    def add_stop(self, stod, timi, stnum):
        self.stops.append({
            'stod': stod,
            'timi': timi,
            'stnum': stnum
        })

    def store(self, curs):
        ferd_start = self.stops[0]['timi']
        ferd_stop = self.stops[-1]['timi']

        for variant in self.variants:
            ferd = self.create_ferd(curs, self.lid, variant, ferd_start, ferd_stop)
            for stop in self.stops:
                self.create_stop(curs, ferd, stop['stod'], stop['timi'], stop['stnum'])

    def create_ferd(self, curs, lid, variant, start, stop):
        curs.execute('INSERT INTO ferdir(lid, variant, start, stop) VALUES(%s, %s, %s, %s) RETURNING ferdir.id',
                (lid, variant, start, stop))
        return curs.fetchone()[0]

    def create_stop(self, curs, ferd, stod, timi, stnum):
        curs.execute('INSERT INTO stops(ferd, stod, timi, stnum) VALUES(%s, %s, %s, %s)',
                (ferd, stod, timi, stnum))


class EmptyFerd(object):
    def store(self, curs):
        pass


if __name__ == '__main__':
    main()
