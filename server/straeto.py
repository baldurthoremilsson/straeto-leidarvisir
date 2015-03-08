#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import flask
from flask import Flask, render_template, request
from db import Provider
from collections import defaultdict
from datetime import datetime

DBPARAMS = {
    'host': os.environ['STRAETO_HOST'],
    'port': os.environ['STRAETO_PORT'],
    'user': os.environ['STRAETO_USER'],
    'password': os.environ['STRAETO_PASSWORD'],
    'database': os.environ['STRAETO_DATABASE']
}

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/rest/1/leidir')
def rest_1_leidir():
    provider = Provider(**DBPARAMS)
    leidir = defaultdict(list)
    for lid, num, leid in provider.leidir():
        leidir[num].append({
            'lid': lid,
            'nafn': leid
        })
    return flask.jsonify(leidir)

@app.route('/rest/1/stodvar')
def rest_1_stodvar():
    provider = Provider(**DBPARAMS)
    stodvar = {}
    for stod, lon, lat, nafn in provider.stodvar():
        stodvar[stod] = {
            'lon': lon,
            'lat': lat,
            'nafn': nafn
        }
    return flask.jsonify(stodvar)

@app.route('/rest/1/dagar')
def rest_1_dagar():
    provider = Provider(**DBPARAMS)
    dagar = {}
    for dag, variant in provider.dagar():
        dagar[dag.strftime('%Y-%m-%d')] = variant
    return flask.jsonify(dagar)

@app.route('/rest/1/stod')
def rest_1_stod():
    def comparator(a, b):
        aa = a['date'] + a['time']
        bb = b['date'] + b['time']
        if aa < bb:
            return -1
        return 1

    provider = Provider(**DBPARAMS)
    ids = request.args.getlist('id')
    date = request.args.get('datetime', None)
    stop = request.args.get('stop', 10)

    if date == None:
        date = datetime.now()
    else:
        date = datetime.strptime(date, '%Y-%m-%d %H:%M')

    stops = defaultdict(list)
    for stod, lid, dag, timi in provider.stod(ids, date, stop):
        stops[stod].append({
            'lid': lid,
            'date': dag.strftime('%Y-%m-%d'),
            'time': timi.strftime('%H:%M')
        })

    for stod, stoplist in stops.iteritems():
        stoplist.sort(cmp=comparator)
    return flask.jsonify(stops)

@app.route('/rest/1/leid')
def rest_1_leid():
    provider = Provider(**DBPARAMS)
    lids = request.args.getlist('lid')
    date = request.args.get('datetime', None)
    ferdir = request.args.get('ferdir', 4)

    if date == None:
        date = datetime.now()
    else:
        date = datetime.strptime(date, '%Y-%m-%d %H:%M')

    results = defaultdict(lambda: defaultdict(list))
    for lid, ferd_id, ferdir_start, stod, timi, dag, stnum in provider.leid(lids, date, ferdir):
        key = datetime(dag.year, dag.month, dag.day, ferdir_start.hour, ferdir_start.minute).strftime('%Y-%m-%d %H:%M')
        results[lid][key].append((stod,
            timi.strftime('%H-%M'), dag.strftime('%Y-%m-%d'), stnum))

    data = defaultdict(list)
    for lid, ferdir_dict in results.iteritems():
        keys = ferdir_dict.keys()
        keys.sort()
        for key in keys:
            data[lid].append(ferdir_dict[key])

    return flask.jsonify(data)


if __name__ == '__main__':
    app.debug = True
    app.run()
