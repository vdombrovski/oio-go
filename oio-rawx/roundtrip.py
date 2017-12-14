#!/usr/bin/env python

import time
import hashlib
import requests

bulk = "0123" * 256
HDR_PREFIX = 'X-oio-'
reqid = "tnx0123456789ABCDEF"

def generate_chunkid(seed):
    h = hashlib.sha256()
    h.update(str(time.time()))
    h.update("+")
    h.update(str(seed))
    return h.hexdigest()

def dump_reply(op, rep):
    print op, repr(rep.status_code), repr(rep.headers), "body.length", len(rep.text)

def stat_get(session):
    headers = { HDR_PREFIX + "reqid": reqid }
    url = "http://127.0.0.1:5999/stat"
    reply = session.get(url, headers=headers)
    dump_reply("stat/get", reply)

def stat_head(session):
    headers = { HDR_PREFIX + "reqid": reqid }
    url = "http://127.0.0.1:5999/stat"
    reply = session.head(url, headers=headers)
    dump_reply("stat/head", reply)

def delete(session, chunkid):
    headers = { HDR_PREFIX + "reqid": reqid }
    url = "http://127.0.0.1:5999/" + chunkid
    reply = session.delete(url, headers=headers)
    dump_reply("delete", reply)

def head(session, chunkid):
    headers = { HDR_PREFIX + "reqid": reqid }
    url = "http://127.0.0.1:5999/" + chunkid
    reply = session.head(url, headers=headers)
    dump_reply("head", reply)

def get(session, chunkid):
    headers = { HDR_PREFIX + "reqid": reqid,
                "Range": "bytes=0-65" }
    url = "http://127.0.0.1:5999/" + chunkid
    reply = session.get(url, headers=headers)
    dump_reply("get", reply)

def put(session, chunkid):
    headers = { HDR_PREFIX + "reqid": reqid,
                HDR_PREFIX + 'Alias': 'plop',
                HDR_PREFIX + 'Chunk-Meta-Content-Mime-Type': 'octet/stream',
                HDR_PREFIX + 'Chunk-Meta-Content-Storage-Policy': 'SINGLE',
                HDR_PREFIX + 'Chunk-Meta-Content-Chunk-Method': 'plain/nb=3',
                HDR_PREFIX + 'Chunk-Meta-Chunk-Id': chunkid,
                HDR_PREFIX + 'Chunk-Meta-Chunk-Size': str(1024),
                HDR_PREFIX + 'Chunk-Meta-Chunk-Pos': str(0) }
    url = "http://127.0.0.1:5999/" + chunkid
    reply = session.put(url, data=bulk, headers=headers)
    dump_reply("put", reply)

session = requests.Session()
for i in range(3):
    chunkid = generate_chunkid(i)
    for j in range(3):
        put(session, chunkid)
        head(session, chunkid)
        get(session, chunkid)
        delete(session, chunkid)
        stat_get(session)
        stat_head(session)

