#!/usr/bin/env python2

import time
import hashlib
import requests

bulk = "0123456789101112" * 65536
HDR_PREFIX = 'X-oio-'
reqid = "tnx0123456789ABCDEF"


def generate_chunkid(seed):
    h = hashlib.sha256()
    h.update(str(time.time()))
    h.update("+")
    h.update(str(seed))
    return h.hexdigest()

#
# def dump_reply(op, rep):
#     print op, repr(rep.status_code), repr(rep.headers), "body.length", len(rep.text)

def dump_reply(op, rep):
    print op, repr(rep.status_code)

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
for i in range(10):
    chunkid = generate_chunkid(i)
    for j in range(10):
        print "\nChunk: %s" % chunkid
        put(session, chunkid)
        #put(session, chunkid)
        # print "\nHEAD %s %s" % (session, chunkid)
        # head(session, chunkid)
        get(session, chunkid)

        #delete(session, chunkid)

        #get(session, chunkid)

        #delete(session, chunkid)
        # print "\nSTAT GET %s" % session,
        # stat_get(session)
        # print "\nSTAT HEAD %s" % session
        # stat_head(session)

        # delete(session, "C0626110C50D8EACEE53595AB131DCDF9106C078744FC8104A762D958E55A8C4")

# Not found chunks
# print "-------------- NOT FOUND ---------------"
# session = requests.Session()
# for i in range(10):
#     chunkid = generate_chunkid(i)
#     get(session, chunkid)
