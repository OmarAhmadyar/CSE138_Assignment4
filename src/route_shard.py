from flask import Flask, abort, request
from addr import Address, Port
from sys import exit
import shard
import vc
import requests
import json
from typing import Tuple

import route
import route_keyval

# Shard --------------------------------------------------------------------
@route.app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_shard_ids():
    return {'message': "Shard IDs retrieved successfully", \
            'shard-ids': [x for x in range(len(shard.shards))]} \
            ,200


@route.app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_shard_id():
    return {'message': 'Shard ID of the node retrieved successfully'
           ,'shard-id': str(shard.get_my_shard())}, 200


@route.app.route('/key-value-store-shard/shard-id-members/<int:idn>', methods=['GET'])
def get_shard_members(idn):
    strlist = list()
    for server in shard.shards[idn]:
        strlist.append(str(server))
    return {'message': 'Members of shard ID retrieved successfully'
           ,'shard-id-members': json.dumps(strlist)}, 200


@route.app.route('/key-value-store-shard/shard-id-key-count/<int:idx>', methods=['GET'])
def get_shard_key_count(idx):
    if shard.get_my_shard() == idx:
        return {'message':'Key count of shared ID retrieved successfully'
               ,'shard-id-key-count': len(route_keyval.store)}, 200
    else:
        return route.forward_req_shard(idx, request)


@route.app.route('/key-value-store-shard/add-member/<int:shard_id>', methods=['PUT'])
def add_member(shard_id):
    # IF SELF NOT IN SHARD THEN FORWARD
    if shard.get_my_shard() != shard_id:
        return route.forward_req_shard(shard_id, request)

    data = json.loads(request.get_data())

    # Malformed Req
    if not ("socket-address" in data):
        abort(400)

    addr = None
    try:
        straddr = data["socket-address"]
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
    except: abort(400)

    # Check if it's already there
    for serv in shard.shards[shard_id]:
        if serv == addr:
            return (  {  "error":"Socket address already exists in the shard"
                      ,  "message":"Error in PUT"
                      }
                   ,  404
                   )
    # ADD TO VIEW IF NOT ALREADY THERE
    if not (addr in shard.view):
        if addr in shard.master_view:
            idx = shard.master_view.index(addr)
            assert(shard.view[idx] is None)
            shard.view[idx] = addr
        else:
            shard.master_view.append(addr)
            shard.view.append(addr)

    # TELL EVERYONE ELSE TO ADD IT TO THEIR VIEW
    shard.add_server_all(addr)

    # TELL EVERYONE ELSE (INCLUDING YOURSELF) THAT HE IS IN SHARD_ID
    shard.internal_new_member_all(addr, shard_id)

    return ({'message':'Node added successfully to shard'}, 200)


@route.app.route('/key-value-store-shard/reshard', methods=['PUT'])
def reshard():
    data = json.loads(request.get_data())
    shard_count = int(data['shard-count'])

    # TODO ERROR IF INSUFFICIENT NODES FOR SHARD_COUNT
    view_count = 0;
    for serv in shard.view:
        if serv is not None: view_count += 1
    if view_count // shard_count < 2:
        return {'message':'Not enough nodes to provide fault-tolerance with the given shard count!'}, 400

    # GET STORE FROM EVERY SINGLE GROUP
    ostores = list()
    for shard_group in shard.shards:
        for serv in shard_group:
            ret = None
            try: ret = requests.get(f'http://{str(serv)}/internal/populate', timeout=shard.atimeout)
            except: continue
            ostores.append(json.loads(json.loads(ret.text)['data']))
            break

    # UNION SELF STORE W/ EACH GROUP STORE
    for ostore in ostores:
        route_keyval.store.update(ostore)

    # SELF STORE NOW HAS ALL KEY/VAL PAIRS
    # RESHARD LOCALLY
    shard.shard_view(shard_count)
    # TELL EVERYONE TO POP THEMSELVES OFF ME
    shard.all_pop_off_me()
    # DISCARD ALL EXCESSIVE KEYS IN MY STORE
    for key in route_keyval.store:
        if not is_my_key(key):
            del route_keyval.store[key]

    return {'message': 'Resharding done successfully'}, 200


@route.app.route('/internal/add-member/<int:shard_id>', methods=['PUT'])
def int_add_member(shard_id:int):
    try:
        straddr = json.loads(request.get_data())['socket-address']
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))

        # Check if he's already there
        for serv in shard.shards[shard_id]:
            if serv == addr: abort(404)

        # Remove him if he's in any other shards
        for group in range(len(shard.shards)):
            for i in range(len(shard.shards[group])):
                if shard.shards[group][i] == addr:
                    shard.shards[group][i] = None

        # Reinstate him if he previously belonged to this shard
        reinstate = False
        for i in range(len(shard.master_shards[shard_id])):
            if shard.master_shards[shard_id][i] == addr:
                shard.shards[shard_id][i] = addr
                reinstate = True

        # Add him if he's brand new to this shard
        if not reinstate:
            shard.master_shards[shard_id].append(addr)
            shard.shards[shard_id].append(addr)
            if shard_id == shard.get_my_shard(): vc.vc.add()
        return {'success': True}
    except: pass
    return {'success': False}, 400


@route.app.route('/internal/add-member/YOU', methods=['PUT'])
def pop_yourself():
    data = json.loads(request.get_data())
    spl = data['callback'].split(':')
    addr = Address(spl[0], int(spl[1]))
    ret = requests.get(f'http://{str(addr)}/internal/populate', timeout=route.timeout)

    data = json.loads(ret.text)

    # VIEW
    shard.view = load_view(data['view'])
    shard.master_view = load_view(data['mview'])

    # SHARDS
    shard.shards = load_shards(json.loads(data['shards']))
    shard.master_shards = load_shards(json.loads(data['mshards']))

    # STORE -- ONLY STORE THOSE WHICH BELONG TO YOU
    route_keyval.store.clear()
    ostore = json.loads(data['data'])
    myidx = shard.get_my_shard()
    for key in ostore:
        if shard.get_shard_key(key) == myidx:
            route_keyval.store[key] = ostore[key]

    # VC
    meta = json.loads(data['causal-metadata'])
    assert(int(meta[0]) == shard.get_my_shard())
    vc.vc = vc.VectorClock(meta[1])

    return 'NOICE'

def load_shards(strshard):
    result = list()
    for shstr in strshard:
        result.append(list())
        sh = json.loads(shstr)
        for servstr in sh:
            if servstr == '':
                result[-1].append(None)
            else:
                spl = servstr.split(':')
                result[-1].append(Address(spl[0], int(spl[1])))
    return result

def load_view(strview):
    result = list()
    for addrstr in json.loads(strview):
        if addrstr is None: result.append('')
        else:
            spl = addrstr.split(':')
            result.append(Address(spl[0], int(spl[1])))
    return result

