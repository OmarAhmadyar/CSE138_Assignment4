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
    # TELL EVERYONE ELSE TO ADD IT TO THEIR VIEW
    add_server_all(addr)

    # Add for everyone
    shard.internal_new_member_all(addr, shard_id)

    return ({'message':'Node added successfully to shard'}, 200)


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
        return {'success': True}
    except: pass
    return {'success': False}


@route.app.route('/internal/add-member/YOU', methods=['PUT'])
def pop_yourself():
    data = json.loads(request.get_data())
    spl = data['callback'].split(':')
    addr = Address(spl[0], int(spl[1]))
    ret = requests.get(f'http://{str(addr)}/internal/populate', timeout=route.timeout)

    data = json.loads(ret.text)

    # VC
    vc.vc = vc.VectorClock(json.loads(data['causal-metadata']))

    # VIEW
    shard.view.clear()
    shard.master_view.clear()
    for addrstr in json.loads(data['view']):
        spl = addrstr.split(':')
        shard.view.append(Address(spl[0], int(spl[1])))
        shard.master_view.append(Address(spl[0], int(spl[1])))

    # STORE
    route_keyval.store.clear()
    ostore = json.loads(data['data'])
    for key in ostore:
        route_keyval.store[key] = ostore[key]

    # SHARDS
    shard.shards.clear()
    shard.master_shards.clear()
    shardstr = json.loads(data['shards'])
    for shstr in shardstr:
        shard.shards.append(list())
        shard.master_shards.append(list())
        sh = json.loads(shstr)
        for servstr in sh:
            spl = servstr.split(':')
            shard.shards[-1].append(Address(spl[0], int(spl[1])))
            shard.master_shards[-1].append(Address(spl[0], int(spl[1])))

    return 'NOICE'
