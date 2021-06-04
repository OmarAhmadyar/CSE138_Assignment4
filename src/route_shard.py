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

    # Add for everyone
    shard.shards[shard_id].route.append(addr)
    shard.internal_new_member_all(addr, shard_id)

    # Populate
    viewlist = list()
    for addr in shard.view:
        if addr is None:
            viewlist.append('')
        else: viewlist.append(str(addr))

    shardlist = list()
    for shart in shard.shards:
        shardlist.append(list())
        for serv in shart:
            shardlist[-1].append(str(serv))
    popdata = {'store': json.dumps(route_keyval.store), 'view':json.dumps(viewlist), 'causal-metadata':str(vc.vc), 'shards': json.dumps(shardlist)}

    requests.put('http://' + str(addr) + '/internal/populate/', data=json.dumps(popdata))
    return ({'message':'Node added successfully to shard'}, 200)

@route.app.route('/internal/add-member/<int:shard_id>', methods=['PUT'])
def int_add_member(shard_id:int):
    try:
        straddr = json.loads(request.get_data())['socket-address']
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
        shard.shards[shard_id].route.append(addr)
        return {'success': True}
    except: pass
    return {'success': False}

