from flask import Flask, abort, request
from addr import Address, Port
from sys import exit
import shard
import vc
import requests
import json
from typing import Tuple

import route

# View --------------------------------------------------------------------
# Get View - Get the current view from this server
# From Client
@route.app.route('/key-value-store-view', methods=['GET'])
def get_view():
    strview = ""
    for addr in shard.view:
        if addr is None: pass
        else: strview += str(addr) + ','
    strview = strview[0:-1]  #trim off leading comma
    return ({"message":"View retrieved successfully","view":strview},200)


# Delete View - Delete a server from the view
# From Client
@route.app.route('/key-value-store-view', methods=['DELETE'])
def del_view():
    data = json.loads(request.get_data())

    # Malformed Req
    if not ("socket-address" in data):
        abort(400)

    # Get Server Address
    addr = None
    try:
        straddr = data["socket-address"]
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
    except:
        abort(400)

    # Delete Server From view but preserve indexes
    if shard.invalidate_server(addr):
        return ({"message": "Replica deleted successfully from the view"}, 200)
    else:
        return ({ "error": "Socket address does not exist in the view"
                , "message": "Error in DELETE"
                }
                , 404
                )


# Add View - Add a server to the view
# From Client
@route.app.route('/key-value-store-view', methods=['PUT'])
def add_view():
    data = json.loads(request.get_data())

    # Malformed Req
    if not ("socket-address" in data):
        abort(400)

    # Get Server Address
    addr = None
    try:
        straddr = data["socket-address"]
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
    except:
        abort(400)

    # Check if it's already there
    for i in range(len(shard.view)):
        if shard.view[i] == addr:
            return (  {  "error":"Socket address already exists in the view"
                      ,  "message":"Error in PUT"
                      }
                   ,  404
                   )

    # Add server to view
    if addr in shard.master_view:
        index = shard.master_view.index(addr)
        assert(shard.view[index] is None)
        shard.view[index] = shard.master_view[index]
    else:
        vc.vc.add()
        shard.view.route.append(addr)
        shard.master_view.route.append(addr)
    #viewlist = list()
    #for addr in shard.view:
    #    if addr is None:
    #        viewlist.route.append('')
    #    else: viewlist.route.append(str(addr))
    #popdata = {"store": json.dumps(store), "view": json.dumps(viewlist), "causal-metadata":str(vc.vc)}
    #requests.put(f"http://{str(addr)}/internal/populate", data=json.dumps(popdata))
    #shard.add_server_all(addr)
    return ({"message":"Replica added successfully to the view"},201)

# Internal delete view/server - Delete a server from the view on this server
# From Server
# Changes do not propagate
@route.app.route('/internal/view', methods=['DELETE'])
def int_del_view():
    data = json.loads(request.get_data())
    # Malformed Req
    if not ("socket-address" in data):
        abort(400)
    # Get Server Address
    addr = None
    try:
        straddr = data["socket-address"]
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
    except:
        abort(400)
    for i in range(len(shard.view)):
        if shard.view[i] == addr:
            shard.view[i] = None
            return {'success': True}
    return {'success':False}, 404


# Internal add view - Add a server to the view on this server
# From Server
# Changes do not propagate
@route.app.route('/internal/view', methods=['PUT'])
def int_add_view():
    data = json.loads(request.get_data())
    # Malformed Req
    if not ("socket-address" in data):
        abort(400)
    # Get Server Address
    addr = None
    try:
        straddr = data["socket-address"]
        spl = straddr.split(':')
        addr = Address(spl[0], int(spl[1]))
    except:
        abort(400)
    for i in range(len(shard.view)):
        if shard.view[i] == addr:
            return {'success': False}, 404

    if addr in shard.master_view:
        index = shard.master_view.index(addr)
        assert (shard.view[index] is None)
        shard.view[index] = shard.master_view[index]
    else:
        vc.vc.add()
        shard.view.route.append(addr)
        shard.master_view.route.append(addr)
    return {'success': True}


