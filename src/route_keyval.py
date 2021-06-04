from flask import Flask, abort, request
from addr import Address, Port
from sys import exit
import shard
import vc
import requests
import json
from typing import Tuple

import route

store = dict() #type: dict[str, str]

# Put Key - Insert or update a key/val pair
# From Client
@route.app.route('/key-value-store/<string:key>', methods=['PUT'])
def put_key(key:str):
    if not shard.is_my_key(key): return route.forward_keyval_shard(key, request)

    data = json.loads(request.get_data())
    val = None

    # Load val or error
    if len(key) > 50: return ({"error":"Key is too long","message":"Error in PUT"},400)
    elif "value" in data: val = data["value"]
    else: return ({"error" : "Value is missing", "message" : "Error in PUT"}, 400)


    # See if client provided vector clock
    myidx = shard.get_my_shard()
    recv_vc = None
    if 'causal-metadata' in data and len(data['causal-metadata']) > 1:
        meta = json.loads(data['causal-metadata'])
        if int(meta[0]) == myidx:
            recv_vc = vc.VectorClock(json.loads(meta[1]))

    # Update your past events
    if recv_vc is not None and vc.vc < recv_vc:
        get_key(key, True)

    # Successful PUT
    good = {"message": "Added successfully", "replaced": False}
    if key in store:
        good["message"] = "Updated successfully"
        good["replaced"] = True

    # If put is in our causal past, then ignore it
    if (recv_vc is None) or (not (recv_vc < vc.vc)):
        store[key] = val
        shard.put_all(key, val)
        index = shard.shards[shard.get_my_shard()].index(shard.self)
        vc.vc.clock[index] += 1

    good['causal-metadata'] = json.dumps([myidx, str(vc.vc)])
    good['shard-id'] = shard.get_my_shard()
    return (good, 200 if good["replaced"] else 201)


# Get Key - Retrieve a val mroute.apping for a key
# From Client
@route.app.route('/key-value-store/<string:key>', methods=['GET'])
def get_key(key:str, force_outdated = False):
    if not shard.is_my_key(key): return route.forward_keyval_shard(key, request)
    if type(key) is not str:
        abort(400)

    # IF DATA IS OUTDATED:
        # if internal call: jump to next server in view
        # else: jump to first server in view
    recv_vc = None
    outdated = False
    internal = False
    data = None
    myidx = shard.get_my_shard()

    if request.get_data() != b'':
        data = json.loads(request.get_data())
        if 'causal-metadata' in data and len(data['causal-metadata']) > 1:
            meta = json.loads(data['causal-metadata'])
            if int(meta[0]) == myidx:
                recv_vc = vc.VectorClock(meta[1])
                outdated = vc.vc < recv_vc
        if 'internal' in data: internal = True
        #else: outdated = False  #TODO Not sure why this line is here

    if outdated or force_outdated:
        shard.add_server_all(shard.self)
        data['internal'] = True
        result = None
        if internal:  #go to next server
            idx = shard.view.index(shard.self)
            for i in range(idx, len(shard.view)):
                if shard.view[i] is None: continue
                elif shard.view[i] == shard.self: continue
                result = requests.get('http://'+str(shard.view[i])+f'/key-value-store/{key}', data=json.dumps(data))
                break
        else:  #go to first server
            for addr in shard.view:
                if addr is not None:
                    result = requests.get('http://'+str(addr)+f'/key-value-store/{key}', data=json.dumps(data))
                    break
        if result is not None and result.status_code == 200:
            store[key] = json.loads(result.text)['value']
            meta = json.loads(result.text)['causal-metadata']
            vc.vc.max(vc.VectorClock(json.loads(meta[1])))

    # Causal Metadata
    causal_meta = json.dumps([myidx, str(vc.vc)])
    if key in store:
        return ({"doesExist":True,"message":"Retrieved successfully","value":store[key], 'causal-metadata':causal_meta},200)
    else:
        return ({"doesExist":False,"error":"Key does not exist","message":"Error in GET",'causal-metadata':causal_meta},404)


# Delete key - Remove a value mroute.apping for a key
# From Client
@route.app.route('/key-value-store/<string:key>', methods=['DELETE'])
def delete_key(key:str):
    if not shard.is_my_key(key): return route.forward_keyval_shard(key, request)
    if type(key) is not str:
        abort(400)

    # See if client provided vector clock
    if request.get_data() != b'':
        data = json.loads(request.get_data())
        recv_vc = None
        if 'causal-metadata' in data and len(data['causal-metadata']) > 1:
            meta = json.loads(data['causal-metadata'])
            if int(meta[0]) == shard.get_my_shard():
                recv_vc = vc.VectorClock(meta[1])

    # If put is in our causal past, then ignore it
    deleted = False
    if (recv_vc is None) or (not (recv_vc < vc.vc)):
        index = shard.shards[shard.get_my_shard()].index(shard.self)
        if key in store:
            deleted = True
            vc.vc.clock[index] += 1
            shard.del_all(key)
            store.pop(key)

    retval = dict()
    retCode = 200
    if deleted:
        retval = {"doesExist":True,"message":"Deleted successfully"}
    else:
        retval = {"doesExist":False,"error":"Key does not exist","message":"Error in DELETE"}
        retCode = 404
    retval['causal-metadata'] = json.dumps([shard.get_my_shard(), str(vc.vc)])
    retval['shard-id'] = shard.get_my_shard()
    return retval, retCode

# Internal -----------------------------------------------------------------
# Internal Put Key - Put a key/val pair in this server
# From Server
# Changes do not propagate
@route.app.route('/internal/kvs/<string:key>', methods=['PUT'])
def int_put_key(key:str):
    data = json.loads(request.get_data())
    meta = json.loads(data['causal-metadata'])
    recv_vc = vc.VectorClock(meta[1])

    try:
        from_spl = data['from'].split(':')
        from_ = Address(from_spl[0], int(from_spl[1]))
        index = shard.shards[shard.get_my_shard()].index(from_)

        if recv_vc < vc.vc: pass
        else: store[key] = data["value"]

        vc.vc.clock[index] += 1
    except: pass
    return {'success': True}


# Internal delete key - Delete a key/val pair from this server
# From Server
# Changes do not propogate
@route.app.route('/internal/kvs/<string:key>', methods=['DELETE'])
def int_del_key(key:str):
    data = json.loads(request.get_data())

    meta = json.loads(data['causal-metadata'])
    recv_vc = vc.VectorClock(meta[1])
    past = recv_vc < vc.vc

    try:
        from_spl = data['from'].split(':')
        from_ = Address(from_spl[0], int(from_spl[1]))
        index = shard.shards[shard.get_my_shard()].index(from_)
        vc.vc.clock[index] += 1

        if key in store and (not past):
            del store[key]
            return {'success': True}
    except: pass
    return {'success': True}, 404


# Internal populate - This server is going to be populated with key/val pairs and a new view
# From Server
# DEPRECATED -- Use get_data() below instead
@route.app.route('/internal/populate', methods=['PUT'])
def int_populate():
    print("USAGE OF DEPRECATED FUNCTION")
    data = None
    store.clear()
    shard.view.clear()
    try:
        # Key/Val pairs
        data = json.loads(request.get_data())
        ostore = json.loads(data['store'])
        for key in ostore:
            store[key] = ostore[key]

        # View
        oview = json.loads(data['view'])
        for addr in oview:
            if addr != '':
                spl = addr.split(':')
                shard.view.append(Address(spl[0], int(spl[1])))
                shard.master_view.append(Address(spl[0], int(spl[1])))
            else: shard.view.append(None)

        # Vector Clock
        meta = json.loads(data['causal-metadata'])
        vc.vc = vc.VectorClock(meta[1])
    except:
        return {'success':False}, 400

    return {'success': True}


@route.app.route('/internal/populate', methods=['GET'])
def get_data():
    # View
    view = list()
    for addr in shard.view:
        if addr is not None: view.append(str(addr))
        else: view.append('')

    # Master View
    mview = list()
    for addr in shard.master_view:
        mview.append(str(addr))

    # Shard
    strshard = list()
    for sh in shard.shards:
        temp = list()
        for serv in sh:
            if serv is None: temp.append('')
            else: temp.append(str(serv))
        strshard.append(json.dumps(temp))
    strshard = json.dumps(strshard)

    # Master Shard
    masterstrshard = list()
    for sh in shard.master_shards:
        temp = list()
        for serv in sh:
            temp.append(str(serv))
        masterstrshard.append(json.dumps(temp))
    masterstrshard = json.dumps(masterstrshard)

    # All together
    data = {  "causal-metadata": json.dumps([shard.get_my_shard(), str(vc.vc)])
           ,  'view': json.dumps(view)
           ,  'mview':json.dumps(mview)
           ,  'shards':strshard
           ,  'mshards': masterstrshard
           ,  'data': json.dumps(store)
           }
    return json.dumps(data)
