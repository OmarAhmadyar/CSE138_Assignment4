from flask import Flask, abort, request
from addr import Address, Port
from sys import exit
import shard
import vc
import requests
import json
from typing import Tuple

store = dict() #type: dict[str, str]
timeout = 2
app = Flask(__name__)


def run(p: Port = Port(8085)) -> None:
    if not isinstance(p, Port):
        raise("route.py:run(...) supplied non Port type argument")
    app.run('0.0.0.0', len(p))


# Put Key - Insert or update a key/val pair
# From Client
@app.route('/key-value-store/<string:key>', methods=['PUT'])
def put_key(key:str):
    if not shard.is_my_key(key): return forward_keyval_shard(key, request)

    data = json.loads(request.get_data())
    val = None

    # Load val or error
    if len(key) > 50: return ({"error":"Key is too long","message":"Error in PUT"},400)
    elif "value" in data: val = data["value"]
    else: return ({"error" : "Value is missing", "message" : "Error in PUT"}, 400)

    # See if client provided vector clock
    recv_vc = None
    try:
        if 'causal-metadata' in data:
            recv_vc = vc.VectorClock(json.loads(data['causal-metadata']))
    except:
        recv_vc = None

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
        index = shard.view.index(shard.self)
        vc.vc.clock[index] += 1

    good['causal-metadata'] = str(vc.vc)
    return (good, 200 if good["replaced"] else 201)


# Get Key - Retrieve a val mapping for a key
# From Client
@app.route('/key-value-store/<string:key>', methods=['GET'])
def get_key(key:str, force_outdated = False):
    if not shard.is_my_key(key): return forward_keyval_shard(key, request)
    if type(key) is not str:
        abort(400)

    # if data is outdated:
        # if internal call: jump to next server in view
        # else: jump to first server in view
    recv_vc = None
    outdated = False
    internal = False
    data = None
    try:
        data = json.loads(request.get_data())
        if 'causal-metadata' in data:
            recv_vc = vc.VectorClock(json.loads(data['causal-metadata']))
            outdated = vc.vc < recv_vc
        if 'internal' in data: internal = True
        else: outdated = False
    except: outdated = False

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
            vc.vc.max(vc.VectorClock(json.loads(json.loads(result.text)['causal-metadata'])))

    if key in store:
        return ({"doesExist":True,"message":"Retrieved successfully","value":store[key], 'causal-metadata':str(vc.vc)},200)
    else:
        return ({"doesExist":False,"error":"Key does not exist","message":"Error in GET",'causal-metadata':str(vc.vc)},404)


# Delete key - Remove a value mapping for a key
# From Client
@app.route('/key-value-store/<string:key>', methods=['DELETE'])
def delete_key(key:str):
    if not shard.is_my_key(key): return forward_keyval_shard(key, request)
    if type(key) is not str:
        abort(400)

    # See if client provided vector clock
    data = json.loads(request.get_data())
    recv_vc = None
    try:
        if 'causal-metadata' in data:
            recv_vc = vc.VectorClock(json.loads(data['causal-metadata']))
    except:
        recv_vc = None

    # If put is in our causal past, then ignore it
    if (recv_vc is None) or (not (recv_vc < vc.vc)):
        index = shard.view.index(shard.self)
        if key in store:
            deleted = True
            vc.vc.clock[index] += 1
            shard.del_all(key)
            store.pop(key)

    retval = dict()
    retCode = 200
    if deleted:
        retVal = {"doesExist":True,"message":"Deleted successfully"}
    else:
        retVal = {"doesExist":False,"error":"Key does not exist","message":"Error in DELETE"}
        retCode = 404
    retval['causal-metadata'] = str(vc.vc)
    return retVal, retCode


# View --------------------------------------------------------------------
# Get View - Get the current view from this server
# From Client
@app.route('/key-value-store-view', methods=['GET'])
def get_view():
    strview = ""
    for addr in shard.view:
        if addr is None: pass
        else: strview += str(addr) + ','
    strview = strview[0:-1]  #trim off leading comma
    return ({"message":"View retrieved successfully","view":strview},200)


# Delete View - Delete a server from the view
# From Client
@app.route('/key-value-store-view', methods=['DELETE'])
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
@app.route('/key-value-store-view', methods=['PUT'])
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
        shard.view.append(addr)
        shard.master_view.append(addr)
    viewlist = list()
    for addr in shard.view:
        if addr is None:
            viewlist.append('')
        else: viewlist.append(str(addr))
    popdata = {"store": json.dumps(store), "view": json.dumps(viewlist), "causal-metadata":str(vc.vc)}
    requests.put(f"http://{str(addr)}/internal/populate", data=json.dumps(popdata))
    shard.add_server_all(addr)
    return ({"message":"Replica added successfully to the view"},201)


# Shard --------------------------------------------------------------------
@app.route('/key-value-store-shard/shard-ids', methods=['GET'])
def get_shard_ids():
    return {'message': "Shard IDs retrieved successfully", \
            'shard-ids': [x for x in range(len(shard.shards))]} \
            ,200


@app.route('/key-value-store-shard/node-shard-id', methods=['GET'])
def get_shard_id():
    return {'message': 'Shard ID of the node retrieved successfully'
           ,'shard-id': str(shard.get_my_shard())}, 200


@app.route('/key-value-store-shard/shard-id-members/<int:idn>', methods=['GET'])
def get_shard_members(idn):
    strlist = list()
    for server in shard.shards[idn]:
        strlist.append(str(server))
    return {'message': 'Members of shard ID retrieved successfully'
           ,'shard-id-members': json.dumps(strlist)}, 200


@app.route('/key-value-store-shard/shard-id-key-count/<int:idx>', methods=['GET'])
def get_shard_key_count(idx):
    if shard.get_my_shard() == idx:
        return {'message':'Key count of shared ID retrieved successfully'
               ,'shard-id-key-count': len(store)}, 200
    else:
        return forward_req_shard(idx, request)



# Internal -----------------------------------------------------------------
# Internal Put Key - Put a key/val pair in this server
# From Server
# Changes do not propagate
@app.route('/internal/kvs/<string:key>', methods=['PUT'])
def int_put_key(key:str):
    data = json.loads(request.get_data())
    recv_vc = vc.VectorClock(json.loads(data['causal-metadata']))

    try:
        from_spl = data['from'].split(':')
        from_ = Address(from_spl[0], int(from_spl[1]))
        index = shard.view.index(from_)

        if recv_vc < vc.vc: pass
        else: store[key] = data["value"]

        vc.vc.clock[index] += 1
    except:
        {'success':False},403
    return {'success': True}


# Internal delete key - Delete a key/val pair from this server
# From Server
# Changes do not propogate
@app.route('/internal/kvs/<string:key>', methods=['DELETE'])
def int_del_key(key:str):
    data = json.loads(request.get_data())

    recv_vc = vc.VectorClock(json.loads(data['causal-metadata']))
    past = recv_vc < vc.vc

    try:
        from_spl = data['from'].split(':')
        from_ = Address(from_spl[0], int(from_spl[1]))
        index = shard.view.index(from_)
        vc.vc.clock[index] += 1

        if key in store and (not past):
            del store[key]
            return {'success': True}
    except:
        {'success':False},403
    return {'success': True}, 404


# Internal delete view/server - Delete a server from the view on this server
# From Server
# Changes do not propagate
@app.route('/internal/view', methods=['DELETE'])
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
@app.route('/internal/view', methods=['PUT'])
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
        shard.view.append(addr)
        shard.master_view.append(addr)
    return {'success': True}


# Internal populate - This server is going to be populated with key/val pairs and a new view
# From Server
@app.route('/internal/populate', methods=['PUT'])
def int_populate():
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
        vc.vc = vc.VectorClock(json.loads(data['causal-metadata']))
    except:
        return {'success':False}, 400

    return {'success': True}


@app.route('/internal/populate', methods=['GET'])
def get_data():
    # View
    view = list()
    for addr in shard.view:
        view.append(str(addr))

    # All together
    data = {"causal-metadata": str(vc.vc), 'view': json.dumps(view), 'data': json.dumps(store)}
    return json.dumps(data)


# Helper -------------------------------------------------------------------
def forward_req(addr: Address, req) -> Tuple[str,int]:
    error_msg = {"error" : "Main instance is down", "message" : f"Error in {str(req.method)}"}
    error_code = 503
    url = f'http://{str(addr)}'
    ret = None
    try:
        if req.method == 'GET':
            ret = requests.get(url+req.full_path, timeout=timeout, data=req.get_data())
        elif req.method == 'PUT':
            ret = requests.put(url+req.full_path, timeout=timeout, data=req.get_data())
        elif req.method == 'DELETE':
            ret = requests.delete(url+req.full_path, timeout=timeout, data=req.get_data())
        else: exit(4)
    except:
        return (json.dumps(error_msg), error_code)

    return (ret.text, ret.status_code)

def forward_keyval_shard(key, req) -> Tuple[str,int]:
    return forward_req_shard(shard.get_shard_key(key), req)

def forward_req_shard(serv_id, req) -> Tuple[str,int]:
    ret = None
    for serv in shard.shards[serv_id]:
        ret = forward_req(serv, req)
        if ret[1] < 500: break
    return ret


# Debug --------------------------------------------------------------------
if __name__ == '__main__':
    app.run("0.0.0.0", 1234)
