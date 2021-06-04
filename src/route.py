from flask import Flask, abort, request
from addr import Address, Port
from sys import exit
import shard
import vc
import requests
import json
from typing import Tuple

timeout = 2
app = Flask(__name__)


def run(p: Port = Port(8085)) -> None:
    if not isinstance(p, Port):
        raise Exception("route.py:run(...) supplied non Port type argument")
    app.run('0.0.0.0', len(p))

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
