from os import getenv
import time
from sys import argv, exit
import route
import requests
import json
import shard
from addr import Address, Port
import vc
import threading


def env_read(tup:tuple) -> dict:
    ret = dict() #type: dict[str,str]
    for name in tup:
        val = getenv(name)
        if val is not None:
            ret[name] = val
    return ret


def main():
    # Get ENV Vars
    evars = ("SOCKET_ADDRESS","VIEW")
    emap = env_read(evars)

    # Self ADDR
    if "SOCKET_ADDRESS" in emap:
        data = emap["SOCKET_ADDRESS"].split(':')
        shard.self = Address(data[0], int(data[1]))
        print(f"Self: {shard.self}")
    else:
        print("No SOCKET_ADDRESS")
        exit(2)

    # Setup VIEW and VC
    if "VIEW" in emap:
        view = emap["VIEW"].split(',')
        vc.vc = vc.VectorClock(len(view))
        for server in view:
            data = server.split(':')
            print(f"VIEW add: {data}")
            shard.view.append(Address(data[0], int(data[1])))
            shard.master_view.append(Address(data[0], int(data[1])))
    else:
        print("No VIEW EVAR")
        exit(1)

    # Setup bind port
    port = Port(len(shard.self.getPort()))
    if len(argv) > 1:
        port = Port(int(argv[1]))

    # Add yourself to everyones view if not already there
    threads = list()
    if True:
        x = threading.Thread(target=add_self)
        threads.append(x)
        x.start()

    # Run Server
    route.run(port)

    # Close Threads
    for i in range(len(threads)):
        threads[i].join()


def add_self():
    time.sleep(1)
    data = None
    for addr in shard.view:
        if addr == shard.self: continue
        data = {'socket-address': str(shard.self)}
        try:
            requests.put('http://'+str(addr)+f'/key-value-store-view', data=json.dumps(data), timeout=2)
        except: continue
        break


if __name__ == "__main__":
    main()
