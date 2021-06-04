from os import getenv
import time
from sys import argv, exit
import route
import route_shard
import route_view
import route_keyval
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
    evars = ("SOCKET_ADDRESS","VIEW", "SHARD_COUNT")
    emap = env_read(evars)

    # Self ADDR
    if "SOCKET_ADDRESS" in emap:
        data = emap["SOCKET_ADDRESS"].split(':')
        shard.self = Address(data[0], int(data[1]))
    else:
        print("No SOCKET_ADDRESS")
        exit(2)

    # Setup VIEW
    if "VIEW" in emap:
        view = emap["VIEW"].split(',')
        for server in view:
            data = server.split(':')
            shard.view.append(Address(data[0], int(data[1])))
            shard.master_view.append(Address(data[0], int(data[1])))
    else:
        print("No VIEW EVAR")
        exit(2)

    # Split Shards
    if "SHARD_COUNT" in emap:
        sc = int(emap["SHARD_COUNT"])
        shard.shard_view(sc)
        for sh in shard.shards:
            shard.master_shards.append(list())
            for serv in sh:
                shard.master_shards[-1].append(serv)
    else:
        print("No SHARD_COUNT EVAR")
        exit(2)

    # Setup Vector Clock for shard
    vc.vc = vc.VectorClock(len(shard.shards[shard.get_my_shard()]))

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

    # Print Data Before Start
    print(f"SELF: {str(shard.self)}")
    for serv in shard.view:
        print(f"VIEW: {str(serv)}")
    for i in range(len(shard.shards)):
        for serv in shard.shards[i]:
            print(f"SHARD {i}: {str(serv)}")

    # Run Server
    route.run(port)

    # Close Threads
    for i in range(len(threads)):
        threads[i].join()


def add_self():
    """
    on wakeup add self into view

    This is for whenever the server crashes and comes back to life it can
    add itself back into its old position
    """
    # TODO Add back into shard
    time.sleep(1)
    data = None
    for addr in shard.shards[shard.get_my_shard()]:
        if addr == shard.self: continue
        data = {'socket-address': str(shard.self)}
        try:
            requests.put('http://'+str(addr)+f'/key-value-store-view', data=json.dumps(data), timeout=2)
        except: continue
        break


if __name__ == "__main__":
    main()
