import vc
from addr import IP, Port, Address
import json
import aiohttp
import asyncio
import async_timeout

self = Address("0.0.0.0", 8085)
master_view = list()
view = list()
atimeout = 2


# Invalidate Server --------------------------------------------------------
# Remove a server from the view of all servers (except the one being removed)
def invalidate_server(addr: Address):
    for i in range(len(view)):
        if view[i] == addr:
            view[i] = None
            del_view_all(addr)
            return True
    return False


# Call the invalidate_server_coroutine
def del_view_all(addr: Address):
    asyncio.run(invalidate_server_coroutine(addr))


# Remove a server from the view of all servers except yourself and the one being removed
async def invalidate_server_coroutine(inval: Address):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for addr in view:
            if addr is None: continue
            elif addr == self: continue
            elif addr == inval: continue
            tasks.append(asyncio.ensure_future(inval_one(session, addr, inval)))
        results = await asyncio.gather(*tasks)

        # Invalidate anyone who failed
        for addr in results:
            if addr is None: continue
            for i in range(len(view)):
                if view[i] == addr:
                    view[i] = None
                    break
            await invalidate_server_coroutine(addr)


# Send an internal view delete to a target server to tell it to remove a server from it's view
async def inval_one(session, toserv, inval):
    data_ = json.dumps({"socket-address": str(inval)})
    try:
        async with async_timeout.timeout(atimeout):
            async with session.delete('http://'+str(toserv)+f'/internal/view', data=data_) as resp:
                data = await resp.json()
                if 'success' in data and data['success']:
                    return None
    except:
        return toserv


# Add Server to View -------------------------------------------------------
# Call the add server coroutine
def add_server_all(addr: Address):
    asyncio.run(add_server_coroutine(addr))


# Tell all servers (except yourself and the one you are adding) to add a new server to their view
async def add_server_coroutine(add: Address):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for addr in view:
            if addr is None: continue
            elif addr == self: continue
            elif addr == add: continue
            tasks.append(asyncio.ensure_future(add_server_one(session, addr, add)))
        results = await asyncio.gather(*tasks)

        # Invalidate anyone who failed
        for addr in results:
            if addr is None: continue
            for i in range(len(view)):
                if view[i] == addr:
                    view[i] = None
                    break
            await invalidate_server_coroutine(addr)


# Tell a single server to add a new server to it's view
async def add_server_one(session, toserv, add):
    data_ = json.dumps({"socket-address": str(add)})
    try:
        async with async_timeout.timeout(atimeout):
            async with session.put('http://'+str(toserv)+f'/internal/view', data=data_) as resp:
                data = await resp.json()
                if 'success' in data and data['success']:
                    return None
    except:
        return toserv


# PUT Key/Val --------------------------------------------------------------
# Call the put all coroutine
def put_all(key, val):
    asyncio.run(put_all_coroutine(key,val))
    # send put request to all servers in view


# Send an internal put to all other servers to put a new key/val
async def put_all_coroutine(key, val):
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Send Puts
        for addr in view:
            if addr is None: continue
            elif addr == self: continue
            tasks.append(asyncio.ensure_future(put_one(session, addr, key, val)))
        results = await asyncio.gather(*tasks)

        # Invalidate anyone who failed
        for addr in results:
            if addr is None: continue
            for i in range(len(view)):
                if view[i] == addr:
                    view[i] = None
                    break
            await invalidate_server_coroutine(addr)


# Send an internal put to a single server to put a new key/val
async def put_one(session, addr, key, val):
    data = json.dumps({"value": str(val), "from": str(self), "causal-metadata": str(vc.vc)})
    try:
        async with async_timeout.timeout(atimeout):
            async with session.put('http://'+str(addr)+f'/internal/kvs/{key}', data=data) as resp:
                data = await resp.json()
                if 'success' in data and data['success'] == True:
                    return None
    except:
        pass
    return addr


# DEL Key/Val --------------------------------------------------------------
# Call delete all coroutine
def del_all(key):
    asyncio.run(del_all_coroutine(key))
    # send all nodes the command to delete a key/val pair


# Send an internal delete to all other servers to delete a key/val mapping
async def del_all_coroutine(key):
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Send Puts
        for addr in view:
            if addr is None: continue
            elif addr == self: continue
            tasks.append(asyncio.ensure_future(del_one(session, addr, key)))
        results = await asyncio.gather(*tasks)

        # Invalidate anyone who failed
        for addr in results:
            if addr is None: continue
            for i in range(len(view)):
                if view[i] == addr:
                    view[i] = None
                    break
            await invalidate_server_coroutine(addr)


# Send an internal delete to a single server to delete a key/val mapping
async def del_one(session, addr, key):
    data_ = json.dumps({"from": str(self), "causal-metadata": str(vc.vc)})
    try:
        async with async_timeout.timeout(atimeout):
            async with session.delete('http://'+str(addr)+f'/internal/kvs/{key}', data=data_) as resp:
                data = await resp.json()
                if 'success' in data and data['success'] == True:
                    return None
    except:
        pass
    return addr
