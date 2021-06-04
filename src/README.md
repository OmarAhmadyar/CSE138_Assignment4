## Team Contributions
Omar Ahmadyar
- Causal consistency implementation
   - Vector clock implementation (vc.py)
   - Causal history checks (ie vector clock checks and handling in int_put_key, int_del_key and read_key)
   - Vector clock/causal metadata updates (maintaining accurate vector clocks)
- Sharding implementation
   - Asyncronous communication between shards in shard.py
   - Maintence of the view
- Testing/debugging of python implementation
   - Running and testing python nodes directly

Ryan Steinwert
- Routing of requests in route.py
   - Handling client requests using Flask routing
   - Responding to requests with json formatting
- Docker file
- Documentation (readme and in-code)
- Testing/debugging of concurrent nodes and docker implementation
   - Running docker containers on subnet with manual testing
   - Using provided test suite

## Acknowledgements
- N/A

## Citations
[Flask Docs](https://flask.palletsprojects.com/en/1.1.x/)
- For syntax and usage of Flask, specifically routing in route.py

[Request Docs](https://docs.python-requests.org/en/master/index.html)
- For syntax and usage of the requests library
- Consulted for handling of http requests in route.py

[asyncio Docs](https://docs.python.org/3/library/asyncio.html)
- For working with concurrent threads
- Consulted for handling asynchronous shards of the key value store over the network

[Asynchronous HTTP Requests in Python with aiohttp and asyncio](https://www.twilio.com/blog/asynchronous-http-requests-in-python-with-aiohttp)
- Used for further guidance on the usage of asyncio and aiohttp for asyncronous request handling
- Specifically helped to jumpstart the implementation of shard.py, which communicates asyncronously with other shards in the network

[An Intro to Threading in Python](https://realpython.com/intro-to-python-threading/)
- Referenced for simple multithreading
- Used in main.py to add replicas back to the view when they come back online 

apropos docker
- Consulted for information on the use of docker

man docker ...
- Consulted for specific usage of the docker command


# TODO
add citation for MD5 hashing
https://www.geeksforgeeks.org/md5-hash-python/
