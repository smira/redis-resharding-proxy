Redis Resharding Proxy
======================

.. image:: https://travis-ci.org/smira/redis-resharding-proxy.png?branch=master
    :target: https://travis-ci.org/smira/redis-resharding-proxy

.. image:: https://coveralls.io/repos/smira/redis-resharding-proxy/badge.png?branch=HEAD
    :target: https://coveralls.io/r/smira/redis-resharding-proxy?branch=HEAD

Redis Resharding Proxy could be used to split (re-shard) instance of Redis into several smaller instances without interrupting
normal operations.

Introduction
------------

.. image:: https://raw.github.com/smira/redis-resharding-proxy/master/redis-resharding.png
    :width: 500px

Resharding is using Redis built-in `replication <http://redis.io/topics/replication>`_ to transfer data from master Redis node
(existing big node) to slave (new smaller node) through special proxy which filters keys in both initial data (RDB) and incremental
updates in real-time.

For example, let's assume that keys in Redis are numeric (``[0-9]+``) distributed evenly. We would like to split it into two parts, so
that 50% of keys goes to first Redis and 50% to another one. So we would set up two redis resharding proxies, one with regular
expression ``^[0-4].*`` and another one with ``^[5-9].*``. Both proxies would be using the same original master Redis as their upstream
master server. We would launch two new Redis instances, making them slaves of respective resharding proxies, replication would start
from master to two new slaves via proxy which would filter keys by regexps splitting original dataset into two halves.

Redis resharding proxy is written in Go and requires no dependencies.

Installing/building
-------------------

If you have Go environment ready::

    go get github.com/smira/redis-resharding-proxy

Otherwise install Go and set up environment::

    $ mkdir $HOME/go
    $ export GOPATH=$HOME/go
    $ export PATH=$PATH:$GOPATH/bin

After that you can run ``redis-resharding-proxy``.

Using
-----

``redis-resharding-proxy`` accepts several options::

  -master-host="localhost": Master Redis host
  -master-port=6379: Master Redis port
  -proxy-host="": Proxy listening interface, default is all interfaces
  -proxy-port=6380: Proxy port for listening

They are used to configure proxy's listening address (which is used in Redis slave to connect to) and master Redis address.

Regular expression is given as the only argument which controls which keys should pass through proxy::

    redis-resharding-proxy --master-host=redis1.srv --proxy-port=5400 '^[a-e].*'

Example
-------

First, let's launch master Redis server::

    redis-server -p 6400

And fill it with some data::

    $ redis-cli -p 6400
    redis 127.0.0.1:6400> set apple red
    OK
    redis 127.0.0.1:6400> set banana yellow
    OK
    redis 127.0.0.1:6400> set cucumber green
    OK
    redis 127.0.0.1:6400>

Then, let's launch slaves::

    redis-server -p 6410
    redis-server -p 6420

And resharding proxies::

    redis-resharding-proxy -master-port=6400 --proxy-port=6401 '^a.*'
    redis-resharding-proxy -master-port=6400 --proxy-port=6402 '^b.*'

First proxy would pass only keys that start with ``a``, second one only keys that start with ``b``.

Then, let's start replication::

    $ redis-cli -p 6410
    redis 127.0.0.1:6410> slaveof localhost 6401
    OK
    redis 127.0.0.1:6410>

And with another slave::

    $ redis-cli -p 6420
    redis 127.0.0.1:6420> slaveof localhost 6402
    OK
    redis 127.0.0.1:6420>

You should see replication progress both in Redis output and resharding proxy log.

Now, we can verify that replication went well::

    $ redis-cli -p 6410
    redis 127.0.0.1:6410> get apple
    "red"
    redis 127.0.0.1:6410> get banana
    (nil)

And with another slave::

    $ redis-cli -p 6420
    redis 127.0.0.1:6420> get apple
    (nil)
    redis 127.0.0.1:6420> get banana
    "yellow"

Let's try to change key on master::

    $ redis-cli -p 6400
    redis 127.0.0.1:6400> set apple blue
    OK

The change would be propagated to slave::

    $ redis-cli -p 6410
    redis 127.0.0.1:6410> get apple
    "blue"

Now, replication could be switched off on slaves, master and proxies shut down. One Redis has been split into two Redises, one with keys
starting with a and another one with keys starting with b.

Performance
-----------

Resharding proxy is filtering RDB approximately 50% slower than Redis itself is loading RDB into memory, so replication may take twice the time
with proxy compared to direct Redis to Redis replication.

Compatibility
-------------

Resharding proxy should be compatible with any Redis version, it has been extensively tested with 2.6.16. When filtering live commands,
only commands which affect one key are supported (that's majority of Redis commands), e.g. ``SET``, ``INCR``, ``LPUSH``, etc. Commands that affect
several keys may lead to unexpected results (like commands ``BITOP``, ``SUNIONSTORE``.)


Thanks
------

I would like to say thanks for ideas and inspiration to Vasiliy Evseenko, Alexander Titov and Alexey Palazhchenko.

Copyright and Licensing
-----------------------

Copyright 2013 Andrey Smirnov. Unless otherwise noted, the source files are distributed under the MIT License found in the LICENSE file.