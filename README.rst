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

If you have Go environment ready:

    go get github.com/smira/redis-resharding-proxy

Otherwise install Go and set up environment:

    $ mkdir $HOME/go
    $ export GOPATH=$HOME/go
    $ export PATH=$PATH:$GOPATH/bin

After that you can run ``redis-resharding-proxy``.

Thanks
------

I would like to say thanks for ideas and inspiration to Vasiliy Evseenko, Alexander Titov and Alexey Palazhchenko.

Copyright and Licensing
-----------------------

Copyright 2013 Andrey Smirnov. Unless otherwise noted, the source files are distributed under the MIT License found in the LICENSE file.