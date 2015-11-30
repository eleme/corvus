# Corvus

[![Build Status](https://travis-ci.org/eleme/corvus.svg)](https://travis-ci.org/eleme/corvus)

A fast and lightweight Redis Cluster Proxy for Redis 3.0 with cluster mode enabled
focused on Linux >= 3.9. Clients designed for normal mode Redis can still be used
through the proxy. Redirection errors are handled and slot map is cached in proxy.


## Build

Build from source:

```bash
$ make init
$ make
```

Build in debug mode:

```bash
$ make init
$ make debug
```

## Run Proxy

Configuration file:

```conf
# corvus.conf

# binding port
bind 12345

# redis cluster nodes, comma separated list, can supply only one node
node localhost:8000,localhost:8001,localhost:8002

# threads share the same binding port using SO_REUSEPORT
thread 4

# log level, available values: debug, info, warn, error
# if not supplied or wrong value `info` is used
loglevel info

# whether log to syslog
syslog 0
```

Run proxy:

```bash
./corvus corvus.conf
```

## Commands

All single key commands are supported. Commands performing complex multi-key
operations like unions or intersections are available as well as long as
the keys all belong to the same node.

#### Modified commands

- `MGET`: split to multiple `GET`
- `MSET`: split to multiple `SET`
- `DEL`: split to multiple single key `DEL`
- `EXISTS`: split to multiple single key `EXISTS`
- `PING`: not forward to redis server
- `INFO`: not forward to redis server, return information collected in proxy


#### Restricted commands

- `EVAL`: at least one key, if mutiple keys given, all keys should belong to same node


#### All keys should belong to the same node

```
# key
SORT(with STORE)

# list
RPOPLPUSH

# set
SDIFF SDIFFSTORE SINTER SINTERSTORE SMOVE SUNION SUNIONSTORE

# sorted set
ZINTERSTORE ZUNIONSTORE

# hyperloglog
PFCOUNT PFMERGE
```

#### Unsupported commands

```
KEYS MIGRATE MOVE OBJECT RANDOMKEY RENAME RENAMENX SCAN WAIT

# string
BITOP MSETNX

# list
BLPOP BRPOP BRPOPLPUSH

# pub/sub
PSUBSCRIBE PUBLISH PUBSUB PUNSUBSCRIBE SUBSCRIBE UNSUBSCRIBE

# script
EVALSHA SCRIPT

# transaction
DISCARD EXEC MULTI UNWATCH WATCH

# cluster
CLUSTER

AUTH ECHO QUIT SELECT

BGREWRITEAOF BGSAVE CLIENT COMMAND CONFIG DBSIZE DEBUG FLUSHALL FLUSHDB LASTSAVE
MONITOR ROLE SAVE SHUTDOWN SLAVEOF SLOWLOG SYNC TIME
```
