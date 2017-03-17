Corvus
======

[![Build Status](https://travis-ci.org/eleme/corvus.svg)](https://travis-ci.org/eleme/corvus)

Corvus is a fast and lightweight redis cluster proxy for redis 3.0 with cluster mode enabled.

Why
---

Most redis client implementations don't support redis cluster. We have a lot of services relying
on redis, which are written in Python, Java, Go, Nodejs etc. It's hard to provide redis client
libraries for multiple languages without breaking compatibilities. We used [twemproxy](https://github.com/twitter/twemproxy)
before, but it relies on sentinel for high availabity, it also requires restarting to add or
remove backend redis instances, which causes service interruption. And twemproxy is single
threaded, we have to deploy multiple twemproxy instances for large number of clients, which
causes the sa headaches.

Therefore, we made corvus.

Features
--------

* Fast.
* Lightweight.
* Painless upgrade to official redis cluster from twemproxy.
* Multiple threading.
* Reuseport support.
* Pipeline support.
* Statsd integration.
* Syslog integration.
* Can direct read operations to slaves.

Performance
------------

See details in our [test results](docs/lpt.md).

Requirements
------------

* Linux >= 3.9
* Redis <= 3.0.7 (For version up to/including 0.2.3)

Build
-----

If use the releases downloaded from
[releases page](https://github.com/eleme/corvus/releases), just make:

```
$ make
```

With debug mode enabled:

```
make debug
```

If build from latest source:
```bash
git clone https://github.com/eleme/corvus.git
cd corvus
git submodule update --init
make deps # need autoconf
make
```

Binary can be found at `./src/corvus`.

Configuration
-------------

Example configuration file is at [corvus.conf](corvus.conf).

Usage
-----

```bash
$ ./src/corvus path/to/corvus.conf
```

Commands
--------

* All single-key commands (like `SET`, `GET`, `INCR`..) are supported.
* Batch commands are split into multiple single-key commands.
* Commands performing complex multi-key operations like unions or intersections
   are available as well as long as the keys all belong to the same node.

#### Modified commands

* `MGET`: split to multiple `GET`.
* `MSET`: split to multiple `SET`.
* `DEL`: split to multiple single key `DEL`.
* `EXISTS`: split to multiple single key `EXISTS`.
* `PING`: ignored and won't be forwarded.
* `INFO`, `TIME`: won't be forwarded to backend redis, information collected in proxy
   will be returned.
* `SLOWLOG`: return the slowlogs saved by corvus itself. Note that unlike redis,
   in the slowlog entry there's an additional `remote latency` field before
   the `total latency` field. The slowlog will also log the slowest
   sub command for multiple-key commands: `MGET`, `MSET`, `DEL`, `EXISTS`.
   The total latency of sub cmd entry will just be the same as its parent cmd.
* `AUTH`: do authentication in proxy.
* `CONFIG`: support `get`, `set`, and `rewrite` sub-command to retrieve and manipulate corvus config.
* `SELECT`: ignored if index is `0`, won't be forwarded.

#### Restricted commands

* `EVAL`: at least one key should be given. If there are multiple keys, all of
   them should belong to the same node.

The following commands require all argument keys to belong to the same redis node:

* `SORT`.
* `RPOP`, `LPUSH`.
* `SDIFF`, `SDIFFSTORE`, `SINTER`, `SINTERSTORE`, `SMOVE`, `SUNION`, `SUNIONSTORE`.
* `ZINTERSTORE`, `ZUNIONSTORE`.
* `PFCOUNTE`, `PFMERGE`.

#### Unsupported commands

The following commands are not available, such as `KEYS`, we can't search keys across
all backend redis instances.

* `KEYS`, `MIGRATE`, `MOVE`, `OBJECT`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `SCAN`, `WAIT`.
* `BITOP`, `MSETNX`
* `BLPOP`, `BRPOP`, `BRPOPLPUSH`.
* `PSUBSCRIBE`, `PUBLISH`, `PUBSUB`, `PUNSUBSCRIBE`, `SUBSCRIBE`, `UNSUBSCRIBE`.
* `EVALSHA`, `SCRIPT`.
* `DISCARD`, `EXEC`, `MULTI`, `UNWATCH`, `WATCH`.
* `CLUSTER`.
* `ECHO`, `QUIT`.
* `BGREWRITEAOF`, `BGSAVE`, `CLIENT`, `COMMAND`, `CONFIG`, `DBSIZE`, `DEBUG`, `FLUSHALL`,
   `FLUSHDB`, `LASTSAVE`, `MONITOR`, `ROLE`, `SAVE`, `SHUTDOWN`, `SLAVEOF`, `SYNC`.

License
-------

MIT. Copyright (c) 2016 Eleme Inc.

See [LICENSE](LICENSE) for details.
