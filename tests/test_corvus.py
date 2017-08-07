# -*- coding: utf-8 -*-

import pytest
import redis
import time

from ruskit.cluster import Cluster, ClusterNode

SLOT = 866
PROXY_PORT = 12345
REDIS_URI_SRC = "redis://localhost:8000"
REDIS_URI_DST = "redis://localhost:8001"


class Redis(redis.Redis):
    def decr(self, name):
        return self.execute_command('DECR', name)

    def decrby(self, name, amount):
        return self.execute_command('DECRBY', name, amount)

    def incr(self, name):
        return self.execute_command('INCR', name)

    def incrby(self, name, amount):
        return self.execute_command('INCRBY', name, amount)

    def hstrlen(self, name, key):
        return self.execute_command("HSTRLEN", name, key)

    def quit(self):
        return self.execute_command("QUIT")

    def select(self, dbnum):
        return self.execute_command("SELECT", dbnum)

    def exists(self, *names):
        return self.execute_command('EXISTS', *names)

    def zrevrangebylex(self, key, *args):
        return self.execute_command('ZREVRANGEBYLEX', key, *args)

    def pfcount(self, *args):
        return self.execute_command('PFCOUNT', *args)

    def auth(self, password):
        return self.execute_command('AUTH', password)


r = Redis(port=PROXY_PORT)


@pytest.fixture
def delete_keys(request):
    _keys = []
    class _O(object):
        def keys(self, *args):
            _keys.extend(args)

    def fin():
        print("delete", _keys);
        r.delete(*_keys)
        _keys[:] = []
    request.addfinalizer(fin)
    return _O()


@pytest.fixture
def moved(request):
    def fin():
        try:
            Cluster.from_node(ClusterNode.from_uri(REDIS_URI_SRC)) \
                .migrate_slot(ClusterNode.from_uri(REDIS_URI_DST),
                              ClusterNode.from_uri(REDIS_URI_SRC), SLOT)
        except redis.RedisError:
            pass
    request.addfinalizer(fin)


@pytest.fixture
def asked(request):
    def fin():
        for uri in (REDIS_URI_DST, REDIS_URI_SRC):
            ClusterNode.from_uri(uri).setslot("STABLE", 866)
    request.addfinalizer(fin)


def test_null_key(delete_keys):
    delete_keys.keys('')

    assert r.set('', 1) is True
    assert r.get('') == '1'


def test_del(delete_keys):
    """ DEL key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of keys that will be
            removed. When a key to remove holds a value other than a string,
            the individual complexity for this key is O(M) where M is the
            number of elements in the list, set, sorted set or hash. Removing
            a single key that holds a string value is O(1).

    redis> SET key1 "Hello"
    OK
    redis> SET key2 "World"
    OK
    redis> DEL key1 key2 key3
    (integer) 2
    redis>
    """
    delete_keys.keys("key1", "key2", "key3")

    assert r.set("key1", "Hello") is True
    assert r.set("key2", "World") is True
    assert r.delete("key1", "key2", "key3") == 2


def test_dump(delete_keys):
    """ DUMP key
            Available since 2.6.0.
            Time complexity: O(1) to access the key and additional O(N*M) to
            serialized it, where N is the number of Redis objects composing the
            value and M their average size. For small string values the time
            complexity is thus O(1)+O(1*M) where M is small, so simply O(1).

    redis> SET mykey 10
    OK
    redis> DUMP mykey
    "\u0000\xC0\n\a\u0000\x91\xAD\x82\xB6\u0006d\xB6\xA1"
    redis>
    """
    delete_keys.keys("mykey")
    node = ClusterNode.from_uri(REDIS_URI_SRC)
    slot = node.execute_command("CLUSTER KEYSLOT", "mykey")
    cluster = Cluster.from_node(node)
    node = next((n for n in cluster.nodes if slot in n.slots), None)
    assert node

    assert r.set("mykey", 10) is True
    assert r.dump("mykey") == node.dump("mykey")


def test_exists(delete_keys):
    """ EXISTS key [key ...]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET key1 "Hello"
    OK
    redis> EXISTS key1
    (integer) 1
    redis> EXISTS nosuchkey
    (integer) 0
    redis> SET key2 "World"
    OK
    redis> EXISTS key1 key2 nosuchkey
    (integer) 2
    redis>
    """
    delete_keys.keys("key1", "nosuchkey", "key2")

    assert r.set("key1", "Hello") is True
    assert r.exists("key1") == 1
    assert r.exists("nosuchkey") == 0
    assert r.set("key2", "World") is True
    assert r.exists("key1", "key2", "nosuchkey")


def test_expire(delete_keys):
    """ EXPIRE key seconds
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> EXPIRE mykey 10
    (integer) 1
    redis> TTL mykey
    (integer) 10
    redis> SET mykey "Hello World"
    OK
    redis> TTL mykey
    (integer) -1
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10
    assert r.set("mykey", "Hello World") is True
    assert r.ttl("mykey") is None


def test_expireat(delete_keys):
    """ EXPIREAT key timestamp
            Available since 1.2.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> EXISTS mykey
    (integer) 1
    redis> EXPIREAT mykey 1293840000
    (integer) 1
    redis> EXISTS mykey
    (integer) 0
    redis>
    """
    delete_keys.keys("mykey")
    assert r.set("mykey", "Hello") is True
    assert r.exists("mykey") is True
    assert r.expireat("mykey", 1293840000) == 1
    assert r.exists("mykey") is False


# def test_keys():
#     """ KEYS pattern
#             Available since 1.0.0.
#             Time complexity: O(N) with N being the number of keys in the
#             database, under the assumption that the key names in the database
#             and the given pattern have limited length.
#
#     redis> MSET one 1 two 2 three 3 four 4
#     OK
#     redis> KEYS *o*
#     1) "two"
#     2) "four"
#     3) "one"
#     redis> KEYS t??
#     1) "two"
#     redis> KEYS *
#     1) "two"
#     2) "four"
#     3) "three"
#     4) "one"
#     redis>
#     """
#     assert r.mset({"one": 1, "two": 2, "three": 3, "four": 4}) is True
#     assert r.keys("*o*") == ["two", "four", "one"]
#     assert r.keys("t??") == ["two"]
#     assert r.keys("*") == ["two", "three", "four", "one"]


# def test_migrate():
#     """ MIGRATE host port key destination-db timeout [COPY] [REPLACE]
#             Available since 2.6.0.
#             Time complexity: This command actually executes a DUMP+DEL in the
#             source instance, and a RESTORE in the target instance. See the
#             pages of these commands for time complexity. Also an O(N) data
#             transfer between the two instances is performed.
#     """


# def test_move():
#     """ MOVE key db
#             Available since 1.0.0.
#             Time complexity: O(1)
#     """


# def test_object():
#     """ OBJECT subcommand [arguments [arguments ...]]
#             Available since 2.2.3.
#             Time complexity: O(1) for all the currently implemented subcommands.
#
#     # 1
#     redis> lpush mylist "Hello World"
#     (integer) 4
#     redis> object refcount mylist
#     (integer) 1
#     redis> object encoding mylist
#     "ziplist"
#     redis> object idletime mylist
#     (integer) 10
#
#     # 2
#     redis> set foo 1000
#     OK
#     redis> object encoding foo
#     "int"
#     redis> append foo bar
#     (integer) 7
#     redis> get foo
#     "1000bar"
#     redis> object encoding foo
#     "raw"
#     """
#     assert r.lpush("mylist", "Hello World") == 1
#     assert r.object("refcount", "mylist")  == 1
#     assert r.object("encoding", "mylist") == "ziplist"
#     assert r.object("idletime", "mylist") == 0


def test_persist(delete_keys):
    """ PERSIST key
            Available since 2.2.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> EXPIRE mykey 10
    (integer) 1
    redis> TTL mykey
    (integer) 10
    redis> PERSIST mykey
    (integer) 1
    redis> TTL mykey
    (integer) -1
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10
    assert r.persist("mykey") == 1
    assert r.ttl("mykey") is None


def test_pexpire(delete_keys):
    """ PEXPIRE key milliseconds
            Available since 2.6.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> PEXPIRE mykey 1500
    (integer) 1
    redis> TTL mykey
    (integer) 1
    redis> PTTL mykey
    (integer) 1498
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.pexpire("mykey", 1500) == 1
    assert r.ttl("mykey")


def test_pexpireat(delete_keys):
    """ PEXPIREAT key milliseconds-timestamp
            Available since 2.6.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> PEXPIREAT mykey 1555555555005
    (integer) 1
    redis> TTL mykey
    (integer) 107040076
    redis> PTTL mykey
    (integer) 107040075951
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.pexpireat("mykey", 1555555555005) == 1
    assert r.ttl("mykey")
    assert r.pttl("mykey")


def test_pttl(delete_keys):
    """ PTTL key
            Available since 2.6.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> EXPIRE mykey 1
    (integer) 1
    redis> PTTL mykey
    (integer) 999
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 1) == 1
    assert r.pttl("mykey")


# def test_randomkey():
#     """ RANDOMKEY
#             Available since 1.0.0.
#             Time complexity: O(1)
#     """


# def test_rename():
#     """ RENAME key newkey
#             Available since 1.0.0.
#             Time complexity: O(1)
#
#     redis> SET mykey "Hello"
#     OK
#     redis> RENAME mykey myotherkey
#     OK
#     redis> GET myotherkey
#     "Hello"
#     redis>
#     """
#     assert r.set("mykey", "Hello") is True
#     assert r.rename("mykey", "myotherkey") is True
#     assert r.get("myotherkey") == "Hello"


# def test_renamenx():
#     """ RENAMENX key newkey
#             Available since 1.0.0.
#             Time complexity: O(1)
#
#     redis> SET mykey "Hello"
#     OK
#     redis> SET myotherkey "World"
#     OK
#     redis> RENAMENX mykey myotherkey
#     (integer) 0
#     redis> GET myotherkey
#     "World"
#     redis>
#     """
#     assert r.set("mykey", "Hello") is True
#     assert r.set("myotherkey", "World") is True
#     assert r.renamenx("mykey", "myotherkey") is False
#     assert r.get("myotherkey") == "World"


def test_restore(delete_keys):
    """ RESTORE key ttl serialized-value [REPLACE]
            Available since 2.6.0.
            Time complexity: O(1) to create the new key and additional O(N*M)
            to reconstruct the serialized value, where N is the number of Redis
            objects composing the value and M their average size. For small
            string values the time complexity is thus O(1)+O(1*M) where M is
            small, so simply O(1). However for sorted set values the complexity
            is O(N*M*log(N)) because inserting values into sorted sets
            is O(log(N)).

    redis> DEL mykey
    0
    redis> RESTORE mykey 0 "\n\x17\x17\x00\x00\x00\x12\x00\x00\x00\x03\x00\
                            x00\xc0\x01\x00\x04\xc0\x02\x00\x04\xc0\x03\x00\
                            xff\x04\x00u#<\xc0;.\xe9\xdd"
    OK
    redis> TYPE mykey
    list
    redis> LRANGE mykey 0 -1
    1) "1"
    2) "2"
    3) "3"
    """
    delete_keys.keys("mykey")

    res = "\n\x11\x11\x00\x00\x00\x0e\x00\x00\x00\x03\x00\x00\xf2\x02\xf3\x02\xf4\xff\x06\x00Z1_\x1cg\x04!\x18"
    assert r.restore("mykey", 0, res) == "OK"
    assert r.type("mykey") == "list"
    assert r.lrange("mykey", 0, -1) == ["1", "2", "3"]


def test_sort(delete_keys):
    """ SORT key [BY pattern] [LIMIT offset count]
        [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
            Available since 1.0.0.
            Time complexity: O(N+M*log(M)) where N is the number of elements in
            the list or set to sort, and M the number of returned elements.
            When the elements are not sorted, complexity is currently O(N) as
            there is a copy step that will be avoided in next releases.

    redis> RPUSH mylist 2 1 3
    (integer) 3
    redis> LRANGE mylist 0 -1
    1) "2"
    2) "1"
    3) "3"
    redis> SORT mylist
    1) "1"
    2) "2"
    3) "3"
    redis> SORT mylist DESC STORE myotherlist
    (integer) 3
    127.0.0.1:6379> LRANGE myotherlist 0 -1
    1) "3"
    2) "2"
    3) "1"
    127.0.0.1:6379>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", 2, 1, 3) == 3
    assert r.lrange("mylist", 0, -1) == ["2", "1", "3"]
    assert r.sort("mylist") == ["1", "2", "3"]
    #assert r.sort("mylist", desc=True, store="myotherlist") == 3
    #assert r.lrange("myotherlist", 0, -1) == ["3", "2", "1"]


def test_ttl(delete_keys):
    """ TTL key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> EXPIRE mykey 10
    (integer) 1
    redis> TTL mykey
    (integer) 10
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10


def test_type(delete_keys):
    """ TYPE key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET key1 "value"
    OK
    redis> LPUSH key2 "value"
    (integer) 1
    redis> SADD key3 "value"
    (integer) 1
    redis> TYPE key1
    string
    redis> TYPE key2
    list
    redis> TYPE key3
    set
    redis>
    """
    delete_keys.keys("key1", "key2", "key3")

    assert r.set("key1", "value") is True
    assert r.lpush("key2", "value") == 1
    assert r.sadd("key3", "value") == 1
    assert r.type("key1") == "string"
    assert r.type("key2") == "list"
    assert r.type("key3") == "set"


# def test_wait():
#     """ WAIT numslaves timeout
#             Available since 3.0.0.
#             Time complexity: O(1)
#     """


# def test_scan():
#     """ SCAN cursor [MATCH pattern] [COUNT count]
#             Available since 2.8.0.
#             Time complexity: O(1) for every call. O(N) for a complete
#             iteration, including enough command calls for the cursor to return
#             back to 0. N is the number of elements inside the collection.
#     """



###strings
def test_append(delete_keys):
    """ APPEND key value
            Available since 2.0.0.
            Time complexity: O(1). The amortized time complexity is O(1)
            assuming the appended value is small and the already present
            value is of any size, since the dynamic string library used by
            Redis will double the free space available on every reallocation.

    # 1
    redis> EXISTS mykey
    (integer) 0
    redis> APPEND mykey "Hello"
    (integer) 5
    redis> APPEND mykey " World"
    (integer) 11
    redis> GET mykey
    "Hello World"
    redis>

    # 2
    redis> APPEND ts "0043"
    (integer) 4
    redis> APPEND ts "0035"
    (integer) 8
    redis> GETRANGE ts 0 3
    "0043"
    redis> GETRANGE ts 4 7
    "0035"
    redis>
    """
    delete_keys.keys("mykey", "ts")

    assert r.exists("mykey") == 0
    assert r.append("mykey", "Hello") == 5
    assert r.append("mykey", " World") == 11
    assert r.get("mykey") == "Hello World"

    assert r.append("ts", "0043") == 4
    assert r.append("ts", "0035") == 8
    assert r.getrange("ts", 0, 3) == "0043"
    assert r.getrange("ts", 4, 7) == "0035"


def test_bitcount(delete_keys):
    """ BITCOUNT key [start end]
            Available since 2.6.0.
            Time complexity: O(N)

    redis> SET mykey "foobar"
    OK
    redis> BITCOUNT mykey
    (integer) 26
    redis> BITCOUNT mykey 0 0
    (integer) 4
    redis> BITCOUNT mykey 1 1
    (integer) 6
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "foobar") is True
    assert r.bitcount("mykey") == 26
    assert r.bitcount("mykey", 0, 0) == 4
    assert r.bitcount("mykey", 1, 1) == 6


# def test_bitop():
#     """ BITOP operation destkey key [key ...]
#             Available since 2.6.0.
#             Time complexity: O(N)
#
#     redis> SET key1 "foobar"
#     OK
#     redis> SET key2 "abcdef"
#     OK
#     redis> BITOP AND dest key1 key2
#     (integer) 6
#     redis> GET dest
#     "`bc`ab"
#     redis>
#     """
#     assert r.set("key1", "foobar") is True
#     assert r.set("key2", "abcdef") is True
#     assert r.bitop("AND", "dest", "key1", "key2") == 6
#     assert r.get("dest") == "`bc`ab"


def test_bitpos(delete_keys):
    """ BITPOS key bit [start] [end]
            Available since 2.8.7.
            Time complexity: O(N)

    redis> SET mykey "\xff\xf0\x00"
    OK
    redis> BITPOS mykey 0
    (integer) 12
    redis> SET mykey "\x00\xff\xf0"
    OK
    redis> BITPOS mykey 1 0
    (integer) 8
    redis> BITPOS mykey 1 2
    (integer) 16
    redis> set mykey "\x00\x00\x00"
    OK
    redis> BITPOS mykey 1
    (integer) -1
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "\xff\xf0\x00") is True
    assert r.bitpos("mykey", 0) == 12
    assert r.set("mykey", "\x00\xff\xf0") is True
    assert r.bitpos("mykey", 1, 0) == 8
    assert r.bitpos("mykey", 1, 2) == 16
    assert r.set("mykey", "\x00\x00\x00") is True
    assert r.bitpos("mykey", 1) == -1


def test_decr(delete_keys):
    """ DECR key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> DECR mykey
    (integer) 9
    redis> SET mykey "234293482390480948029348230948"
    OK
    redis> DECR mykey
    ERR value is not an integer or out of range
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", 10) is True
    assert r.decr("mykey") == 9
    #assert r.set("mykey", )


def test_decrby(delete_keys):
    """ DECRBY key decrement
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> DECRBY mykey 3
    (integer) 7
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", 10) is True
    assert r.decrby("mykey", 3) == 7


def test_get(delete_keys):
    """ GET key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> GET nonexisting
    (nil)
    redis> SET mykey "Hello"
    OK
    redis> GET mykey
    "Hello"
    redis>
    """
    delete_keys.keys("nonexisting", "mykey")

    assert r.get("nonexisting") is None
    assert r.set("mykey", "Hello") is True
    assert r.get("mykey") == "Hello"


def test_getbit(delete_keys):
    """ GETBIT key offset
            Available since 2.2.0.
            Time complexity: O(1)

    redis> SETBIT mykey 7 1
    (integer) 0
    redis> GETBIT mykey 0
    (integer) 0
    redis> GETBIT mykey 7
    (integer) 1
    redis> GETBIT mykey 100
    (integer) 0
    redis>
    """
    delete_keys.keys("mykey")

    assert r.setbit("mykey", 7, 1) == 0
    assert r.getbit("mykey", 0) == 0
    assert r.getbit("mykey", 7) == 1
    assert r.getbit("mykey", 100) == 0


def test_getrange(delete_keys):
    """ GETRANGE key start end
            Available since 2.4.0.
            Time complexity: O(N) where N is the length of the returned string.
            The complexity is ultimately determined by the returned length,
             but because creating a substring from an existing string is very
             cheap, it can be considered O(1) for small strings.

    redis> SET mykey "This is a string"
    OK
    redis> GETRANGE mykey 0 3
    "This"
    redis> GETRANGE mykey -3 -1
    "ing"
    redis> GETRANGE mykey 0 -1
    "This is a string"
    redis> GETRANGE mykey 10 100
    "string"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "This is a string") is True
    assert r.getrange("mykey", 0, 3) == "This"
    assert r.getrange("mykey", -3, -1) == "ing"
    assert r.getrange("mykey", 0, -1) == "This is a string"
    assert r.getrange("mykey", 10, 100) == "string"


def test_getset(delete_keys):
    """ GETSET key value
            Available since 1.0.0.
            Time complexity: O(1)

    # 1
    redis> INCR mycounter
    (integer) 1
    redis> GETSET mycounter "0"
    "1"
    redis> GET mycounter
    "0"
    redis>

    # 2
    redis> SET mykey "Hello"
    OK
    redis> GETSET mykey "World"
    "Hello"
    redis> GET mykey
    "World"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.getset("mykey", "World") == "Hello"
    assert r.get("mykey") == "World"


def test_incr(delete_keys):
    """ INCR key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> INCR mykey
    (integer) 11
    redis> GET mykey
    "11"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", 10) is True
    assert r.incr("mykey") == 11
    assert r.get("mykey") == "11"


def test_incrby(delete_keys):
    """ INCRBY key increment
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> INCRBY mykey 5
    (integer) 15
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", 10) is True
    assert r.incrby("mykey", 5) == 15


def test_incrbyfloat(delete_keys):
    """ INCRBYFLOAT key increment
            Available since 2.6.0.
            Time complexity: O(1)

    redis> SET mykey 10.50
    OK
    redis> INCRBYFLOAT mykey 0.1
    "10.6"
    redis> SET mykey 5.0e3
    OK
    redis> INCRBYFLOAT mykey 2.0e2
    "5200"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "10.50") is True
    assert r.incrbyfloat("mykey", "0.1") == 10.6
    assert r.set("mykey", "5.0e3") is True
    assert r.incrbyfloat("mykey", "2.0e2") == 5200


def test_mget(delete_keys):
    """ MGET key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of keys to retrieve.

    redis> SET key1 "Hello"
    OK
    redis> SET key2 "World"
    OK
    redis> MGET key1 key2 nonexisting
    1) "Hello"
    2) "World"
    3) (nil)
    redis>
    """
    delete_keys.keys("key1", "key2", "nonexisting")

    assert r.set("key1", "Hello") is True
    assert r.set("key2", "World") is True
    assert r.mget("key1", "key2", "nonexisting") == ["Hello", "World", None]


def test_mset(delete_keys):
    """ MSET key value [key value ...]
            Available since 1.0.1.
            Time complexity: O(N) where N is the number of keys to set.

    redis> MSET key1 "Hello" key2 "World"
    OK
    redis> GET key1
    "Hello"
    redis> GET key2
    "World"
    redis>
    """
    delete_keys.keys("key1", "key2")

    assert r.mset({"key1": "Hello", "key2": "World"}) is True
    assert r.get("key1") == "Hello"
    assert r.get("key2") == "World"


#def test_msetnx():
    """ MSETNX key value [key value ...]
            Available since 1.0.1.
            Time complexity: O(N) where N is the number of keys to set.

    redis> MSETNX key1 "Hello" key2 "there"
    (integer) 1
    redis> MSETNX key2 "there" key3 "world"
    (integer) 0
    redis> MGET key1 key2 key3
    1) "Hello"
    2) "there"
    3) (nil)
    redis>
    """
#    assert r.msetnx({"key1": "Hello", "key2": "there"}) == 1
#    assert r.msetnx({"key2": "there", "key3": "world"}) == 0
#    assert r.mget("key1", "key2", "key3") == ["Hello", "there", None]


def test_psetex(delete_keys):
    """ PSETEX key milliseconds value
            Available since 2.6.0.
            Time complexity: O(1)

    redis> PSETEX mykey 1000 "Hello"
    OK
    redis> PTTL mykey
    (integer) 999
    redis> GET mykey
    "Hello"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.psetex("mykey", 1000, "Hello") is True
    assert r.pttl("mykey") <= 1000
    assert r.get("mykey") == "Hello"


def test_set(delete_keys):
    """ SET key value [EX seconds] [PX milliseconds] [NX|XX]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> GET mykey
    "Hello"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.set("mykey", "Hello") is True
    assert r.get("mykey") == "Hello"


def test_setbit(delete_keys):
    """ SETBIT key offset value
            Available since 2.2.0.
            Time complexity: O(1)

    redis> SETBIT mykey 7 1
    (integer) 0
    redis> SETBIT mykey 7 0
    (integer) 1
    redis> GET mykey
    "\u0000"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.setbit("mykey", 7, 1) == 0
    assert r.setbit("mykey", 7, 0) == 1
    assert r.get("mykey") == "\x00"


def test_setex(delete_keys):
    """ SETEX key seconds value
            Available since 2.0.0.
            Time complexity: O(1)

    redis> SETEX mykey 10 "Hello"
    OK
    redis> TTL mykey
    (integer) 10
    redis> GET mykey
    "Hello"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.setex("mykey", "Hello", 10)            #
    assert r.ttl("mykey") == 10
    assert r.get("mykey") == "Hello"


def test_setnx(delete_keys):
    """ SETNX key value
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SETNX mykey "Hello"
    (integer) 1
    redis> SETNX mykey "World"
    (integer) 0
    redis> GET mykey
    "Hello"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.setnx("mykey", "Hello") == 1
    assert r.setnx("mykey", "World") == 0
    assert r.get("mykey") == "Hello"


def test_setrange(delete_keys):
    """ SETRANGE key offset value
            Available since 2.2.0.
            Time complexity: O(1), not counting the time taken to copy the new
            string in place. Usually, this string is very small so the
            amortized complexity is O(1). Otherwise, complexity is O(M)
            with M being the length of the value argument.

    # 1
    redis> SET key1 "Hello World"
    OK
    redis> SETRANGE key1 6 "Redis"
    (integer) 11
    redis> GET key1
    "Hello Redis"
    redis>

    # 2
    redis> SETRANGE key2 6 "Redis"
    (integer) 11
    redis> GET key2
    "\u0000\u0000\u0000\u0000\u0000\u0000Redis"
    redis>
    """
    delete_keys.keys("key1")

    assert r.set("key1", "Hello World") is True
    assert r.setrange("key1", 6, "Redis") == 11
    assert r.get("key1") == "Hello Redis"


def test_strlen(delete_keys):
    """ STRLEN key
            Available since 2.2.0.
            Time complexity: O(1)

    redis> SET mykey "Hello world"
    OK
    redis> STRLEN mykey
    (integer) 11
    redis> STRLEN nonexisting
    (integer) 0
    redis>
    """
    delete_keys.keys("mykey", "nonexisting")

    assert r.set("mykey", "Hello world") is True
    assert r.strlen("mykey") == 11
    assert r.strlen("nonexisting") == 0


###hashes
def test_hdel(delete_keys):
    """ HDEL key field [field ...]

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HDEL myhash field1
    (integer) 1
    redis> HDEL myhash field2
    (integer) 0
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hdel("myhash", "field1") == 1
    assert r.hdel("myhash", "field2") == 0


def test_hexists(delete_keys):
    """ HEXISTS key field

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HEXISTS myhash field1
    (integer) 1
    redis> HEXISTS myhash field2
    (integer) 0
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hexists("myhash", "field1") == 1
    assert r.hexists("myhash", "field2") == 0


def test_hget(delete_keys):
    """ HGET key field

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HGET myhash field1
    "foo"
    redis> HGET myhash field2
    (nil)
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hget("myhash", "field1") == "foo"
    assert r.hget("myhash", "field2") is None


def test_hgetall(delete_keys):
    """ HGETALL key

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HGETALL myhash
    1) "field1"
    2) "Hello"
    3) "field2"
    4) "World"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hgetall("myhash") == {"field1": "Hello", "field2": "World"}


def test_hincrby(delete_keys):
    """ HINCRBY key field increment

    redis> HSET myhash field 5
    (integer) 1
    redis> HINCRBY myhash field 1
    (integer) 6
    redis> HINCRBY myhash field -1
    (integer) 5
    redis> HINCRBY myhash field -10
    (integer) -5
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field", 5) == 1
    assert r.hincrby("myhash", "field", 1) == 6
    assert r.hincrby("myhash", "field", -1) == 5
    assert r.hincrby("myhash", "field", -10) == -5


def test_hincrbyfloat(delete_keys):
    """ HINCRBYFLOAT key field increment

    redis> HSET mykey field 10.50
    (integer) 1
    redis> HINCRBYFLOAT mykey field 0.1
    "10.60000000000000001"
    redis> HSET mykey field 5.0e3
    (integer) 0
    redis> HINCRBYFLOAT mykey field 2.0e2
    "5200"
    redis>
    """
    delete_keys.keys("mykey")

    assert r.hset("mykey", "field", 10.50) == 1
    assert r.hincrbyfloat("mykey", "field", 0.1) == 10.6
    assert r.hset("mykey", "field", "5.0e3") == 0
    assert r.hincrbyfloat("mykey", "field", "2.0e2") == 5200.0


def test_hkeys(delete_keys):
    """ HKEYS key

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HKEYS myhash
    1) "field1"
    2) "field2"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hkeys("myhash") == ["field1", "field2"]


def test_hlen(delete_keys):
    """ HLEN key

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HLEN myhash
    (integer) 2
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hlen("myhash") == 2


def test_hmget(delete_keys):
    """ HMGET key field [field ...]

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HMGET myhash field1 field2 nofield
    1) "Hello"
    2) "World"
    3) (nil)
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hmget("myhash", "field1", "field2", "field3") == ["Hello", "World", None]


def test_hmset(delete_keys):
    """ HMSET key field value [field value ...]

    redis> HMSET myhash field1 "Hello" field2 "World"
    OK
    redis> HGET myhash field1
    "Hello"
    redis> HGET myhash field2
    "World"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hmset("myhash", {"field1": "Hello", "field2": "World"}) is True


def test_hset(delete_keys):
    """ HSET key field value

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HGET myhash field1
    "Hello"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1    # tes
    assert r.hget("myhash", "field1") == "Hello"


def test_hsetnx(delete_keys):
    """ HSETNX key field value

    redis> HSETNX myhash field "Hello"
    (integer) 1
    redis> HSETNX myhash field "World"
    (integer) 0
    redis> HGET myhash field
    "Hello"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hsetnx("myhash", "field", "Hello") == 1
    assert r.hsetnx("myhash", "field", "World") == 0
    assert r.hget("myhash", "field") == "Hello"


# def test_hstrlen():
#     """ HSTRLEN key field
#             Available since 3.2.0.
#             Time complexity: O(1)
#
#     redis> HMSET myhash f1 HelloWorld f2 99 f3 -256
#     OK
#     redis> HSTRLEN myhash f1
#     (integer) 10
#     redis> HSTRLEN myhash f2
#     (integer) 2
#     redis> HSTRLEN myhash f3
#     (integer) 4
#     redis>
#     """
#     assert r.hmset("myhash", {"f1": "HelloWorld", "f2": "99", "f3": -256}) == True
#     assert r.hstrlen("myhash", "f1") == 10
#     assert r.hstrlen("myhash", "f2") == 2
#     assert r.hstrlen("myhash", "f3") == 4


def test_hvals(delete_keys):
    """ HVALS key
            Available since 2.0.0.
            Time complexity: O(N) where N is the size of the hash.

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HVALS myhash
    1) "Hello"
    2) "World"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hvals("myhash") == ["Hello", "World"]


def test_hscan(delete_keys):
    """ HSCAN key cursor [MATCH pattern] [COUNT count]
            Available since 2.8.0.
            Time complexity: O(1) for every call. O(N) for a complete
            iteration, including enough command calls for the cursor to return
            back to 0. N is the number of elements inside the collection..

    redis> HMSET myhash field1 Hello field2 World
    OK
    redis> HSCAN myhash 0
    1) "0"
    2) 1) "field1"
       2) "Hello"
       3) "field2"
       4) "World"
    redis>
    """
    delete_keys.keys("myhash")

    assert r.hmset("myhash", {"field1": "Hello", "field2": "World"}) is True
    assert r.hscan("myhash", 0) == (0, {"field1": "Hello", "field2": "World"})



###lists
# def test_blpop():
#     """ BLPOP key [key ...] timeout
#             Available since 2.0.0.
#             Time complexity: O(1)
#     """


# def test_brpop():
#     """ BRPOP key [key ...] timeout
#             Available since 2.0.0.
#             Time complexity: O(1)
#
#     redis> DEL list1 list2
#     (integer) 0
#     redis> RPUSH list1 a b c
#     (integer) 3
#     redis> BRPOP list1 list2 0
#     1) "list1"
#     2) "c"
#     """


# def test_brpoplpush():
#     """ BRPOPLPUSH source destination timeout
#         Available since 2.2.0.
#         Time complexity: O(1)
#     """


def test_lindex(delete_keys):
    """ LINDEX key index
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of elements to
            traverse to get to the element at index. This makes asking for
            the first or the last element of the list O(1).

    redis> LPUSH mylist "World"
    (integer) 1
    redis> LPUSH mylist "Hello"
    (integer) 2
    redis> LINDEX mylist 0
    "Hello"
    redis> LINDEX mylist -1
    "World"
    redis> LINDEX mylist 3
    (nil)
    redis>
    """
    delete_keys.keys("mylist")

    assert r.lpush("mylist", "World") == 1
    assert r.lpush("mylist", "Hello") == 2
    assert r.lindex("mylist", 0) == "Hello"
    assert r.lindex("mylist", -1) == "World"
    assert r.lindex("mylist", 3) is None


def test_linsert(delete_keys):
    """ LINSERT key BEFORE|AFTER pivot value
            Available since 2.2.0.
            Time complexity: O(N) where N is the number of elements to traverse
            before seeing the value pivot. This means that inserting somewhere
            on the left end on the list (head) can be considered O(1) and
            inserting somewhere on the right end (tail) is O(N).

    redis> RPUSH mylist "Hello"
    (integer) 1
    redis> RPUSH mylist "World"
    (integer) 2
    redis> LINSERT mylist BEFORE "World" "There"
    (integer) 3
    redis> LRANGE mylist 0 -1
    1) "Hello"
    2) "There"
    3) "World"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "Hello") == 1
    assert r.rpush("mylist", "World") == 2
    assert r.linsert("mylist", "BEFORE", "World", "There") == 3
    assert r.lrange("mylist", 0, -1) == ["Hello", "There", "World"]


def test_llen(delete_keys):
    """ LLEN key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> LPUSH mylist "World"
    (integer) 1
    redis> LPUSH mylist "Hello"
    (integer) 2
    redis> LLEN mylist
    (integer) 2
    redis>
    """
    delete_keys.keys("mylist")

    assert r.lpush("mylist", "World") == 1
    assert r.lpush("mylist", "Hello") == 2
    assert r.llen("mylist") == 2


def test_lpop(delete_keys):
    """ LPOP key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> LPOP mylist
    "one"
    redis> LRANGE mylist 0 -1
    1) "two"
    2) "three"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lpop("mylist") == "one"
    assert r.lrange("mylist", 0, -1) == ["two", "three"]


def test_lpush(delete_keys):
    """ LPUSH key value [value ...]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> LPUSH mylist "world"
    (integer) 1
    redis> LPUSH mylist "hello"
    (integer) 2
    redis> LRANGE mylist 0 -1
    1) "hello"
    2) "world"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.lpush("mylist", "world") == 1
    assert r.lpush("mylist", "hello") == 2
    assert r.lrange("mylist", 0, -1) == ["hello", "world"]


def test_lpushx(delete_keys):
    """ LPUSHX key value
            Available since 2.2.0.
            Time complexity: O(1)

    redis> LPUSH mylist "World"
    (integer) 1
    redis> LPUSHX mylist "Hello"
    (integer) 2
    redis> LPUSHX myotherlist "Hello"
    (integer) 0
    redis> LRANGE mylist 0 -1
    1) "Hello"
    2) "World"
    redis> LRANGE myotherlist 0 -1
    (empty list or set)
    redis>
    """
    delete_keys.keys("mylist", "myotherlist")

    assert r.lpush("mylist", "World") == 1
    assert r.lpushx("mylist", "Hello") == 2
    assert r.lpushx("myotherlist", "Hello") == 0
    assert r.lrange("mylist", 0, -1) == ["Hello", "World"]
    assert r.lrange("myotherlist", 0, -1) == []


def test_lrange(delete_keys):
    """ LRANGE key start stop
            Available since 1.0.0.
            Time complexity: O(S+N) where S is the distance of start offset
            from HEAD for small lists, from nearest end (HEAD or TAIL) for
            large lists; and N is the number of elements in the specified range.

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> LRANGE mylist 0 0
    1) "one"
    redis> LRANGE mylist -3 2
    1) "one"
    2) "two"
    3) "three"
    redis> LRANGE mylist -100 100
    1) "one"
    2) "two"
    3) "three"
    redis> LRANGE mylist 5 10
    (empty list or set)
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lrange("mylist", 0, 0) == ["one"]
    assert r.lrange("mylist", -3, 2) == ["one", "two", "three"]
    assert r.lrange("mylist", -100, 100) == ["one", "two", "three"]
    assert r.lrange("mylist", 5, 10) == []


def test_lrem(delete_keys):
    """ LREM key count value
            Available since 1.0.0.
            Time complexity: O(N) where N is the length of the list.

    redis> RPUSH mylist "hello"
    (integer) 1
    redis> RPUSH mylist "hello"
    (integer) 2
    redis> RPUSH mylist "foo"
    (integer) 3
    redis> RPUSH mylist "hello"
    (integer) 4
    redis> LREM mylist -2 "hello"
    (integer) 2
    redis> LRANGE mylist 0 -1
    1) "hello"
    2) "foo"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "hello") == 1
    assert r.rpush("mylist", "hello") == 2
    assert r.rpush("mylist", "foo") == 3
    assert r.rpush("mylist", "hello") == 4
    assert r.lrem("mylist", "hello", -2) == 2  # lrem(self, name, value, num=0)
    assert r.lrange("mylist", 0, -1) == ["hello", "foo"]


def test_lset(delete_keys):
    """ LSET key index value
            Available since 1.0.0.
            Time complexity: O(N) where N is the length of the list. Setting
            either the first or the last element of the list is O(1).

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> LSET mylist 0 "four"
    OK
    redis> LSET mylist -2 "five"
    OK
    redis> LRANGE mylist 0 -1
    1) "four"
    2) "five"
    3) "three"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lset("mylist", 0, "four") is True
    assert r.lset("mylist", -2, "five") is True
    assert r.lrange("mylist", 0, -1) == ["four", "five", "three"]


def test_ltrim(delete_keys):
    """ LTRIM key start stop
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of elements
            to be removed by the operation.

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> LTRIM mylist 1 -1
    OK
    redis> LRANGE mylist 0 -1
    1) "two"
    2) "three"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.ltrim("mylist", 1, -1) is True
    assert r.lrange("mylist", 0, -1) == ["two", "three"]


def test_rpop(delete_keys):
    """ RPOP key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> RPOP mylist
    "three"
    redis> LRANGE mylist 0 -1
    1) "one"
    2) "two"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.rpop("mylist") == "three"
    assert r.lrange("mylist", 0, -1) == ["one", "two"]


def test_rpoplpush(delete_keys):
    """ RPOPLPUSH source destination
            Available since 1.2.0.
            Time complexity: O(1)

    redis> RPUSH mylist "one"
    (integer) 1
    redis> RPUSH mylist "two"
    (integer) 2
    redis> RPUSH mylist "three"
    (integer) 3
    redis> RPOPLPUSH mylist myotherlist
    "three"
    redis> LRANGE mylist 0 -1
    1) "one"
    2) "two"
    redis> LRANGE myotherlist 0 -1
    1) "three"
    redis>
    """
    delete_keys.keys("{tag}mylist", "{tag}myotherlist")

    assert r.rpush("{tag}mylist", "one") == 1
    assert r.rpush("{tag}mylist", "two") == 2
    assert r.rpush("{tag}mylist", "three") == 3
    assert r.rpoplpush("{tag}mylist", "{tag}myotherlist") == "three"
    assert r.lrange("{tag}mylist", 0, -1) == ["one", "two"]
    assert r.lrange("{tag}myotherlist", 0, -1) == ["three"]


def test_rpush(delete_keys):
    """ RPUSH key value [value ...]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> RPUSH mylist "hello"
    (integer) 1
    redis> RPUSH mylist "world"
    (integer) 2
    redis> LRANGE mylist 0 -1
    1) "hello"
    2) "world"
    redis>
    """
    delete_keys.keys("mylist")

    assert r.rpush("mylist", "hello") == 1
    assert r.rpush("mylist", "world") == 2
    assert r.lrange("mylist", 0, -1) == ["hello", "world"]


def test_rpushx(delete_keys):
    """ RPUSHX key value
            Available since 2.2.0.
            Time complexity: O(1)

    redis> RPUSH mylist "Hello"
    (integer) 1
    redis> RPUSHX mylist "World"
    (integer) 2
    redis> RPUSHX myotherlist "World"
    (integer) 0
    redis> LRANGE mylist 0 -1
    1) "Hello"
    2) "World"
    redis> LRANGE myotherlist 0 -1
    (empty list or set)
    redis>
    """
    delete_keys.keys("mylist", "myotherlist")

    assert r.rpush("mylist", "Hello") == 1
    assert r.rpushx("mylist", "World") == 2
    assert r.rpushx("myotherlist", "World") == 0
    assert r.lrange("mylist", 0, -1) == ["Hello", "World"]
    assert r.lrange("myotherlist", 0, -1) == []




###sets
def test_sadd(delete_keys):
    """ SADD key member [member ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of members to be added.

    redis> SADD myset "Hello"
    (integer) 1
    redis> SADD myset "World"
    (integer) 1
    redis> SADD myset "World"
    (integer) 0
    redis> SMEMBERS myset
    1) "World"
    2) "Hello"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.sadd("myset", "World") == 0
    assert r.smembers("myset") == {"World", "Hello"}


def test_scard(delete_keys):
    """ SCARD key
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SADD myset "Hello"
    (integer) 1
    redis> SADD myset "World"
    (integer) 1
    redis> SCARD myset
    (integer) 2
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.scard("myset") == 2


def test_sdiff(delete_keys):
    """ SDIFF key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the total number of
            elements in all given sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SDIFF key1 key2
    1) "a"
    2) "b"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sdiff("{tag}key1", "{tag}key2") == {"a", "b"}


def test_sdiffstore(delete_keys):
    """ SDIFFSTORE destination key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the total number of
            elements in all given sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SDIFFSTORE key key1 key2
    (integer) 2
    redis> SMEMBERS key
    1) "a"
    2) "b"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2", "{tag}key")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sdiffstore("{tag}key", "{tag}key1", "{tag}key2") == 2
    assert r.smembers("{tag}key") == {"a", "b"}


def test_sinter(delete_keys):
    """ SINTER key [key ...]
            Available since 1.0.0.
            Time complexity: O(N*M) worst case where N is the cardinality of
            the smallest set and M is the number of sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SINTER key1 key2
    1) "c"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sinter("{tag}key1", "{tag}key2") == {"c"}


def test_sinterstore(delete_keys):
    """ SINTERSTORE destination key [key ...]
            Available since 1.0.0.
            Time complexity: O(N*M) worst case where N is the cardinality of
            the smallest set and M is the number of sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SINTERSTORE key key1 key2
    (integer) 1
    redis> SMEMBERS key
    1) "c"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sinterstore("{tag}key", "{tag}key1", "{tag}key2") == 1
    assert r.smembers("{tag}key") == {"c"}


def test_sismember(delete_keys):
    """ SISMEMBER key member
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SADD myset "one"
    (integer) 1
    redis> SISMEMBER myset "one"
    (integer) 1
    redis> SISMEMBER myset "two"
    (integer) 0
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "one") == 1
    assert r.sismember("myset", "one") == 1
    assert r.sismember("myset", "two") == 0


def test_smembers(delete_keys):
    """ SMEMBERS key
            Available since 1.0.0.
            Time complexity: O(N) where N is the set cardinality.

    redis> SADD myset "Hello"
    (integer) 1
    redis> SADD myset "World"
    (integer) 1
    redis> SMEMBERS myset
    1) "World"
    2) "Hello"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.smembers("myset") == {"World", "Hello"}


def test_smove(delete_keys):
    """ SMOVE source destination member
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SADD myset "one"
    (integer) 1
    redis> SADD myset "two"
    (integer) 1
    redis> SADD myotherset "three"
    (integer) 1
    redis> SMOVE myset myotherset "two"
    (integer) 1
    redis> SMEMBERS myset
    1) "one"
    redis> SMEMBERS myotherset
    1) "three"
    2) "two"
    redis>
    """
    delete_keys.keys("{tag}myset", "{tag}myotherset")

    assert r.sadd("{tag}myset", "one") == 1
    assert r.sadd("{tag}myset", "two") == 1
    assert r.sadd("{tag}myotherset", "three") == 1
    assert r.smove("{tag}myset", "{tag}myotherset", "two") == 1
    assert r.smembers("{tag}myset") == {"one"}
    assert r.smembers("{tag}myotherset") == {"three", "two"}


def test_spop(delete_keys):
    """ SPOP key [count]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SADD myset "one"
    (integer) 1
    redis> SADD myset "two"
    (integer) 1
    redis> SADD myset "three"
    (integer) 1
    redis> SPOP myset
    "three"
    redis> SMEMBERS myset
    1) "two"
    2) "one"
    redis> SADD myset "four"
    (integer) 1
    redis> SADD myset "five"
    (integer) 1
    redis> SPOP myset 3
    1) "five"
    2) "four"
    3) "two"
    redis> SMEMBERS myset
    1) "one"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "one") == 1
    assert r.sadd("myset", "two") == 1
    assert r.sadd("myset", "three") == 1
    assert r.spop("myset") in {"one", "two", "three"}        # 随机
    #assert r.smembers("myset") == {"two", "three"}
    #assert r.sadd("myset", "four") == 1
    #assert r.sadd("myset", "five") == 1
    #assert r.spop("myset", 3) == {"five", "four", "two"}
    #assert r.smembers("myset") == {"one"}


def test_srandmember(delete_keys):
    """ SRANDMEMBER key [count]
            Available since 1.0.0.
            Time complexity: Without the count argument O(1), otherwise O(N)
            where N is the absolute value of the passed count.

    redis> SADD myset one two three
    (integer) 3
    redis> SRANDMEMBER myset
    "three"
    redis> SRANDMEMBER myset 2
    1) "three"
    2) "one"
    redis> SRANDMEMBER myset -5
    1) "three"
    2) "two"
    3) "two"
    4) "three"
    5) "one"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "one", "two", "three") == 3
    # random
    assert r.srandmember("myset") in {"one", "two", "three"}
    #assert r.srandmember("myset", 2) in {"one", "two", "three"}
    # list for this
    #assert r.srandmember("myset", -5) == ["three", "two", "two", "three", "one"]


def test_srem(delete_keys):
    """ SREM key member [member ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the number of
            members to be removed.

    redis> SADD myset "one"
    (integer) 1
    redis> SADD myset "two"
    (integer) 1
    redis> SADD myset "three"
    (integer) 1
    redis> SREM myset "one"
    (integer) 1
    redis> SREM myset "four"
    (integer) 0
    redis> SMEMBERS myset
    1) "three"
    2) "two"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "one") == 1
    assert r.sadd("myset", "two") == 1
    assert r.sadd("myset", "three") == 1
    assert r.srem("myset", "one") == 1
    assert r.srem("myset", "four") == 0
    assert r.smembers("myset") == {"three", "two"}


def test_sunion(delete_keys):
    """ SUNION key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the total number of
            elements in all given sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SUNION key1 key2
    1) "a"
    2) "b"
    3) "c"
    4) "d"
    5) "e"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sunion("{tag}key1", "{tag}key2") == {"a", "b", "c", "d", "e"}


def test_sunionstore(delete_keys):
    """ SUNIONSTORE destination key [key ...]
            Available since 1.0.0.
            Time complexity: O(N) where N is the total number of
            elements in all given sets.

    redis> SADD key1 "a"
    (integer) 1
    redis> SADD key1 "b"
    (integer) 1
    redis> SADD key1 "c"
    (integer) 1
    redis> SADD key2 "c"
    (integer) 1
    redis> SADD key2 "d"
    (integer) 1
    redis> SADD key2 "e"
    (integer) 1
    redis> SUNIONSTORE key key1 key2
    (integer) 5
    redis> SMEMBERS key
    1) "a"
    2) "b"
    3) "c"
    4) "d"
    5) "e"
    redis>
    """
    delete_keys.keys("{tag}key1", "{tag}key2", "{tag}key")

    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sunionstore("{tag}key", "{tag}key1", "{tag}key2") == 5
    assert r.smembers("{tag}key") == {"a", "b", "c", "d", "e"}


def test_sscan(delete_keys):
    """ SSCAN key cursor [MATCH pattern] [COUNT count]
            Available since 2.8.0.
            Time complexity: O(1) for every call. O(N) for a complete
            iteration, including enough command calls for the cursor
            to return back to 0. N is the number of
            elements inside the collection..

    redis> sadd myset a b c
    (integer) 3
    redis> sscan myset 0
    1) "0"
    2) 1) "c"
       2) "b"
       3) "a"
    redis>
    """
    delete_keys.keys("myset")

    assert r.sadd("myset", "a", "b", "c") == 3
    # assert r.sscan("myset", 0) == (0, ["c", "b", "a])    #
    cursor = r.sscan("myset", 0)
    assert isinstance(cursor, tuple)
    assert cursor[0] == 0
    assert sorted(cursor[1]) == sorted(["c", "b", "a"])




###sorted sets
def test_zadd(delete_keys):
    """ ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
            Available since 1.2.0.
            Time complexity: O(log(N)) for each item added, where N is
            the number of elements in the sorted set.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 1 "uno"
    (integer) 1
    redis> ZADD myzset 2 "two" 3 "three"
    (integer) 2
    redis> ZRANGE myzset 0 -1 WITHSCORES
    1) "one"
    2) "1"
    3) "uno"
    4) "1"
    5) "two"
    6) "2"
    7) "three"
    8) "3"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1       # reverserd
    assert r.zadd("myzset", "uno", 1) == 1
    assert r.zadd("myzset", "two", 2, "three", 3) == 2
    assert r.zrange("myzset", 0, -1, withscores=True) == [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]


def test_zcard(delete_keys):
    """ ZCARD key
            Available since 1.2.0.
            Time complexity: O(1)

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZCARD myzset
    (integer) 2
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zcard("myzset") == 2


def test_zcount(delete_keys):
    """ ZCOUNT key min max
            Available since 2.0.0.
            Time complexity: O(log(N)) with N being the number of
            elements in the sorted set.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZCOUNT myzset -inf +inf
    (integer) 3
    redis> ZCOUNT myzset (1 3
    (integer) 2
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zcount("myzset", "-inf", "+inf") == 3
    assert r.zcount("myzset", "(1", 3) == 2


def test_zincrby(delete_keys):
    """ ZINCRBY key increment member
            Available since 1.2.0.
            Time complexity: O(log(N)) where N is the number of
            elements in the sorted set.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZINCRBY myzset 2 "one"
    "3"
    redis> ZRANGE myzset 0 -1 WITHSCORES
    1) "two"
    2) "2"
    3) "one"
    4) "3"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zincrby("myzset", "one", 2) == 3
    assert r.zrange("myzset", 0, -1, withscores=True) == [("two", 2), ("one", 3)]


def test_zinterstore(delete_keys):
    """ ZINTERSTORE destination numkeys key [key ...]
        [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
            Available since 2.0.0.
            Time complexity: O(N*K)+O(M*log(M)) worst case with N being the
            smallest input sorted set, K being the number of input sorted sets
            and M being the number of elements in the resulting sorted set.

    redis> ZADD zset1 1 "one"
    (integer) 1
    redis> ZADD zset1 2 "two"
    (integer) 1
    redis> ZADD zset2 1 "one"
    (integer) 1
    redis> ZADD zset2 2 "two"
    (integer) 1
    redis> ZADD zset2 3 "three"
    (integer) 1
    redis> ZINTERSTORE out 2 zset1 zset2 WEIGHTS 2 3
    (integer) 2
    redis> ZRANGE out 0 -1 WITHSCORES
    1) "one"
    2) "5"
    3) "two"
    4) "10"
    redis>
    """
    delete_keys.keys("{tag}zset1", "{tag}zset2", "{tag}out")

    assert r.zadd("{tag}zset1", "one", 1) == 1
    assert r.zadd("{tag}zset1", "two", 2) == 1
    assert r.zadd("{tag}zset2", "one", 1) == 1
    assert r.zadd("{tag}zset2", "two", 2) == 1
    assert r.zadd("{tag}zset2", "three", 3) == 1
    assert r.zinterstore("{tag}out", {"{tag}zset1": 2, "{tag}zset2": 3}) == 2
    assert r.zrange("{tag}out", 0, -1, withscores=True) == [("one", 5), ("two", 10)]


def test_zlexcount(delete_keys):
    """ ZLEXCOUNT key min max
            Available since 2.8.9.
            Time complexity: O(log(N)) with N being the number of elements
            in the sorted set.

    redis> ZADD myzset 0 a 0 b 0 c 0 d 0 e
    (integer) 5
    redis> ZADD myzset 0 f 0 g
    (integer) 2
    redis> ZLEXCOUNT myzset - +
    (integer) 7
    redis> ZLEXCOUNT myzset [b [f
    (integer) 5
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0) == 5
    assert r.zadd("myzset", "f", 0, "g", 0) == 2
    assert r.zlexcount("myzset", "-", "+") == 7
    assert r.zlexcount("myzset", "[b", "[f") == 5


def test_zrange(delete_keys):
    """ ZRANGE key start stop [WITHSCORES]
            Available since 1.2.0.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements returned.

    # 1
    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZRANGE myzset 0 -1
    1) "one"
    2) "two"
    3) "three"
    redis> ZRANGE myzset 2 3
    1) "three"
    redis> ZRANGE myzset -2 -1
    1) "two"
    2) "three"
    redis>

    # 2
    redis> ZRANGE myzset 0 1 WITHSCORES
    1) "one"
    2) "1"
    3) "two"
    4) "2"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrange("myzset", 0, -1) == ["one", "two", "three"]
    assert r.zrange("myzset", 2, 3) == ["three"]
    assert r.zrange("myzset", -2, -1) == ["two", "three"]


def test_zrangebylex(delete_keys):
    """ ZRANGEBYLEX key min max [LIMIT offset count]
            Available since 2.8.9.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements being returned.
            If M is constant (e.g. always asking for the first 10 elements
            with LIMIT), you can consider it O(log(N)).

    redis> ZADD myzset 0 a 0 b 0 c 0 d 0 e 0 f 0 g
    (integer) 7
    redis> ZRANGEBYLEX myzset - [c
    1) "a"
    2) "b"
    3) "c"
    redis> ZRANGEBYLEX myzset - (c
    1) "a"
    2) "b"
    redis> ZRANGEBYLEX myzset [aaa (g
    1) "b"
    2) "c"
    3) "d"
    4) "e"
    5) "f"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g", 0) == 7
    assert r.zrangebylex("myzset", "-", "[c") == ["a", "b", "c"]
    assert r.zrangebylex("myzset", "-", "(c") == ["a", "b"]
    assert r.zrangebylex("myzset", "[aaa", "(g") == ["b", "c", "d", "e", "f"]


def test_zrevrangebylex(delete_keys):
    """ ZREVRANGEBYLEX key max min [LIMIT offset count]
            Available since 2.8.9.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements being returned.
            If M is constant (e.g. always asking for the first 10 elements
            with LIMIT), you can consider it O(log(N)).

    redis> ZADD myzset 0 a 0 b 0 c 0 d 0 e 0 f 0 g
    (integer) 7
    redis> ZREVRANGEBYLEX myzset [c -
    1) "c"
    2) "b"
    3) "a"
    redis> ZREVRANGEBYLEX myzset (c -
    1) "b"
    2) "a"
    redis> ZREVRANGEBYLEX myzset (g [aaa
    1) "f"
    2) "e"
    3) "d"
    4) "c"
    5) "b"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g", 0) == 7
    assert r.zrevrangebylex("myzset", "[c", "-") == ["c", "b", "a"]
    assert r.zrevrangebylex("myzset", "(c", "-") == ["b", "a"]
    assert r.zrevrangebylex("myzset", "(g", "[aaa") == ["f", "e", "d", "c", "b"]


def test_zrangebyscore(delete_keys):
    """ ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
            Available since 1.0.5.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements being returned.
            If M is constant (e.g. always asking for the first 10 elements
            with LIMIT), you can consider it O(log(N)).

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZRANGEBYSCORE myzset -inf +inf
    1) "one"
    2) "two"
    3) "three"
    redis> ZRANGEBYSCORE myzset 1 2
    1) "one"
    2) "two"
    redis> ZRANGEBYSCORE myzset (1 2
    1) "two"
    redis> ZRANGEBYSCORE myzset (1 (2
    (empty list or set)
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrangebyscore("myzset", "-inf", "+inf") == ["one", "two", "three"]
    assert r.zrangebyscore("myzset", 1, 2) == ["one", "two"]
    assert r.zrangebyscore("myzset", "(1", 2) == ["two"]
    assert r.zrangebyscore("myzset", "(1", "(2") == []


def test_zrank(delete_keys):
    """ ZRANK key member
            Available since 2.0.0.
            Time complexity: O(log(N))

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZRANK myzset "three"
    (integer) 2
    redis> ZRANK myzset "four"
    (nil)
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrank("myzset", "three") == 2
    assert r.zrank("myzset", "four") is None


def test_zrem(delete_keys):
    """ ZREM key member [member ...]
            Available since 1.2.0.
            Time complexity: O(M*log(N)) with N being the number of elements
            in the sorted set and M the number of elements to be removed.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREM myzset "two"
    (integer) 1
    redis> ZRANGE myzset 0 -1 WITHSCORES
    1) "one"
    2) "1"
    3) "three"
    4) "3"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrem("myzset", "two") == 1
    assert r.zrange("myzset", 0, -1, withscores=True) == [("one", 1), ("three", 3)]


def test_zremrangebylex(delete_keys):
    """ ZREMRANGEBYLEX key min max
            Available since 2.8.9.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements
            removed by the operation.

    redis> ZADD myzset 0 aaaa 0 b 0 c 0 d 0 e
    (integer) 5
    redis> ZADD myzset 0 foo 0 zap 0 zip 0 ALPHA 0 alpha
    (integer) 5
    redis> ZRANGE myzset 0 -1
    1) "ALPHA"
    2) "aaaa"
    3) "alpha"
    4) "b"
    5) "c"
    6) "d"
    7) "e"
    8) "foo"
    9) "zap"
    10) "zip"
    redis> ZREMRANGEBYLEX myzset [alpha [omega
    (integer) 6
    redis> ZRANGE myzset 0 -1
    1) "ALPHA"
    2) "aaaa"
    3) "zap"
    4) "zip"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "e", 0, "d", 0, "c", 0, "b", 0, "aaaa", 0) == 5
    assert r.zadd("myzset", "alpha", 0, "ALPHA", 0, "zip", 0, "zap", 0, "foo", 0) == 5
    assert r.zrange("myzset", 0, -1) == ["ALPHA", "aaaa", "alpha", "b", "c", "d", "e", "foo", "zap", "zip"]
    assert r.zremrangebylex("myzset", "[alpha", "[omega") == 6
    assert r.zrange("myzset", 0, -1) == ["ALPHA", "aaaa", "zap", "zip"]


def test_zremrangebyrank(delete_keys):
    """ ZREMRANGEBYRANK key start stop
            Available since 2.0.0.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements
            removed by the operation.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREMRANGEBYRANK myzset 0 1
    (integer) 2
    redis> ZRANGE myzset 0 -1 WITHSCORES
    1) "three"
    2) "3"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zremrangebyrank("myzset", 0, 1) == 2
    assert r.zrange("myzset", 0, -1, withscores=True) == [("three", 3)]


def test_zremrangebyscore(delete_keys):
    """ ZREMRANGEBYSCORE key min max
            Available since 1.2.0.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements
            removed by the operation.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREMRANGEBYSCORE myzset -inf (2
    (integer) 1
    redis> ZRANGE myzset 0 -1 WITHSCORES
    1) "two"
    2) "2"
    3) "three"
    4) "3"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zremrangebyscore("myzset", "-inf", "(2") == 1
    assert r.zrange("myzset", 0, -1, withscores=True) == [("two", 2), ("three", 3)]


def test_zrevrange(delete_keys):
    """ ZREVRANGE key start stop [WITHSCORES]
            Available since 1.2.0.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements returned.

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREVRANGE myzset 0 -1
    1) "three"
    2) "two"
    3) "one"
    redis> ZREVRANGE myzset 2 3
    1) "one"
    redis> ZREVRANGE myzset -2 -1
    1) "two"
    2) "one"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrange("myzset", 0, -1) == ["three", "two", "one"]
    assert r.zrevrange("myzset", 2, 3) == ["one"]
    assert r.zrevrange("myzset", -2, -1) == ["two", "one"]


def test_zrevrangebyscore(delete_keys):
    """ ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
            Available since 2.2.0.
            Time complexity: O(log(N)+M) with N being the number of elements
            in the sorted set and M the number of elements being returned.
            If M is constant (e.g. always asking for the first 10 elements
            with LIMIT), you can consider it O(log(N)).

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREVRANGEBYSCORE myzset +inf -inf
    1) "three"
    2) "two"
    3) "one"
    redis> ZREVRANGEBYSCORE myzset 2 1
    1) "two"
    2) "one"
    redis> ZREVRANGEBYSCORE myzset 2 (1
    1) "two"
    redis> ZREVRANGEBYSCORE myzset (2 (1
    (empty list or set)
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrangebyscore("myzset", "+inf", "-inf") == ["three", "two", "one"]
    assert r.zrevrangebyscore("myzset", 2, 1) == ["two", "one"]
    assert r.zrevrangebyscore("myzset", 2, "(1") == ["two"]
    assert r.zrevrangebyscore("myzset", "(2", "(1") == []


def test_zrevrank(delete_keys):
    """ ZREVRANK key member
            Available since 2.0.0.
            Time complexity: O(log(N))

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZADD myzset 2 "two"
    (integer) 1
    redis> ZADD myzset 3 "three"
    (integer) 1
    redis> ZREVRANK myzset "one"
    (integer) 2
    redis> ZREVRANK myzset "four"
    (nil)
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrank("myzset", "one") == 2
    assert r.zrevrank("myzset", "four") is None


def test_zscore(delete_keys):
    """ ZSCORE key member
            Available since 1.2.0.
            Time complexity: O(1)

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZSCORE myzset "one"
    "1"
    redis>
    """
    delete_keys.keys("myzset")

    assert r.zadd("myzset", "one", 1) == 1
    assert r.zscore("myzset", "one") == 1


def test_zunionstore(delete_keys):
    """ ZUNIONSTORE destination numkeys key [key ...]
        [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
            Available since 2.0.0.
            Time complexity: O(N)+O(M log(M)) with N being the sum of the
            sizes of the input sorted sets, and M being the number of elements
            in the resulting sorted set.

    redis> ZADD zset1 1 "one"
    (integer) 1
    redis> ZADD zset1 2 "two"
    (integer) 1
    redis> ZADD zset2 1 "one"
    (integer) 1
    redis> ZADD zset2 2 "two"
    (integer) 1
    redis> ZADD zset2 3 "three"
    (integer) 1
    redis> ZUNIONSTORE out 2 zset1 zset2 WEIGHTS 2 3
    (integer) 3
    redis> ZRANGE out 0 -1 WITHSCORES
    1) "one"
    2) "5"
    3) "three"
    4) "9"
    5) "two"
    6) "10"
    redis>
    """
    delete_keys.keys("{tag}zset1", "{tag}zset2", "{tag}out")

    assert r.zadd("{tag}zset1", "one", 1) == 1
    assert r.zadd("{tag}zset1", "two", 2) == 1
    assert r.zadd("{tag}zset2", "one", 1) == 1
    assert r.zadd("{tag}zset2", "two", 2) == 1
    assert r.zadd("{tag}zset2", "three", 3) == 1
    assert r.zunionstore("{tag}out", {"{tag}zset1": 2, "{tag}zset2": 3}) == 3
    assert r.zrange("{tag}out", 0, -1, withscores=True) == [("one", 5), ("three", 9), ("two", 10)]


def test_zscan(delete_keys):
    """ ZSCAN key cursor [MATCH pattern] [COUNT count]
            Available since 2.8.0.
            Time complexity: O(1) for every call. O(N) for a complete
            iteration, including enough command calls for the cursor to return
            back to 0. N is the number of elements inside the collection..

    redis> ZADD zset1 1 one 2 two 3 three
    (integer) 3
    redis> ZSCAN zset1 0
    1) "0"
    2) 1) "one"
       2) "1"
       3) "two"
       4) "2"
       5) "three"
       6) "3"
    redis>
    """
    delete_keys.keys("zset1")

    assert r.zadd("zset1", "one", 1, "two", 2, "three", 3) == 3
    assert r.zscan("zset1", 0) == (0, [("one", 1), ("two", 2), ("three", 3)])


###hyperloglog
def test_pfadd(delete_keys):
    """ PFADD key element [element ...]
            Available since 2.8.9.
            Time complexity: O(1) to add every element.

    redis> PFADD hll a b c d e f g
    (integer) 1
    redis> PFCOUNT hll
    (integer) 7
    redis>
    """
    delete_keys.keys("hll")

    assert r.pfadd("hll", "a", "b", "c", "d", "e", "f", "g") == 1
    assert r.pfcount("hll") == 7


def test_pfcount(delete_keys):
    """ PFCOUNT key [key ...]
            Available since 2.8.9.
            Time complexity: O(1) with every small average constant times when
            called with a single key. O(N) with N being the number of keys,
            and much bigger constant times, when called with multiple keys.

    redis> PFADD hll foo bar zap
    (integer) 1
    redis> PFADD hll zap zap zap
    (integer) 0
    redis> PFADD hll foo bar
    (integer) 0
    redis> PFCOUNT hll
    (integer) 3
    redis> PFADD some-other-hll 1 2 3
    (integer) 1
    redis> PFCOUNT hll some-other-hll
    (integer) 6
    redis>
    """
    delete_keys.keys("{tag}hll", "{tag}some-other-hll")

    assert r.pfadd("{tag}hll", "foo", "bar", "zap") == 1
    assert r.pfadd("{tag}hll", "zap", "zap", "zap") == 0
    assert r.pfadd("{tag}hll", "foo", "bar") == 0
    assert r.pfcount("{tag}hll") == 3
    assert r.pfadd("{tag}some-other-hll", 1, 2, 3) == 1
    assert r.pfcount("{tag}hll", "{tag}some-other-hll") == 6


def test_pfmerdeletege(delete_keys):
    """ PFMERGE destkey sourcekey [sourcekey ...]
            Available since 2.8.9.
            Time complexity: O(N) to merge N HyperLogLogs,
            but with high constant times.

    redis> PFADD hll1 foo bar zap a
    (integer) 1
    redis> PFADD hll2 a b c foo
    (integer) 1
    redis> PFMERGE hll3 hll1 hll2
    OK
    redis> PFCOUNT hll3
    (integer) 6
    redis>
    """
    delete_keys.keys("{tag}hll1", "{tag}hll2", "{tag}hll3")

    assert r.pfadd("{tag}hll1", "foo", "bar", "zap", "a") == 1
    assert r.pfadd("{tag}hll2", "a", "b", "c", "foo") == 1
    assert r.pfmerge("{tag}hll3", "{tag}hll1", "{tag}hll2") is True
    assert r.pfcount("{tag}hll3") == 6


###script
def test_eval(delete_keys):
    """ EVAL script numkeys key [key ...] arg [arg ...]
            Available since 2.6.0.
            Time complexity: Depends on the script that is executed.

    http://redis.io/commands/eval
    """
    delete_keys.keys("key1")

    lua = "return {KEYS[1],ARGV[1]}"
    keys_and_args = ["key1","first"]
    assert r.eval(lua, 1, *keys_and_args) == keys_and_args


# def test_evalsha():
#     """ EVALSHA sha1 numkeys key [key ...] arg [arg ...]
#             Available since 2.6.0.
#             Time complexity: Depends on the script that is executed.
#     """


# misc
def test_auth():
    """ AUTH password
            Available since 1.0.0.
    """
    with pytest.raises(redis.RedisError) as exc:
        r.auth(123)
    assert 'Client sent AUTH, but no password is set' in str(exc.value)


# def test_echo():
#     """ ECHO message
#             Available since 1.0.0.
#
#     redis> ECHO "Hello World!"
#     "Hello World!"
#     redis>
#     """
#     assert r.echo("Hello World") == "Hello World"


def test_ping():
    """ PING
            Available since 1.0.0.

    redis> PING
    PONG
    redis> PING "hello world"
    "hello world"
    redis>
    """
    assert r.ping() is True
    #assert r.ping("hello world") == "hello world"


def test_quit():
    """ QUIT
            Available since 1.0.0.
    """
    assert r.quit() == "OK"


# def test_select():
#     """ SELECT index
#             Available since 1.0.0.
#     """
#     assert r.select(0) is True


def test_info():
    """ INFO [section]
            Available since 1.0.0.

    redis> INFO
    # Server
    redis_version:999.999.999
    redis_git_sha1:ceaf58df
    redis_git_dirty:1
    ...
    redis>
    """
    assert r.info()


def test_time():
    assert type(r.time()) is tuple


def test_large_value(delete_keys):
    delete_keys.keys("hello")

    length = 1024 * 1024 * 100
    r.set("hello", "h" * length)
    a = r.get("hello")

    assert len(a) == length


def test_moved(delete_keys, moved):
    delete_keys.keys("hello")

    r.set("hello", 123)
    cluster = Cluster.from_node(ClusterNode.from_uri(REDIS_URI_SRC))
    cluster.migrate_slot(ClusterNode.from_uri(REDIS_URI_SRC),
                         ClusterNode.from_uri(REDIS_URI_DST),
                         SLOT)
    # double check
    assert r.get("hello") == "123"
    time.sleep(0.6)
    assert r.get("hello") == "123"


def test_ask(delete_keys, asked):
    delete_keys.keys("hello")

    r.set("hello", 123)
    node1 = ClusterNode.from_uri(REDIS_URI_SRC)
    node2 = ClusterNode.from_uri(REDIS_URI_DST)

    node1.setslot("MIGRATING", 866, node2.name)
    node2.setslot("IMPORTING", 866, node1.name)

    with pytest.raises(redis.ResponseError) as excinfo:
        node1.get("a{hello}")

    assert "ASK 866 127.0.0.1:8001" in str(excinfo.value)
    assert r.get("a{hello}") is None


def test_delete_node(delete_keys):
    delete_keys.keys("hello")

    r.set("hello", 123)
    src = ClusterNode.from_uri(REDIS_URI_SRC)
    target = ClusterNode.from_uri("redis://127.0.0.1:8003")

    cluster = Cluster.from_node(src)
    cluster.add_node({"addr": "127.0.0.1:8003", "role": "master"})
    cluster.wait()

    cluster.migrate_slot(src, target, 866)
    cluster.wait()

    assert r.get("hello") == "123"

    time.sleep(0.2)

    cluster.delete_node(target)
    cluster.wait()

    time.sleep(0.5)
    r.execute_command('PROXY UPDATESLOTMAP')
    time.sleep(0.5)

    assert r.get("hello") == "123"


def test_writable(delete_keys):
    delete_keys.keys("hello")

    with r.pipeline(transaction=False) as p:
        for u in range(20):
            p.set('hello', 'x' * 1024 * 1024 * 17)
            p.get('hello')
        p.execute()
