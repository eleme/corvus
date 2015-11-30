# -*- coding: utf-8 -*-

import inspect
import redis


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


r = Redis(port=12345)
#r = Redis(port=6379)



def delete_keys(*keys):
    def decorator(func):
        setattr(func, "keys", keys)
        return func
    return decorator


@delete_keys("a", "b")
def test_null_key():
    assert r.set('', 1) is True
    assert r.get('') == '1'


@delete_keys("key1", "key2", "key3")
def test_del():
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
    assert r.set("key1", "Hello") is True
    assert r.set("key2", "World") is True
    assert r.delete("key1", "key2", "key3") == 2


@delete_keys("mykey")
def test_dump():
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
    assert r.set("mykey", 10) is True
    assert r.dump("mykey") == "\x00\xc0\n\x06\x00\xf8r?\xc5\xfb\xfb_("


@delete_keys("key1", "nosuchkey", "key2")
def test_exists():
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
    assert r.set("key1", "Hello") is True
    assert r.exists("key1") == 1
    assert r.exists("nosuchkey") == 0
    assert r.set("key2", "World") is True
    assert r.exists("key1", "key2", "nosuchkey")


@delete_keys("mykey")
def test_expire():
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
    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10
    assert r.set("mykey", "Hello World") is True
    assert r.ttl("mykey") is None


@delete_keys("mykey")
def test_expireat():
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
    assert r.set("mykey", "Hello") is True
    assert r.exists("mykey") is True
    assert r.expireat("mykey", 1293840000) == 1
    assert r.exists("mykey") is False


#def test_keys():
    """ KEYS pattern
            Available since 1.0.0.
            Time complexity: O(N) with N being the number of keys in the
            database, under the assumption that the key names in the database
            and the given pattern have limited length.

    redis> MSET one 1 two 2 three 3 four 4
    OK
    redis> KEYS *o*
    1) "two"
    2) "four"
    3) "one"
    redis> KEYS t??
    1) "two"
    redis> KEYS *
    1) "two"
    2) "four"
    3) "three"
    4) "one"
    redis>
    """
#    assert r.mset({"one": 1, "two": 2, "three": 3, "four": 4}) is True
#    assert r.keys("*o*") == ["two", "four", "one"]
#    assert r.keys("t??") == ["two"]
#    assert r.keys("*") == ["two", "three", "four", "one"]


#def test_migrate():
    """ MIGRATE host port key destination-db timeout [COPY] [REPLACE]
            Available since 2.6.0.
            Time complexity: This command actually executes a DUMP+DEL in the
            source instance, and a RESTORE in the target instance. See the
            pages of these commands for time complexity. Also an O(N) data
            transfer between the two instances is performed.
    """



#def test_move():
    """ MOVE key db
            Available since 1.0.0.
            Time complexity: O(1)
    """


#def test_object():
    """ OBJECT subcommand [arguments [arguments ...]]
            Available since 2.2.3.
            Time complexity: O(1) for all the currently implemented subcommands.

    # 1
    redis> lpush mylist "Hello World"
    (integer) 4
    redis> object refcount mylist
    (integer) 1
    redis> object encoding mylist
    "ziplist"
    redis> object idletime mylist
    (integer) 10

    # 2
    redis> set foo 1000
    OK
    redis> object encoding foo
    "int"
    redis> append foo bar
    (integer) 7
    redis> get foo
    "1000bar"
    redis> object encoding foo
    "raw"
    """
#    assert r.lpush("mylist", "Hello World") == 1
#    assert r.object("refcount", "mylist")  == 1
#    assert r.object("encoding", "mylist") == "ziplist"
#    assert r.object("idletime", "mylist") == 0


@delete_keys("mykey")
def test_persist():
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
    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10
    assert r.persist("mykey") == 1
    assert r.ttl("mykey") is None


@delete_keys("mykey")
def test_pexpire():
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
    assert r.set("mykey", "Hello") is True
    assert r.pexpire("mykey", 1500) == 1
    assert r.ttl("mykey")


@delete_keys("mykey")
def test_pexpireat():
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
    assert r.set("mykey", "Hello") is True
    assert r.pexpireat("mykey", 1555555555005) == 1
    assert r.ttl("mykey")
    assert r.pttl("mykey")


@delete_keys("mykey")
def test_pttl():
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
    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 1) == 1
    assert r.pttl("mykey")


#def test_randomkey():
    """ RANDOMKEY
            Available since 1.0.0.
            Time complexity: O(1)
    """


#def test_rename():
    """ RENAME key newkey
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> RENAME mykey myotherkey
    OK
    redis> GET myotherkey
    "Hello"
    redis>
    """
#    assert r.set("mykey", "Hello") is True
#    assert r.rename("mykey", "myotherkey") is True
#    assert r.get("myotherkey") == "Hello"


#def test_renamenx():
    """ RENAMENX key newkey
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> SET myotherkey "World"
    OK
    redis> RENAMENX mykey myotherkey
    (integer) 0
    redis> GET myotherkey
    "World"
    redis>
    """
#    assert r.set("mykey", "Hello") is True
#    assert r.set("myotherkey", "World") is True
#    assert r.renamenx("mykey", "myotherkey") is False
#    assert r.get("myotherkey") == "World"


@delete_keys("mykey")
def test_restore():
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
    res = "\n\x11\x11\x00\x00\x00\x0e\x00\x00\x00\x03\x00\x00\xf2\x02\xf3\x02\xf4\xff\x06\x00Z1_\x1cg\x04!\x18"
    assert r.restore("mykey", 0, res) == "OK"
    assert r.type("mykey") == "list"
    assert r.lrange("mykey", 0, -1) == ["1", "2", "3"]


@delete_keys("mylist")
def test_sort():
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
    assert r.rpush("mylist", 2, 1, 3) == 3
    assert r.lrange("mylist", 0, -1) == ["2", "1", "3"]
    assert r.sort("mylist") == ["1", "2", "3"]
    #assert r.sort("mylist", desc=True, store="myotherlist") == 3
    #assert r.lrange("myotherlist", 0, -1) == ["3", "2", "1"]


@delete_keys("mykey")
def test_ttl():
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
    assert r.set("mykey", "Hello") is True
    assert r.expire("mykey", 10) == 1
    assert r.ttl("mykey") == 10


@delete_keys("key1", "key2", "key3")
def test_type():
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
    assert r.set("key1", "value") is True
    assert r.lpush("key2", "value") == 1
    assert r.sadd("key3", "value") == 1
    assert r.type("key1") == "string"
    assert r.type("key2") == "list"
    assert r.type("key3") == "set"


#def test_wait():
    """ WAIT numslaves timeout
            Available since 3.0.0.
            Time complexity: O(1)
    """


#def test_scan():
    """ SCAN cursor [MATCH pattern] [COUNT count]
            Available since 2.8.0.
            Time complexity: O(1) for every call. O(N) for a complete
            iteration, including enough command calls for the cursor to return
            back to 0. N is the number of elements inside the collection.
    """



###strings
@delete_keys("mykey", "ts")
def test_append():
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
    assert r.exists("mykey") == 0
    assert r.append("mykey", "Hello") == 5
    assert r.append("mykey", " World") == 11
    assert r.get("mykey") == "Hello World"

    assert r.append("ts", "0043") == 4
    assert r.append("ts", "0035") == 8
    assert r.getrange("ts", 0, 3) == "0043"
    assert r.getrange("ts", 4, 7) == "0035"


@delete_keys("mykey")
def test_bitcount():
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
    assert r.set("mykey", "foobar") is True
    assert r.bitcount("mykey") == 26
    assert r.bitcount("mykey", 0, 0) == 4
    assert r.bitcount("mykey", 1, 1) == 6


#def test_bitop():
    """ BITOP operation destkey key [key ...]
            Available since 2.6.0.
            Time complexity: O(N)

    redis> SET key1 "foobar"
    OK
    redis> SET key2 "abcdef"
    OK
    redis> BITOP AND dest key1 key2
    (integer) 6
    redis> GET dest
    "`bc`ab"
    redis>
    """
#    assert r.set("key1", "foobar") is True
#    assert r.set("key2", "abcdef") is True
#    assert r.bitop("AND", "dest", "key1", "key2") == 6
#    assert r.get("dest") == "`bc`ab"


@delete_keys("mykey")
def test_bitpos():
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
    assert r.set("mykey", "\xff\xf0\x00") is True
    assert r.bitpos("mykey", 0) == 12
    assert r.set("mykey", "\x00\xff\xf0") is True
    assert r.bitpos("mykey", 1, 0) == 8
    assert r.bitpos("mykey", 1, 2) == 16
    assert r.set("mykey", "\x00\x00\x00") is True
    assert r.bitpos("mykey", 1) == -1


@delete_keys("mykey")
def test_decr():
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
    assert r.set("mykey", 10) is True
    assert r.decr("mykey") == 9
    #assert r.set("mykey", )


@delete_keys("mykey")
def test_decrby():
    """ DECRBY key decrement
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> DECRBY mykey 3
    (integer) 7
    redis>
    """
    assert r.set("mykey", 10) is True
    assert r.decrby("mykey", 3) == 7


@delete_keys("nonexisting", "mykey")
def test_get():
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
    assert r.get("nonexisting") is None
    assert r.set("mykey", "Hello") is True
    assert r.get("mykey") == "Hello"


@delete_keys("mykey")
def test_getbit():
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
    assert r.setbit("mykey", 7, 1) == 0
    assert r.getbit("mykey", 0) == 0
    assert r.getbit("mykey", 7) == 1
    assert r.getbit("mykey", 100) == 0


@delete_keys("mykey")
def test_getrange():
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
    assert r.set("mykey", "This is a string") is True
    assert r.getrange("mykey", 0, 3) == "This"
    assert r.getrange("mykey", -3, -1) == "ing"
    assert r.getrange("mykey", 0, -1) == "This is a string"
    assert r.getrange("mykey", 10, 100) == "string"


@delete_keys("mykey")
def test_getset():
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
    assert r.set("mykey", "Hello") is True
    assert r.getset("mykey", "World") == "Hello"
    assert r.get("mykey") == "World"


@delete_keys("mykey")
def test_incr():
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
    assert r.set("mykey", 10) is True
    assert r.incr("mykey") == 11
    assert r.get("mykey") == "11"


@delete_keys("mykey")
def test_incrby():
    """ INCRBY key increment
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "10"
    OK
    redis> INCRBY mykey 5
    (integer) 15
    redis>
    """
    assert r.set("mykey", 10) is True
    assert r.incrby("mykey", 5) == 15


@delete_keys("mykey")
def test_incrbyfloat():
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
    assert r.set("mykey", "10.50") is True
    assert r.incrbyfloat("mykey", "0.1") == 10.6
    assert r.set("mykey", "5.0e3") is True
    assert r.incrbyfloat("mykey", "2.0e2") == 5200


@delete_keys("key1", "key2", "nonexisting")
def test_mget():
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
    assert r.set("key1", "Hello") is True
    assert r.set("key2", "World") is True
    assert r.mget("key1", "key2", "nonexisting") == ["Hello", "World", None]


@delete_keys("key1", "key2")
def test_mset():
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


@delete_keys("mykey")
def test_psetex():
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
    assert r.psetex("mykey", 1000, "Hello") is True
    assert r.pttl("mykey") <= 1000
    assert r.get("mykey") == "Hello"


@delete_keys("mykey")
def test_set():
    """ SET key value [EX seconds] [PX milliseconds] [NX|XX]
            Available since 1.0.0.
            Time complexity: O(1)

    redis> SET mykey "Hello"
    OK
    redis> GET mykey
    "Hello"
    redis>
    """
    assert r.set("mykey", "Hello") is True
    assert r.get("mykey") == "Hello"


@delete_keys("mykey")
def test_setbit():
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
    assert r.setbit("mykey", 7, 1) == 0
    assert r.setbit("mykey", 7, 0) == 1
    assert r.get("mykey") == "\x00"


@delete_keys("mykey")
def test_setex():
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
    assert r.setex("mykey", "Hello", 10)            #
    assert r.ttl("mykey") == 10
    assert r.get("mykey") == "Hello"


@delete_keys("mykey")
def test_setnx():
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
    assert r.setnx("mykey", "Hello") == 1
    assert r.setnx("mykey", "World") == 0
    assert r.get("mykey") == "Hello"


@delete_keys("key1")
def test_setrange():
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
    assert r.set("key1", "Hello World") is True
    assert r.setrange("key1", 6, "Redis") == 11
    assert r.get("key1") == "Hello Redis"


@delete_keys("mykey", "nonexisting")
def test_strlen():
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
    assert r.set("mykey", "Hello world") is True
    assert r.strlen("mykey") == 11
    assert r.strlen("nonexisting") == 0


###hashes
@delete_keys("myhash")
def test_hdel():
    """ HDEL key field [field ...]

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HDEL myhash field1
    (integer) 1
    redis> HDEL myhash field2
    (integer) 0
    redis>
    """
    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hdel("myhash", "field1") == 1
    assert r.hdel("myhash", "field2") == 0


@delete_keys("myhash")
def test_hexists():
    """ HEXISTS key field

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HEXISTS myhash field1
    (integer) 1
    redis> HEXISTS myhash field2
    (integer) 0
    redis>
    """
    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hexists("myhash", "field1") == 1
    assert r.hexists("myhash", "field2") == 0


@delete_keys("myhash")
def test_hget():
    """ HGET key field

    redis> HSET myhash field1 "foo"
    (integer) 1
    redis> HGET myhash field1
    "foo"
    redis> HGET myhash field2
    (nil)
    redis>
    """
    assert r.hset("myhash", "field1", "foo") == 1
    assert r.hget("myhash", "field1") == "foo"
    assert r.hget("myhash", "field2") is None


@delete_keys("myhash")
def test_hgetall():
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
    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hgetall("myhash") == {"field1": "Hello", "field2": "World"}


@delete_keys("myhash")
def test_hincrby():
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
    assert r.hset("myhash", "field", 5) == 1
    assert r.hincrby("myhash", "field", 1) == 6
    assert r.hincrby("myhash", "field", -1) == 5
    assert r.hincrby("myhash", "field", -10) == -5


@delete_keys("mykey")
def test_hincrbyfloat():
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
    assert r.hset("mykey", "field", 10.50) == 1
    assert r.hincrbyfloat("mykey", "field", 0.1) == 10.6
    assert r.hset("mykey", "field", "5.0e3") == 0
    assert r.hincrbyfloat("mykey", "field", "2.0e2") == 5200.0


@delete_keys("myhash")
def test_hkeys():
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
    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hkeys("myhash") == ["field1", "field2"]


@delete_keys("myhash")
def test_hlen():
    """ HLEN key

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HSET myhash field2 "World"
    (integer) 1
    redis> HLEN myhash
    (integer) 2
    redis>
    """
    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hlen("myhash") == 2


@delete_keys("myhash")
def test_hmget():
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
    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hmget("myhash", "field1", "field2", "field3") == ["Hello", "World", None]


@delete_keys("myhash")
def test_hmset():
    """ HMSET key field value [field value ...]

    redis> HMSET myhash field1 "Hello" field2 "World"
    OK
    redis> HGET myhash field1
    "Hello"
    redis> HGET myhash field2
    "World"
    redis>
    """
    assert r.hmset("myhash", {"field1": "Hello", "field2": "World"}) is True


@delete_keys("myhash")
def test_hset():
    """ HSET key field value

    redis> HSET myhash field1 "Hello"
    (integer) 1
    redis> HGET myhash field1
    "Hello"
    redis>
    """
    assert r.hset("myhash", "field1", "Hello") == 1    # tes
    assert r.hget("myhash", "field1") == "Hello"


@delete_keys("myhash")
def test_hsetnx():
    """ HSETNX key field value

    redis> HSETNX myhash field "Hello"
    (integer) 1
    redis> HSETNX myhash field "World"
    (integer) 0
    redis> HGET myhash field
    "Hello"
    redis>
    """
    assert r.hsetnx("myhash", "field", "Hello") == 1
    assert r.hsetnx("myhash", "field", "World") == 0
    assert r.hget("myhash", "field") == "Hello"


#def test_hstrlen():
    """ HSTRLEN key field
            Available since 3.2.0.
            Time complexity: O(1)

    redis> HMSET myhash f1 HelloWorld f2 99 f3 -256
    OK
    redis> HSTRLEN myhash f1
    (integer) 10
    redis> HSTRLEN myhash f2
    (integer) 2
    redis> HSTRLEN myhash f3
    (integer) 4
    redis>
    """
#    assert r.hmset("myhash", {"f1": "HelloWorld", "f2": "99", "f3": -256}) == True
#    assert r.hstrlen("myhash", "f1") == 10
#    assert r.hstrlen("myhash", "f2") == 2
#    assert r.hstrlen("myhash", "f3") == 4


@delete_keys("myhash")
def test_hvals():
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
    assert r.hset("myhash", "field1", "Hello") == 1
    assert r.hset("myhash", "field2", "World") == 1
    assert r.hvals("myhash") == ["Hello", "World"]


@delete_keys("myhash")
def test_hscan():
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
    assert r.hmset("myhash", {"field1": "Hello", "field2": "World"}) is True
    assert r.hscan("myhash", 0) == (0, {"field1": "Hello", "field2": "World"})



###lists
#def test_blpop():
    """ BLPOP key [key ...] timeout
            Available since 2.0.0.
            Time complexity: O(1)
    """


#def test_brpop():
    """ BRPOP key [key ...] timeout
            Available since 2.0.0.
            Time complexity: O(1)

    redis> DEL list1 list2
    (integer) 0
    redis> RPUSH list1 a b c
    (integer) 3
    redis> BRPOP list1 list2 0
    1) "list1"
    2) "c"
    """


#def test_brpoplpush():
    """ BRPOPLPUSH source destination timeout
        Available since 2.2.0.
        Time complexity: O(1)
    """


@delete_keys("mylist")
def test_lindex():
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
    assert r.lpush("mylist", "World") == 1
    assert r.lpush("mylist", "Hello") == 2
    assert r.lindex("mylist", 0) == "Hello"
    assert r.lindex("mylist", -1) == "World"
    assert r.lindex("mylist", 3) is None


@delete_keys("mylist")
def test_linsert():
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
    assert r.rpush("mylist", "Hello") == 1
    assert r.rpush("mylist", "World") == 2
    assert r.linsert("mylist", "BEFORE", "World", "There") == 3
    assert r.lrange("mylist", 0, -1) == ["Hello", "There", "World"]


@delete_keys("mylist")
def test_llen():
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
    assert r.lpush("mylist", "World") == 1
    assert r.lpush("mylist", "Hello") == 2
    assert r.llen("mylist") == 2


@delete_keys("mylist")
def test_lpop():
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
    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lpop("mylist") == "one"
    assert r.lrange("mylist", 0, -1) == ["two", "three"]


@delete_keys("mylist")
def test_lpush():
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
    assert r.lpush("mylist", "world") == 1
    assert r.lpush("mylist", "hello") == 2
    assert r.lrange("mylist", 0, -1) == ["hello", "world"]


@delete_keys("mylist", "myotherlist")
def test_lpushx():
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
    assert r.lpush("mylist", "World") == 1
    assert r.lpushx("mylist", "Hello") == 2
    assert r.lpushx("myotherlist", "Hello") == 0
    assert r.lrange("mylist", 0, -1) == ["Hello", "World"]
    assert r.lrange("myotherlist", 0, -1) == []


@delete_keys("mylist")
def test_lrange():
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
    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lrange("mylist", 0, 0) == ["one"]
    assert r.lrange("mylist", -3, 2) == ["one", "two", "three"]
    assert r.lrange("mylist", -100, 100) == ["one", "two", "three"]
    assert r.lrange("mylist", 5, 10) == []


@delete_keys("mylist")
def test_lrem():
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
    assert r.rpush("mylist", "hello") == 1
    assert r.rpush("mylist", "hello") == 2
    assert r.rpush("mylist", "foo") == 3
    assert r.rpush("mylist", "hello") == 4
    assert r.lrem("mylist", "hello", -2) == 2  # lrem(self, name, value, num=0)
    assert r.lrange("mylist", 0, -1) == ["hello", "foo"]


@delete_keys("mylist")
def test_lset():
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
    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.lset("mylist", 0, "four") is True
    assert r.lset("mylist", -2, "five") is True
    assert r.lrange("mylist", 0, -1) == ["four", "five", "three"]


@delete_keys("mylist")
def test_ltrim():
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
    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.ltrim("mylist", 1, -1) is True
    assert r.lrange("mylist", 0, -1) == ["two", "three"]


@delete_keys("mylist")
def test_rpop():
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
    assert r.rpush("mylist", "one") == 1
    assert r.rpush("mylist", "two") == 2
    assert r.rpush("mylist", "three") == 3
    assert r.rpop("mylist") == "three"
    assert r.lrange("mylist", 0, -1) == ["one", "two"]


@delete_keys("{tag}mylist", "{tag}myotherlist")
def test_rpoplpush():
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
    assert r.rpush("{tag}mylist", "one") == 1
    assert r.rpush("{tag}mylist", "two") == 2
    assert r.rpush("{tag}mylist", "three") == 3
    assert r.rpoplpush("{tag}mylist", "{tag}myotherlist") == "three"
    assert r.lrange("{tag}mylist", 0, -1) == ["one", "two"]
    assert r.lrange("{tag}myotherlist", 0, -1) == ["three"]


@delete_keys("mylist")
def test_rpush():
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
    assert r.rpush("mylist", "hello") == 1
    assert r.rpush("mylist", "world") == 2
    assert r.lrange("mylist", 0, -1) == ["hello", "world"]


@delete_keys("mylist", "myotherlist")
def test_rpushx():
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
    assert r.rpush("mylist", "Hello") == 1
    assert r.rpushx("mylist", "World") == 2
    assert r.rpushx("myotherlist", "World") == 0
    assert r.lrange("mylist", 0, -1) == ["Hello", "World"]
    assert r.lrange("myotherlist", 0, -1) == []




###sets
@delete_keys("myset")
def test_sadd():
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
    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.sadd("myset", "World") == 0
    assert r.smembers("myset") == {"World", "Hello"}


@delete_keys("myset")
def test_scard():
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
    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.scard("myset") == 2


@delete_keys("{tag}key1", "{tag}key2")
def test_sdiff():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sdiff("{tag}key1", "{tag}key2") == {"a", "b"}


@delete_keys("{tag}key1", "{tag}key2", "{tag}key")
def test_sdiffstore():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sdiffstore("{tag}key", "{tag}key1", "{tag}key2") == 2
    assert r.smembers("{tag}key") == {"a", "b"}


@delete_keys("{tag}key1", "{tag}key2")
def test_sinter():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sinter("{tag}key1", "{tag}key2") == {"c"}


@delete_keys("{tag}key1", "{tag}key2")
def test_sinterstore():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sinterstore("{tag}key", "{tag}key1", "{tag}key2") == 1
    assert r.smembers("{tag}key") == {"c"}


@delete_keys("myset")
def test_sismember():
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
    assert r.sadd("myset", "one") == 1
    assert r.sismember("myset", "one") == 1
    assert r.sismember("myset", "two") == 0


@delete_keys("myset")
def test_smembers():
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
    assert r.sadd("myset", "Hello") == 1
    assert r.sadd("myset", "World") == 1
    assert r.smembers("myset") == {"World", "Hello"}


@delete_keys("{tag}myset", "{tag}myotherset")
def test_smove():
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
    assert r.sadd("{tag}myset", "one") == 1
    assert r.sadd("{tag}myset", "two") == 1
    assert r.sadd("{tag}myotherset", "three") == 1
    assert r.smove("{tag}myset", "{tag}myotherset", "two") == 1
    assert r.smembers("{tag}myset") == {"one"}
    assert r.smembers("{tag}myotherset") == {"three", "two"}


@delete_keys("myset")
def test_spop():
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
    assert r.sadd("myset", "one") == 1
    assert r.sadd("myset", "two") == 1
    assert r.sadd("myset", "three") == 1
    assert r.spop("myset") in {"one", "two", "three"}        # 随机
    #assert r.smembers("myset") == {"two", "three"}
    #assert r.sadd("myset", "four") == 1
    #assert r.sadd("myset", "five") == 1
    #assert r.spop("myset", 3) == {"five", "four", "two"}
    #assert r.smembers("myset") == {"one"}


@delete_keys("myset")
def test_srandmember():
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
    assert r.sadd("myset", "one", "two", "three") == 3
    # random
    assert r.srandmember("myset") in {"one", "two", "three"}
    #assert r.srandmember("myset", 2) in {"one", "two", "three"}
    # list for this
    #assert r.srandmember("myset", -5) == ["three", "two", "two", "three", "one"]


@delete_keys("myset")
def test_srem():
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
    assert r.sadd("myset", "one") == 1
    assert r.sadd("myset", "two") == 1
    assert r.sadd("myset", "three") == 1
    assert r.srem("myset", "one") == 1
    assert r.srem("myset", "four") == 0
    assert r.smembers("myset") == {"three", "two"}


@delete_keys("{tag}key1", "{tag}key2")
def test_sunion():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sunion("{tag}key1", "{tag}key2") == {"a", "b", "c", "d", "e"}


@delete_keys("{tag}key1", "{tag}key2", "{tag}key")
def test_sunionstore():
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
    assert r.sadd("{tag}key1", "a") == 1
    assert r.sadd("{tag}key1", "b") == 1
    assert r.sadd("{tag}key1", "c") == 1
    assert r.sadd("{tag}key2", "c") == 1
    assert r.sadd("{tag}key2", "d") == 1
    assert r.sadd("{tag}key2", "e") == 1
    assert r.sunionstore("{tag}key", "{tag}key1", "{tag}key2") == 5
    assert r.smembers("{tag}key") == {"a", "b", "c", "d", "e"}


@delete_keys("myset")
def test_sscan():
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
    assert r.sadd("myset", "a", "b", "c") == 3
    # assert r.sscan("myset", 0) == (0, ["c", "b", "a])    #
    cursor = r.sscan("myset", 0)
    assert isinstance(cursor, tuple)
    assert cursor[0] == 0
    assert sorted(cursor[1]) == sorted(["c", "b", "a"])




###sorted sets
@delete_keys("myzset")
def test_zadd():
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
    assert r.zadd("myzset", "one", 1) == 1       # reverserd
    assert r.zadd("myzset", "uno", 1) == 1
    assert r.zadd("myzset", "two", 2, "three", 3) == 2
    assert r.zrange("myzset", 0, -1, withscores=True) == [("one", 1), ("uno", 1), ("two", 2), ("three", 3)]


@delete_keys("myzset")
def test_zcard():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zcard("myzset") == 2


@delete_keys("myzset")
def test_zcount():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zcount("myzset", "-inf", "+inf") == 3
    assert r.zcount("myzset", "(1", 3) == 2


@delete_keys("myzset")
def test_zincrby():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zincrby("myzset", "one", 2) == 3
    assert r.zrange("myzset", 0, -1, withscores=True) == [("two", 2), ("one", 3)]


@delete_keys("{tag}zset1", "{tag}zset2", "{tag}out")
def test_zinterstore():
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
    assert r.zadd("{tag}zset1", "one", 1) == 1
    assert r.zadd("{tag}zset1", "two", 2) == 1
    assert r.zadd("{tag}zset2", "one", 1) == 1
    assert r.zadd("{tag}zset2", "two", 2) == 1
    assert r.zadd("{tag}zset2", "three", 3) == 1
    assert r.zinterstore("{tag}out", {"{tag}zset1": 2, "{tag}zset2": 3}) == 2
    assert r.zrange("{tag}out", 0, -1, withscores=True) == [("one", 5), ("two", 10)]


@delete_keys("myzset")
def test_zlexcount():
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
    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0) == 5
    assert r.zadd("myzset", "f", 0, "g", 0) == 2
    assert r.zlexcount("myzset", "-", "+") == 7
    assert r.zlexcount("myzset", "[b", "[f") == 5


@delete_keys("myzset")
def test_zrange():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrange("myzset", 0, -1) == ["one", "two", "three"]
    assert r.zrange("myzset", 2, 3) == ["three"]
    assert r.zrange("myzset", -2, -1) == ["two", "three"]


@delete_keys("myzset")
def test_zrangebylex():
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
    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g", 0) == 7
    assert r.zrangebylex("myzset", "-", "[c") == ["a", "b", "c"]
    assert r.zrangebylex("myzset", "-", "(c") == ["a", "b"]
    assert r.zrangebylex("myzset", "[aaa", "(g") == ["b", "c", "d", "e", "f"]


@delete_keys("myzset")
def test_zrevrangebylex():
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
    assert r.zadd("myzset", "a", 0, "b", 0, "c", 0, "d", 0, "e", 0, "f", 0, "g", 0) == 7
    assert r.zrevrangebylex("myzset", "[c", "-") == ["c", "b", "a"]
    assert r.zrevrangebylex("myzset", "(c", "-") == ["b", "a"]
    assert r.zrevrangebylex("myzset", "(g", "[aaa") == ["f", "e", "d", "c", "b"]


@delete_keys("myzset")
def test_zrangebyscore():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrangebyscore("myzset", "-inf", "+inf") == ["one", "two", "three"]
    assert r.zrangebyscore("myzset", 1, 2) == ["one", "two"]
    assert r.zrangebyscore("myzset", "(1", 2) == ["two"]
    assert r.zrangebyscore("myzset", "(1", "(2") == []


@delete_keys("myzset")
def test_zrank():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrank("myzset", "three") == 2
    assert r.zrank("myzset", "four") is None


@delete_keys("myzset")
def test_zrem():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrem("myzset", "two") == 1
    assert r.zrange("myzset", 0, -1, withscores=True) == [("one", 1), ("three", 3)]


@delete_keys("myzset")
def test_zremrangebylex():
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
    assert r.zadd("myzset", "e", 0, "d", 0, "c", 0, "b", 0, "aaaa", 0) == 5
    assert r.zadd("myzset", "alpha", 0, "ALPHA", 0, "zip", 0, "zap", 0, "foo", 0) == 5
    assert r.zrange("myzset", 0, -1) == ["ALPHA", "aaaa", "alpha", "b", "c", "d", "e", "foo", "zap", "zip"]
    assert r.zremrangebylex("myzset", "[alpha", "[omega") == 6
    assert r.zrange("myzset", 0, -1) == ["ALPHA", "aaaa", "zap", "zip"]


@delete_keys("myzset")
def test_zremrangebyrank():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zremrangebyrank("myzset", 0, 1) == 2
    assert r.zrange("myzset", 0, -1, withscores=True) == [("three", 3)]


@delete_keys("myzset")
def test_zremrangebyscore():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zremrangebyscore("myzset", "-inf", "(2") == 1
    assert r.zrange("myzset", 0, -1, withscores=True) == [("two", 2), ("three", 3)]


@delete_keys("myzset")
def test_zrevrange():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrange("myzset", 0, -1) == ["three", "two", "one"]
    assert r.zrevrange("myzset", 2, 3) == ["one"]
    assert r.zrevrange("myzset", -2, -1) == ["two", "one"]


@delete_keys("myzset")
def test_zrevrangebyscore():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrangebyscore("myzset", "+inf", "-inf") == ["three", "two", "one"]
    assert r.zrevrangebyscore("myzset", 2, 1) == ["two", "one"]
    assert r.zrevrangebyscore("myzset", 2, "(1") == ["two"]
    assert r.zrevrangebyscore("myzset", "(2", "(1") == []


@delete_keys("myzset")
def test_zrevrank():
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
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zadd("myzset", "two", 2) == 1
    assert r.zadd("myzset", "three", 3) == 1
    assert r.zrevrank("myzset", "one") == 2
    assert r.zrevrank("myzset", "four") is None


@delete_keys("myzset")
def test_zscore():
    """ ZSCORE key member
            Available since 1.2.0.
            Time complexity: O(1)

    redis> ZADD myzset 1 "one"
    (integer) 1
    redis> ZSCORE myzset "one"
    "1"
    redis>
    """
    assert r.zadd("myzset", "one", 1) == 1
    assert r.zscore("myzset", "one") == 1


@delete_keys("{tag}zset1", "{tag}zset2", "{tag}out")
def test_zunionstore():
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
    assert r.zadd("{tag}zset1", "one", 1) == 1
    assert r.zadd("{tag}zset1", "two", 2) == 1
    assert r.zadd("{tag}zset2", "one", 1) == 1
    assert r.zadd("{tag}zset2", "two", 2) == 1
    assert r.zadd("{tag}zset2", "three", 3) == 1
    assert r.zunionstore("{tag}out", {"{tag}zset1": 2, "{tag}zset2": 3}) == 3
    assert r.zrange("{tag}out", 0, -1, withscores=True) == [("one", 5), ("three", 9), ("two", 10)]


@delete_keys("zset1")
def test_zscan():
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
    assert r.zadd("zset1", "one", 1, "two", 2, "three", 3) == 3
    assert r.zscan("zset1", 0) == (0, [("one", 1), ("two", 2), ("three", 3)])





###hyperloglog
@delete_keys("hll")
def test_pfadd():
    """ PFADD key element [element ...]
            Available since 2.8.9.
            Time complexity: O(1) to add every element.

    redis> PFADD hll a b c d e f g
    (integer) 1
    redis> PFCOUNT hll
    (integer) 7
    redis>
    """
    assert r.pfadd("hll", "a", "b", "c", "d", "e", "f", "g") == 1
    assert r.pfcount("hll") == 7


@delete_keys("{tag}hll", "{tag}some-other-hll")
def test_pfcount():
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
    assert r.pfadd("{tag}hll", "foo", "bar", "zap") == 1
    assert r.pfadd("{tag}hll", "zap", "zap", "zap") == 0
    assert r.pfadd("{tag}hll", "foo", "bar") == 0
    assert r.pfcount("{tag}hll") == 3
    assert r.pfadd("{tag}some-other-hll", 1, 2, 3) == 1
    assert r.pfcount("{tag}hll", "{tag}some-other-hll") == 6


@delete_keys("{tag}hll1", "{tag}hll2", "{tag}hll3")
def test_pfmerge():
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
    assert r.pfadd("{tag}hll1", "foo", "bar", "zap", "a") == 1
    assert r.pfadd("{tag}hll2", "a", "b", "c", "foo") == 1
    assert r.pfmerge("{tag}hll3", "{tag}hll1", "{tag}hll2") is True
    assert r.pfcount("{tag}hll3") == 6


###script
@delete_keys("key1")
def test_eval():
    """ EVAL script numkeys key [key ...] arg [arg ...]
            Available since 2.6.0.
            Time complexity: Depends on the script that is executed.

    http://redis.io/commands/eval
    """
    lua = "return {KEYS[1],ARGV[1]}"
    keys_and_args = ["key1","first"]
    assert r.eval(lua, 1, *keys_and_args) == keys_and_args


#def test_evalsha():
    """ EVALSHA sha1 numkeys key [key ...] arg [arg ...]
            Available since 2.6.0.
            Time complexity: Depends on the script that is executed.
    """



###misc
#def test_auth():
    """ AUTH password
            Available since 1.0.0.
    """

#def test_echo():
    """ ECHO message
            Available since 1.0.0.

    redis> ECHO "Hello World!"
    "Hello World!"
    redis>
    """
#    assert r.echo("Hello World") == "Hello World"


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


#def test_quit():
    """ QUIT
            Available since 1.0.0.
    """
#    assert r.quit() == "OK"


#def test_select():
    """ SELECT index
            Available since 1.0.0.
    """
#    assert r.select(0) is True


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



def call(func):
    RED = "\033[31m"
    GREEN = "\033[32m"
    ENDC = "\033[0m"

    if hasattr(func, "keys"):
        r.delete(*func.keys)
    try:
        func()
    except:
        print "{} {}FAILED{}".format(func.__name__, RED, ENDC)
        raise
    else:
        print "{} {}PASSED{}".format(func.__name__, GREEN, ENDC)


def main():
    d = globals()
    for k in d.keys():
        if not k.startswith("test_"):
            continue
        func = d[k]
        if not inspect.isfunction(func):
            continue
        call(func)


if __name__ == "__main__":
    main()
