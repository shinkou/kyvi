# kyvi

kyvi is a simple key-value pair database

## Description

This project was primarily created for study purposes. Since then, it has
evolved and adopted many of the features from Redis. It is also compatible
(not 100%) with [Redis serialization protocol][1]. In fact, we can access
its service through compatible libraries and clients.

## How to Run

### Install on local

You will need the [rustup toolchain][2]. Once you have it, go to the top
level of the checked out directory and install:

```
$ cargo install --path YOUR\_DESIRED\_PATH
```

Then make sure _YOUR\_DESIRED\_PATH_/bin is in your path. And run:

```
$ kyvi
```

You can also take a look at the adjustable command line parameters by
running:

```
$ kyvi -h
```

### Docker Compose for Try and Debug

Make sure you have docker-compose installed. Then bring up the stack:

```
$ docker-compose up
```

In a separate terminal window, invoke the client like this:

```
$ docker-compose run kyvi-cli
```

Then you will land in __redis-cli__ where you can issue and try out the
commands immediately.

## Redis Compatibility

### Implemented Commands

| Command     | Description                                                                                                             |
|-------------|-------------------------------------------------------------------------------------------------------------------------|
| append      | append value to the string stored at the key                                                                            |
| decr        | decrement the integer value associated with the key                                                                     |
| decrby      | decrement value stored at the key by the integer provided                                                               |
| del         | remove the value associated with the key(s)                                                                             |
| get         | obtain value associated with the key                                                                                    |
| getdel      | obtain the value of the key and delete it                                                                               |
| getset      | obtain the value of the key and set it to a new value                                                                   |
| hdel        | remove specified fields existed in the hash stored at key                                                               |
| hexists     | return 1 if field exists, 0 if not, in the hash stored at key                                                           |
| hget        | get specified field from the hash stored at key                                                                         |
| hgetall     | get all fields and values from the hash stored at key                                                                   |
| hincrby     | increment the numerical value of the field in the hash stored at key by increment                                       |
| hkeys       | get all field names in the hash stored at key                                                                           |
| hlen        | get the number of elements in the hash stored at key                                                                    |
| hmget       | get values associated with the fields in the hash stored at key                                                         |
| hmset       | set specified fields to values in the hash stored at key                                                                |
| hset        | set specified fields to values in the hash stored at key                                                                |
| hsetnx      | set specified non-existing fields to values in the hash stored at key                                                   |
| hvals       | get all field values in the hash stored at key                                                                          |
| incr        | increment the integer value associated with the key                                                                     |
| incrby      | increment value stored at the key by the integer provided                                                               |
| info        | display system info                                                                                                     |
| keys        | list keys matching the REGEX pattern                                                                                    |
| lindex      | get element at the index from the list stored at the key                                                                |
| linsert     | insert element before or after the pivot in the list stored                                                             |
| llen        | get the length of the list stored at the key                                                                            |
| lpop        | remove and return the values from the beginning of the list stored at key                                               |
| lpush       | insert all the specified values at the beginning of the list stored at key                                              |
| lpushx      | insert all the specified values at the beginning of the list stored at key                                              |
| lrange      | get elements from start to stop of the list stored at key                                                               |
| lrem        | remove the first count occurrences of element from the list stored at key                                               |
| lset        | set element at the index in the list stored at key                                                                      |
| ltrim       | trim the list stored at key                                                                                             |
| mget        | get values stored at specified keys                                                                                     |
| mset        | store values with the specified keys                                                                                    |
| quit        | close current connection and quit                                                                                       |
| rpop        | remove and return the values from the end of the list stored at key                                                     |
| rpush       | append all the specified values at the end of the list stored at key                                                    |
| rpushx      | append all the specified values at the end of the list stored at key                                                    |
| sadd        | add specified values to the set stored at key                                                                           |
| scard       | get the cardinality of the set stored at key                                                                            |
| sdiff       | get members that only exist in the set stored at the first key                                                          |
| sdiffstore  | get members that only exist in the set stored at the first key and store them in a new set stored at the destination    |
| set         | record the given key value pair                                                                                         |
| sismember   | return if the specified value is a member of the set stored at key                                                      |
| smembers    | get all values in the set stored at key                                                                                 |
| smismember  | return if the specified values are members of the set stored at key                                                     |
| smove       | move value from the set at source to the set at destination                                                             |
| sinter      | get values that exist in all of the sets stored at the given keys                                                       |
| sinterstore | get values that exist in all of the sets stored at the given keys and store them in a new set stored at the destination |
| spop        | remove and return one or more random values from the set stored at key                                                  |
| srandmember | get a number of random members from the set stored at key                                                               |
| srem        | remove specified values from the set stored at key                                                                      |
| sunion      | get all unique values from all sets stored by the given keys                                                            |
| sunionstore | get all unique values from all sets stored by the given keys and store them in a new set at destination                 |

---
[1]: https://redis.io/docs/latest/develop/reference/protocol-spec/
[2]: https://rustup.rs/
