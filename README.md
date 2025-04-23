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

---
[1]: https://redis.io/docs/latest/develop/reference/protocol-spec/
[2]: https://rustup.rs/
