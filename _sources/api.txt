Usage / API
===========

.. highlight:: lua
   :linenothreshold: 10


Loading TurboRedis
------------------

::

    local turbo = require("turbo")
    local turboredis = require("turboredis")


Creating a Redis connection
---------------------------

Creating a new connection to the default host and port:

::

    local redis = turboredis.Connection:new()


Setting your own host and/or port:

::

    local redis = turboredis.Connection:new({host="myredisbox", port=8765})


Setting just the port:

::
    
    local redis = turboredis.Connection:new({port=8765})


Connecting
----------

::

    local redis = turboredis.Connection:new()
    turbo.ioloop.instance():add_callback(function ()
        local ok = yield(redis:connect())
        if not ok then
            error("Coul not connect to Redis")
        end
    end)

The ``turboredis.Connection`` is not automatically connected when created because the
``connect()`` must be done in the context of Turbo's ``IOLoop``.

Connections have no relationship to eachother and only shares the
global Turbo ``IOLoop``, so commands can be executed
in parallel on multiple connections at the same time without problems. 

The default connect timeout is 5 seconds, and can be changed by passing
the ``connect_timeout`` option to the ``turboredis.Connection:new()`` :

::

    -- Set connect() timeout to 10 seconds instead of the default
    local redis = turboredis.Connection:new({connect_timeout=10})
    turbo.ioloop.instance():add_callback(function ()
        local ok = yield(redis:connect())
        if not ok then
            error("Coul not connect to Redis")
        end
    end)



Executing Commands
------------------

Functions for most of the commands that TurboRedis implements are dynamically
generated from a list of commands and named with lowercase letters.
Spaces between command names (subcommands) are replaced by an underscore.

Example (assuming ``redis`` is the variable holding the Redis connection):

::

    GET foo             ->    yield(redis:get("foo"))
    
    CONFIG GET port     ->    yield(redis:config_get("port"))
    
    BITOP OR dst a1 a2  ->    yield(redis:bitop_or("dst", "a1", "a2"))

    FLUSHDB             ->    yield(redis:flushdb())


TurboRedis does absolutely no checking on the number of arguments to command
functions or their validity. This is by choice since some commands have been
extended in never versions of Redis. This allows for TurboRedis to be used
with the bleeding edge Redis and older stable versions
as long as Redis itself does not break backward compatability in some major way.

All command arguments are converted to strings, so any object that supports being
converted to a string using the Lua ``tostring()`` method can be used as an
argument.


Using callbacks instead of ``coroutine.yield``
----------------------------------------------

All command functions can be 'called' in 2 ways. With coroutine.yield or
with a callback and optional callback argument.

If no callback is specified, TurboRedis returns a ``turbo.async.task`` for
yielding.

Example, ``GET`` with and without callback:

::

    local val = yield(redis:get("foo"))
    print("Value is: ", val)



::
    
    redis:get("foo", function (val)
        print("Value is: ", val)
    end)


A user-defined callback argument can also be passed as the last argument
to command functions. The callback function will then be called with this
value as it's first argument.

::

    redis:get("foo", function (foo, val)
        print("FOO", foo)
        print("Value is:", val)
    end, "bar") -- <- Passing "bar" as the callback argument


Running 'unsupported' commands
------------------------------

If new commands are added and your version of TurboRedis is not updated
with the new commands, they can still be run using ``turboredis.Connection:runc()``

::

    x = yield(redis:runc({"GET", "foo"}))

This can also be useful if using TurboRedis to connect to other software
that implements the Redis protocol, or proxies with extended functionality.


Purist Mode
-----------


TurboRedis tries to make life easier by formatting
some of the replies from different commands.


- Integer replies (``0|1``) from commands like ``EXISTS`` and ``SETNX``
  are converted to booleans.
- Results from commands like ``CONFIG GET`` and ``HGETALL`` 
  are converted from lists to key-value tables.
- Results from some commands (``INCRFLOAT``) are converted to numbers.

This behavoiour is not very well tested, and can be disabled (purist mode)
by passing ``purist=true`` to ``turboredis.Connection:new()``.

::

    con = turboredis.Connection:new({purist=true})


