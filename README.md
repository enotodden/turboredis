turboredis
===========

[Redis](http://redis.io) library for [Turbo](https://github.com/kernelsauce/turbo)


- Support for most Redis commands (see roadmap)
- Pub/Sub support
- No dependencies other than Turbo and Redis
- Everything is in a single file.
- Dual turbo.async.task/callback interface for all 'normal' commands

----------

### Get started:

	local turbo = require("turbo")
	local turboredis = require("turboredis")
	local yield = coroutine.yield
	
	turbo.ioloop.instance():add_callback(function () 
		local redis = turboredis.Connection:new("127.0.0.1", 6379)	
		local r = yield(redis:connect())
		if not r then
			print("Could not connect to Redis")
			return
		end
		
		yield(redis:set("hello", "Hello "))
		yield(redis:set("world", "World!"))

		print("## " .. yield(redis:get("hello")) .. yield(redis:get("world")))

		turbo.ioloop.instance():close()
	end)
	turbo.ioloop.instance():start()


### Pub/Sub:

	local turbo = require("turbo")
	local turboredis = require("turboredis")
	local yield = coroutine.yield
	local ioloop = turbo.ioloop.instance()

	ioloop:add_callback(function () 
		-- Create a normal redis connection for publishing
		local pubcon = turboredis.Connection:new("127.0.0.1", 6379)	
		
		-- Create a PubSub Connection for subscribing
		-- This has the subscriber commands
		local subcon = turboredis.PubSubConnection:new("127.0.0.1", 6379)

		-- Connect both
		yield(pubcon:connect())
		yield(subcon:connect())

		-- Subscribe to the channel 'hello.msgs'
		yield(subcon:subscribe("hello.msgs"))

		-- Wait for messages.
		-- After start() is called, no commands other than
		-- subscribe/unsubscribe commands can be used.
		subcon:start(function (msg)
			print("NEW MESSAGE:")
			print("  Message type: " .. msg.msgtype)	
			print("  Channel: " .. msg.channel)
			print("  Data: " .. msg.data)
			print("--")

			-- If the message is 'exit', close the IOLoop
			if msg.data == "exit" then
				ioloop:close()
			end
		end)

		-- Publish messages
		yield(pubcon:publish("hello.msgs", "Hello "))
		
		ioloop:add_timeout(turbo.util.gettimemonotonic() + 1000, function () 
			yield(pubcon:publish("hello.msgs", "World!!"))
		end)

		ioloop:add_timeout(turbo.util.gettimemonotonic() + 2000, function () 
			yield(pubcon:publish("hello.msgs", "exit"))
		end)
	end)

	ioloop:start()


### Sub-Commands:

Commands with sub-commands like `PUBSUB` are implemented as separate command.

`PUBSUB CHANNELS` == `redis:pubsub_channels()`


### Dual yield/callback interface:

Command functions can be called using coroutine.yield:

	res, msg = coroutine.yield(redis:set("foo", "bar"))

Or with a callback:
	
	redis:set("foo", "bar", function (res, msg) 
		print(res, msg)
	end)

There's no need to wrap the call in `turbo.async.task` for the `yield` version
since this is done internally.


### Purist mode:

TurboRedis tries to make life simpler for the user by formatting
some of the replies from different commands.

- Integer replies (`0|1`) from commands like `EXISTS` and `SETNX`
  are converted to booleans.
- Results from commands like `CONFIG GET` and `HGETALL` 
  are converted from lists to key-value tables.
- Results from some commands (`INCRFLOAT`) are converted to numbers.

This behavior can be disabled (purist mode) by setting `turboredis.purist = true`.


### Tests:

`test.lua` includes basic tests for many of the commands available but still needs a lot of work.


### Roadmap:

- Add support for `PF*` commands.
- Make 'reply formatting' more consistent across commands.
- Write some documentation.
- Add support for the `MONITOR` command.
- Make a nice interface for Redis Transactions (`MULTI/EXEC/WATCH`)
