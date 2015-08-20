### UNMAINTAINED: No new development or fixes will be done on turboredis in the near future. I simply do not have the time. See the v0.1 branch for a semi-stable version.

turboredis
===========

[Redis](http://redis.io) library for [Turbo](https://github.com/kernelsauce/turbo)


- Support for most Redis commands.
- Pub/Sub support.
- No dependencies other than Turbo and Redis.
- Everything is in a single file.
- Dual turbo.async.task/callback interface for all 'normal' commands

----------

### Documentation

Docs are available on [https://enotodden.github.io/turboredis](https://enotodden.github.io/turboredis).

----------

### Example:

	local turbo = require("turbo")
	local turboredis = require("turboredis")
	local yield = coroutine.yield
	
	turbo.ioloop.instance():add_callback(function () 
		local redis = turboredis.Connection:new({host="127.0.0.1", port=6379})	
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

----------

### Tests:

`test.lua` includes basic tests for many of the commands available but 
still needs a lot of work.
