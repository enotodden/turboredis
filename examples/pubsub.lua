local turbo = require("turbo")
local turboredis = require("turboredis")
local yield = coroutine.yield
local ioloop = turbo.ioloop.instance()

ioloop:add_callback(function () 
	-- Create a normal redis connection for publishing
	local pubcon = turboredis.Connection:new({host="127.0.0.1", port=6379})	
	
	-- Create a PubSub Connection for subscribing
	-- This has the subscriber commands
	local subcon = turboredis.PubSubConnection:new({host="127.0.0.1", port=6379})

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
