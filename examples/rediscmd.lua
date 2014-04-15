--
-- TurboRedis run() example.
--
-- Shows how use turboredis.Connection:run() to make a very simple command line
-- redis client.
--
--

local turbo = require("turbo")
local turboredis = require("turboredis")
local yield = coroutine.yield

local USAGE = "Usage: luajit rediscmd.lua COMMAND [SUBCOMMAND] [ARGUMENTS ...]"

turbo.ioloop.instance():add_callback(function () 
	local redis = turboredis.Connection:new("127.0.0.1", 6379)	
	local r = yield(redis:connect())
	if not r then
		print("Could not connect to Redis")
		return
	end

    -- No command, print usage.
    if #arg == 0 then
        print(USAGE)
        turbo.ioloop.instance():close()
    end
	
    -- Copy relevant arguments into cmd.
    local cmd = {}
    for i=1,#arg do
        cmd[#cmd+1] = arg[i]
    end

    -- Run the command and print the result.
    redis:run(cmd, function (r, x)
        print(r, x)
        turbo.ioloop.instance():close()
    end)

end)
turbo.ioloop.instance():start()
