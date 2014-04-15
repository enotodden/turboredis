-- TurboRedis example.
--
-- Shows how to use redis within a Turbo application.
--
local turbo = require("turbo")
local turboredis = require("turboredis")
local yield = coroutine.yield

local redis = turboredis.Connection:new()

local RedisHandler = class("RedisHandler", turbo.web.RequestHandler)

function RedisHandler:get(key)
    if not key or key == "" then
        error(turbo.web.HTTPError(400, "No key."))
    end
    r = yield(redis:get(key))
    self:write(r)
    self:write("\n")
end

function RedisHandler:post(key)
    if not key or key == "" then
        error(turbo.web.HTTPError(400, "No key."))
    end
    local val = self:get_argument("value")
    if not val then
        error(turbo.web.HTTPError(400, "No value."))
    end
    local r, msg = yield(redis:set(key, val))
    if not r then
        error(turbo.web.HTTPError(500, msg))
    end
    self:write("OK\n")
end

turbo.web.Application({{"^/(.-)$", RedisHandler}}):listen(8888)

turbo.ioloop.instance():add_callback(function ()
    yield(redis:connect())
end)

print([[

Try Me!

# Set foo to 'bar'
curl -X POST localhost:8888/foo\?value=bar

# Get the key 'foo'
curl localhost:8888/foo

]])

turbo.ioloop.instance():start()
