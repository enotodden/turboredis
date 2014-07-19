local turbo = require("turbo")
local turboredis = require("turboredis")
 
local redis = turboredis.Connection:new()
 
local HelloRedisHandler = class("RedisHandler", turbo.web.RequestHandler)
 
function HelloRedisHandler:get()
    local msg = coroutine.yield(redis:get("msg"))
    self:write(msg .. "\n")
end
 
local app = turbo.web.Application({
    {"^/", HelloRedisHandler}
})
app:listen(8888)
 
turbo.ioloop.instance():add_callback(function ()
    coroutine.yield(redis:connect())
    coroutine.yield(redis:set("msg", "Hello World!!!"))
end)
 
turbo.ioloop.instance():start()
