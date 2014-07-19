-- Load Turbo and TurboRedis
local turbo = require("turbo")
local turboredis = require("turboredis")
 
-- Create a Redis connection
local redis = turboredis.Connection:new()
 
-- Create a new requesthandler for our simple web application
-- The 'class' function called here is from the 'middleclass'
-- Lua class library that Turbo loads into the global scope
local HelloRedisHandler = class("RedisHandler", turbo.web.RequestHandler)
 
-- Handler function for the HTTP GET method
function HelloRedisHandler:get()
    -- Get the hello world message
    local msg = coroutine.yield(redis:get("msg"))
     
    -- Write it to the client
    self:write("<h1>" .. msg .. "</h1>\n")
     
    -- Increment the visits counter (INCR returns the new value)
    local n_visits = coroutine.yield(redis:incr("visits"))
     
    -- Show the number of visits
    self:write(string.format("This is visit number %d\n", n_visits))
end
 
-- Create our Application object with a single route to our handler
local app = turbo.web.Application({
    {"^/", HelloRedisHandler}
})
-- .. and set it to listen on port 8888 (localhost)
app:listen(8888)
 
turbo.ioloop.instance():add_callback(function ()
    -- Connect to Redis running on 127.0.0.1:6379
    coroutine.yield(redis:connect())
    -- Set the message
    coroutine.yield(redis:set("msg", "Hello World!!!"))
end)
 
turbo.ioloop.instance():start() -- Start the IOLoop
