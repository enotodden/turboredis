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

     local pl = redis:pipeline()
     
     pl:get("hello")
     pl:get("world")

     local hello, world = unpack(yield(pl:run()))
     print(hello .. world)
     
     turbo.ioloop.instance():close()
end)
turbo.ioloop.instance():start()
