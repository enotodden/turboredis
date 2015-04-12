local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield
local task = turbo.async.task
local Connection = require("turboredis.connection")
local COMMANDS = require("turboredis.commands")
local util = require("turboredis.util")
local resp = require("turboredis.resp")

-- ## PUBSUB ##

PUBSUB_COMMANDS = {
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "UNSUBSCRIBE"
}

PubSubConnection = class("PubSubConnection", Connection)

function PubSubConnection:read_msg(callback, callback_arg)
    resp.read_resp_reply(self.stream, false, callback, callback_arg)
end

-- Start the subscriber loop.
--
-- See the Pub/Sub example for usage.
function PubSubConnection:start(callback, callback_arg)
    self.callback = callback
    self.callback_arg = callback_arg
    self.ioloop:add_callback(function ()
        while true do
            local msg = yield(task(self.read_msg, self))
            local res = {}
            res.msgtype = msg[1]
            if res.msgtype == "psubscribe" then
                res.pattern = msg[2]
                res.channel = nil
                res.data = msg[3]
            elseif res.msgtype == "punsubscribe" then
                res.pattern = msg[2]
                res.channel = nil
                res.data = msg[3]
            elseif res.msgtype == "pmessage" then
                res.pattern = msg[2]
                res.channel = msg[3]
                res.data = msg[4]
            else
                res.pattern = nil
                res.channel = msg[2]
                res.data = msg[3]
            end
            if self.callback_arg then
                self.callback(self.callback_arg, res)
            else
                self.callback(res)
            end
        end
    end)
end

-- Generate functions for all commands in `PUBSUB_COMMANDS`
--
-- See http://redis.io for documentation for specific commands.
--
for _, v in ipairs(PUBSUB_COMMANDS) do
    PubSubConnection[v:lower():gsub(" ", "_")] = function (self, ...)
        local cmd = util.flatten({v:split(" "), ...})
        local callback = false
        local callback_arg = nil
        if type(cmd[#cmd]) == "function" then
            callback = cmd[#cmd]
            cmd[#cmd] = nil
        elseif type(cmd[#cmd-1]) == "function" then
            callback = cmd[#cmd-1]
            callback_arg = cmd[#cmd]
            cmd[#cmd-1] = nil
            cmd[#cmd] = nil
        end
        if callback then
            return self:run_noreply(cmd, callback, callback_arg)
        else
            return task(self.run_noreply, self, cmd)
        end
    end
end

return PubSubConnection
