local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield
local task = turbo.async.task
local COMMANDS = require("turboredis.commands")
local util = require("turboredis.util")
local resp = require("turboredis.resp")

-- ## Command ##
--
-- Created with the IOStream instance of the `Connection`
--
Command = class("Command")
function Command:initialize(cmd, stream, opts)
    self.ioloop = turbo.ioloop.instance()
    self.cmd = cmd
    self.cmdstr = resp.pack(cmd)
    self.stream = stream
end

-- Handle a reply from Redis.
--
function Command:_handle_reply(res)
    if self.callback_arg then
        self.callback(self.callback_arg, unpack(res))
    else
        self.callback(unpack(res))
    end
end

function Command:execute(callback, callback_arg)
    self.callback = callback
    self.callback_arg = callback_arg
    self.stream:write(self.cmdstr, function()
        resp.read_resp_reply(self.stream, true, self._handle_reply, self)
    end)
end

-- Execute the command, but unlike :execute() we don't try to
-- read a reply.
--
-- This is useful for SUBSCRIBE/UNSUBSCRIBE commands which 'replies'
-- through PubSub messages.
--
function Command:execute_noreply(callback, callback_arg)
    self.stream:write(self.cmdstr, function()
        if callback_arg then
            callback(callback_arg, true)
        else
            callback(true)
        end
    end)
end

return Command
