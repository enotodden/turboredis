local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield
local task = turbo.async.task
local util = require("turboredis.util")
local resp = require("turboredis.resp")
local COMMANDS = require("turboredis.commands")

-- -- -- -- -- PipeLine -- -- -- -- --
--
-- To buffer up commands to run in bulk.
--
-- Example:
--            
--      local turbo = require("turbo")
--      local turboredis = require("turboredis")
--      local yield = coroutine.yield
--      turbo.ioloop.instance():add_callback(function ()
--          local redis = turboredis.Connection:new("127.0.0.1", 6379)
--          local r = yield(redis:connect())
--          if not r then
--              print("Could not connect to Redis")
--              return
--          end
--          yield(redis:set("hello", "Hello "))
--          yield(redis:set("world", "World!"))
--
--          local pl = redis:pipeline()
--          
--          pl:get("hello")
--          pl:get("world")
--          
--          local hello, world = unpack(yield(pl:run()))
--          print(hello .. " " .. world .. "!")
--          
--          turbo.ioloop.instance():close()
--      end)
--      turbo.ioloop.instance():start()
--
--
-- Important note on replies:
--   Replies are returned 'in full'. Meaning that the 'returned' value of a
--   status reply for example is not a boolean, but a table containing
--   a boolean and the 'message' redis adds for replies.
--   Strings however that only return a single value behave 'as usual'.
--
-- Example:
--
--      pl:set("foo", "bar")
--      replies = yield(pl:run())   -- {{true, "OK"}}
--      ok = replies[1][1]          -- true
--      msg = replies[1][2]         -- "OK"
--      
--      pl:clear()
--      pl:get("foo")
--      replies = yield(pl:run())   -- {"foo"}
--      foo = replies[1]            -- "foo"
--
--
local PipeLine = class("Pipeline")
function PipeLine:initialize(con)
    self.con = con
    self.pending_commands = {}
    self.running = false
    self.buf = nil
end

-- Builds up the buffer of commands, writes it to redis and processes
-- replies.
function PipeLine:_run(callback, callback_arg)
    self.running = true
    -- Don't re-create the buffer if the user is reusing
    -- this pipeline
    if self.buf == nil then
        -- FIXME: This should probably be tweaked/configurable
        self.buf = turbo.structs.buffer(128*#self.pending_commands)
    end
    for i, cmdt in ipairs(self.pending_commands) do
        local cmdstr = resp.pack(cmdt)
        self.buf:append_luastr_right(cmdstr)
    end
    self.con.stream:write_buffer(self.buf, function ()
        local replies = {}
        for i, v in ipairs(self.pending_commands) do
            local res = yield(task(resp.read_resp_reply,
                self.con.stream, false))
            replies[#replies+1] = res
        end
        self.running = false
        if callback_arg then
            callback(callback_arg, replies)
        else
            callback(replies)
        end
    end)
end

-- Wrap _run in dual yield/callback interface
function PipeLine:run(callback, callback_arg)
    if self.running then
        error("Pipeline already running")
    end
    if callback then
        return self:_run(callback, callback_arg)
    else
        return task(self._run, self)
    end
end

-- Remove the previously added command
function PipeLine:undo()
    if self.running then
        error("Pipeline running")
    end
    self.pending_commands[#self.pending_commands] = nil
end

-- Clear the pipeline of pending commands.
-- Allows for reuse.
function PipeLine:clear()
    if self.running then
        error("Pipeline running")
    end
    if self.buf then
        self.buf:clear(true)
    end
    self.pending_commands = {}
end

function PipeLine:cmd(cmd)
    if self.running then
        error("Pipeline running")
    end
    self.pending_commands[#self.pending_commands+1] = cmd
end

-- Generate functions for all commands in `turboredis.COMMANDS` just like
-- turboredis.Connection().
--
-- This applies to all commands except for
-- `SUBSCRIBE/UNSUBSCRIBE` pubsub commands.
--
for _, v in ipairs(COMMANDS) do
    -- Add command+arguments to the list of pending commands
    PipeLine[v:lower():gsub(" ", "_")] = function (self, ...)
        if self.running then
            error("Pipeline running")
        end
        local cmd = util.flatten({v:split(" "), ...})
        self.pending_commands[#self.pending_commands+1] = cmd
    end
end

return PipeLine
