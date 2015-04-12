local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield
local task = turbo.async.task
local COMMANDS = require("turboredis.commands")
local util = require("turboredis.util")
local resp = {}

-- Convert a table of command+arguments to redis format.
function resp.pack(t)
    local out = "*" .. tostring(#t) .. "\r\n"
    for _, v in ipairs(t) do
        out = out .. "$" .. tostring(string.len(v)) .. "\r\n" .. v .. "\r\n"
    end
    return out
end

-- Read a Redis reply
--
-- Parameters:
--
-- - stream[IOStream]: The IOStream object to use
-- - wrap[bool]: Wether or not to wrap the result in a table (if the reply
--   is not an error or 'simple string' reply.
--
function resp.read_resp_reply (stream, wrap, callback, callback_arg)
    turbo.ioloop.instance():add_callback(function ()
        local part
        local first
        local len
        local data
        local is_ss = false

        -- Read the first line of the reply to figure out
        -- reply type and length.
        part = yield(task(stream.read_until, stream, "\r\n"))
        first = part:sub(1,1)

        -- Handle a 'simple string' or error reply.
        if first == "+" or first == "-" then
            is_ss = true
            res = {first == "+", part:sub(2, part:len()-2)}
        -- Handle a 'bulk string' reply.
        elseif first == "$" then
            len = tonumber(part:sub(2, part:len()-2))
            if len == -1 then
                res = nil
            else
                data = yield(task(stream.read_bytes, stream, len+2))
                res = data:sub(1, data:len()-2)
            end
        -- Handle an array reply, we call resp.read_resp_array_reply
        -- if the length is not -1 (nil)
        elseif first == "*" then
            len = tonumber(part:sub(2, part:len()-2))
            if len == -1 then
                res = nil
            else
                res = yield(task(resp.read_resp_array_reply, stream, len))
            end
        -- Handle an integer reply
        elseif first == ":" then
            res = tonumber(part:sub(2, part:len()-2))
        else
            -- Should never get here, but if we do, we fail in
            -- the same way as with an error reply.
            res = {false, "turboredis: Error in reply from redis"}
        end
        if (not is_ss) and wrap then
            -- Wrap result in a table if not a 'simple string' or 'error' reply
            res = {res}
        end
        if callback_arg then
            callback(callback_arg, res)
        else
            callback(res)
        end
    end)
end

-- Read a redis array reply.
-- Calls resp.read_resp_reply() on each element.
--
-- Parameters:
--
-- - stream[IOStream]: The IOStream object to use
-- - n[int]: Number of elements in the array reply.
--
function resp.read_resp_array_reply(stream, n, callback, callback_arg)
    stream.io_loop:add_callback(function ()
        local out = {}
        local i = 0
        while i < n do
            out[#out+1] = yield(task(resp.read_resp_reply, stream, false))
            i = i + 1
        end
        if callback_arg then
            callback(callback_arg, out)
        else
            callback(out)
        end
    end)
end

return resp
