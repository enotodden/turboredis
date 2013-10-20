local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield

turboredis = {
    -- If purist is set to true
    -- turboredis will not convert key-value pair lists to dicts
    -- and will not convert integer replies from certain commands to
    -- booleans for convenience.
    purist=false 
} 

turboredis.COMMANDS = {
    "APPEND",
    -- AUTH, Custom handling in BaseConnection to set pwd so that 
    -- we can 'duplicate' a connection
    "BGREWRITEAOF",
    "BGSAVE",
    "BITCOUNT",
    "BITOP AND",
    "BITOP OR",
    "BITOP XOR",
    "BITOP NOT",
    -- "BLPOP",
    -- "BRPOP",
    -- "BRPOPLPUSH", 
    "CLIENT KILL",
    "CLIENT LIST", -- FIXME: Should parse this..
    "CLIENT GETNAME",
    "CLIENT SETNAME",
    "CONFIG GET",
    -- "CONFIG REWRITE",
    "CONFIG SET",
    "CONFIG RESETSTAT",
    "DBSIZE",
    -- "DEBUG OBJECT",
    -- "DEBUG SEGFAULT",
    "DECR",
    "DECRBY",
    "DEL",
    "DISCARD",
    "DUMP",
    "ECHO",
    "EVAL",
    "EVALSHA",
    "EXEC",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "FLUSHALL",
    "FLUSHDB",
    "GET",
    "GETBIT",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HSET",
    "HSETNX",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INFO",
    "KEYS",
    "LASTSAVE",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MGET",
    "MIGRATE",
    -- MONITOR
    "MOVE",
    "MSET",
    "MSETNX",
    "MULTI",
    "OBJECT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PING",
    "PSETEX",
    -- PSUBSCRIBE
    "PUBSUB",
    "PTTL",
    "PUBLISH",
    -- PUNSUBSCRIBE
    "QUIT",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "RESTORE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCARD",
    "SCRIPT EXISTS",
    "SCRIPT FLUSH",
    "SCRIPT KILL",
    "SCRIPT LOAD",
    "SDIFF",
    "SDIFFSTORE",
    -- SELECT, Custom handling in BaseConnection to set pwd so that 
    -- we can 'duplicate' a connection
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SHUTDOWN",
    "SINTER",
    "SINTERSTORE",
    "SISMEMBER",
    "SLAVEOF",
    "SLOWLOG",
    "SMEMBERS",
    "SMOVE",
    "SORT",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "STRLEN",
    -- SUBSCRIBE
    "SUNION",
    "SUNIONSTORE",
    "SYNC",
    "TIME",
    "TTL",
    "TYPE",
    -- UNSUBSCRIBE
    "UNWATCH",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZINTERSTORE",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCORE",
    "ZUNIONSTORE"
}

function turboredis.pack(t)
    local out = "*" .. tostring(#t) .. "\r\n"
    for _, v in ipairs(t) do
        out = out .. "$" .. tostring(string.len(v)) .. "\r\n" .. v .. "\r\n"
    end
    return out
end

function turboredis.from_kvlist(inp)
    local out={}
    local o = false
    for i, v in ipairs(inp) do
        if o then
            out[inp[i-1]] = v
        end
        o = not o
    end
    return out
end

function turboredis.flatten(t)
    if type(t) ~= "table" then return {t} end
    local flat_t = {}
    for _, elem in ipairs(t) do
        for _, val in ipairs(turboredis.flatten(elem)) do
            flat_t[#flat_t + 1] = val
        end
    end
    return flat_t
end

function turboredis.read_bulk_reply(iostream, len)
    local ctx = turbo.coctx.CoroutineContext:new(iostream.io_loop)
    iostream:read_bytes(len, function (data) 
        iostream:read_bytes(2, function() 
            ctx:set_state(turbo.coctx.states.DEAD)
            ctx:set_arguments({data})
            ctx:finalize_context()
        end)
    end)
    ctx:set_state(turbo.coctx.states.WAIT_COND)
    return ctx
end

-- Wraps read_until_pattern in coctx
function turboredis.read_until_pattern(iostream, pattern)
    local ctx = turbo.coctx.CoroutineContext:new(iostream.io_loop)
    iostream:read_until_pattern(pattern, function (data) 
        ctx:set_state(turbo.coctx.states.DEAD)
        ctx:set_arguments({data})
        ctx:finalize_context()
    end)
    ctx:set_state(turbo.coctx.states.WAIT_COND)
    return ctx
end

function turboredis.read_multibulk_reply(iostream, num_replies)
    local ctx = turbo.coctx.CoroutineContext:new(iostream.io_loop)
    iostream.io_loop:add_callback(function()
        local out = {}
        local data
        local prefix
        local len
        for i=1, num_replies do
            data = yield(turboredis.read_until_pattern(iostream, "\r\n"))
            prefix = data:sub(1,1)
            if prefix == "$" then
                if len == -1 then
                    table.insert(out, nil)
                else
                    len = tonumber(data:strip():sub(2))
                    data = yield(turboredis.read_bulk_reply(iostream, len))
                    table.insert(out, data)
                end
            elseif prefix == ":" then -- integer
                table.insert(out, tonumber(data:strip():sub(2)))
            elseif prefix == "*" then
                table.insert(out, yield(turboredis.read_multibulk_reply(
                    iostream, tonumber(data:strip():sub(2)))))
            elseif prefix == "+" then
                table.insert(out, true)
            elseif prefix == "-" then
                table.insert(out, false)
            else
                table.insert(out, nil, "Could not parse multibulk reply " ..
                    "from redis")
            end

            if #out == num_replies then
                ctx:set_state(turbo.coctx.states.DEAD)
                ctx:set_arguments({out})
                ctx:finalize_context()
            end
        end
    end)

    ctx:set_state(turbo.coctx.states.WAIT_COND)
    return ctx
end

turboredis.Command = class("Command")
function turboredis.Command:initialize(cmd, iostream)
    self.ioloop = turbo.ioloop.instance()
    self.cmd = cmd
    self.cmdstr = turboredis.pack(cmd)
    self.iostream = iostream
end

function turboredis.Command:_format_res(res)
    local out = res
    if not turboredis.purist then
        if self.cmd[1] == "CONFIG" then
            if self.cmd[2] == "GET" then
                out = {turboredis.from_kvlist(res[1])}
            end
        elseif self.cmd[1] == "HGETALL" then
            out = {turboredis.from_kvlist(res[1])}
        else
            if self.cmd[1] == "EXISTS" or self.cmd[1] == "EXPIRE" or 
                self.cmd[1] == "EXPIREAT" or self.cmd[1] == "HEXISTS" or
                self.cmd[1] == "HSETNX" then
                out = {res[1] == 1}
                return out
            end
        end
    end
    return out
end

function turboredis.Command:_handle_reply(prefix)
    function done(arg)
        self.coctx:set_state(turbo.coctx.states.DEAD)
        self.coctx:set_arguments(self:_format_res(arg))
        self.coctx:finalize_context()
    end

    if prefix == "+" then -- status
        self.iostream:read_until_pattern("\r\n", function (data)
            data = data:strip()
            done({true, data})
        end)
    elseif prefix == "-" then -- error
        self.iostream:read_until_pattern("\r\n", function (data)
            data = data:strip()
            done({false, data})
        end)
    elseif prefix == ":" then -- integer
        self.iostream:read_until_pattern("\r\n", function (data)
            done({tonumber(data:strip())})
        end)
    elseif prefix == "$" then
        self.iostream:read_until_pattern("\r\n", function (data)
            local len = tonumber(data:strip())
            if len == -1 then
                done({nil})
            else
                local reply = yield(turboredis.read_bulk_reply(self.iostream,
                                                               len))
                done({reply})
            end
        end)
    elseif prefix == "*" then
        self.iostream:read_until_pattern("\r\n", function(data)
            local num_replies = tonumber(data:strip())
            if num_replies == -1 then
                done({nil})
            else
                local reply = yield(turboredis.read_multibulk_reply(
                    self.iostream, num_replies))
                done({reply})
            end
        end)
    else
        done({nil, "Could not parse reply from redis."})
    end
end

function turboredis.Command:execute()
    self.coctx = turbo.coctx.CoroutineContext:new(self.ioloop)
    self.iostream:write(self.cmdstr, function() 
        self.iostream:read_bytes(1, self._handle_reply, self)
    end)
    self.coctx:set_state(turbo.coctx.states.WAIT_COND)
    return self.coctx
end



turboredis.BaseConnection = class("BaseConnection")
function turboredis.BaseConnection:initialize(host, port)
    self.host = host or "127.0.0.1"
    self.port = port or 6379
    self.family = 2
    self.ioloop = io_loop or turbo.ioloop.instance()
    self.connect_timeout = 5
    self.authenticated = false
    self.selected = false
    self.pwd = nil
    self.dbid = nil
end

function turboredis.BaseConnection:_connect_done(arg)
    self.connect_timeout_ref = nil
    self.connect_coctx:set_state(turbo.coctx.states.DEAD)
    self.connect_coctx:set_arguments(arg)
    self.connect_coctx:finalize_context()
end

function turboredis.BaseConnection:_handle_connect_timeout()
    self:_connect_done({false, {err=-1, msg="Connect timeout"}})
end

function turboredis.BaseConnection:_handle_connect_error(err, strerror)
    self.ioloop:remove_timeout(self.connect_timeout_ref)
    self:_connect_done({false, {err=err, msg=strerror}})
end

function turboredis.BaseConnection:_handle_connect()
    self.ioloop:remove_timeout(self.connect_timeout_ref)
    self:_connect_done({true})
end

function turboredis.BaseConnection:connect(timeout)
    self.ioloop = turbo.ioloop.instance()
    self.connect_coctx = turbo.coctx.CoroutineContext:new(self.ioloop)
    self.connect_coctx:set_state(turbo.coctx.states.WORKING)
    self.sock, msg = turbo.socket.new_nonblock_socket(self.family,
                                                      turbo.socket.SOCK_STREAM,
                                                      0)
    self.iostream = turbo.iostream.IOStream:new(self.sock, self.ioloop, 4096)
    local rc, msg = self.iostream:connect(self.host, 
                                          self.port,
                                          self.family, 
                                          self._handle_connect,
                                          self._handle_connect_error,
                                          self)
    if rc ~= 0 then
        error("Connect failed")
        return -1 --wtf
    end
    self.cmd = cmd 
    timeout = (timeout or self.connect_timeout) * 1000 +
        turbo.util.gettimeofday()
    self.connect_timeout_ref = self.ioloop:add_timeout(timeout,
        self._handle_connect_timeout, self)
    self.connect_coctx:set_state(turbo.coctx.states.WAIT_COND)
    return self.connect_coctx
end

function turboredis.BaseConnection:run(c)
    return turboredis.Command:new(c, self.iostream):execute()
end

function turboredis.BaseConnection:auth(pwd)
    self.authenticated = true
    self.pwd = pwd
    return turboredis.Command:new({"AUTH", pwd}, self.iostream):execute()
end

function turboredis.BaseConnection:select(dbid)
    self.selected = true
    self.dbid = dbid
    return self:run({"SELECT", dbid})
end

turboredis.Connection = class("Connection", turboredis.BaseConnection)
for _, v in ipairs(turboredis.COMMANDS) do
    turboredis.Connection[v:lower():gsub(" ", "_")] = function(self, ...)
        local cmd = turboredis.flatten({v:split(" "), ...})
        return self:run(cmd)
    end
end

return turboredis
