local turbo = require("turbo")
local middleclass =  require("turbo.3rdparty.middleclass")
local ffi = require("ffi")
local yield = coroutine.yield

-- Range iterator from http://lua-users.org/wiki/RangeIterator
local function range(from, to, step)
    step = step or 1
    return function(_, lastvalue)
        local nextvalue = lastvalue + step
        if step > 0 and nextvalue <= to or step < 0 and nextvalue >= to or
             step == 0
        then
            return nextvalue
        end
    end, nil, from - step
end

turboredis = {
    -- If purist is set to true
    -- turboredis will not convert key-value pair lists to dicts
    -- and will not convert integer replies from certain commands to
    -- booleans for convenience.
    purist=false
}

turboredis.COMMANDS = {
    "APPEND",
    -- AUTH (not supported yet)
    "BGREWRITEAOF",
    "BGSAVE",
    "BITCOUNT",
    "BITOP AND",
    "BITOP OR",
    "BITOP XOR",
    "BITOP NOT",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "CLIENT KILL",
    "CLIENT LIST", -- FIXME: Should parse this..
    "CLIENT GETNAME",
    "CLIENT SETNAME",
    "CONFIG GET",
    -- "CONFIG REWRITE" (not yet supported)
    "CONFIG SET",
    "CONFIG RESETSTAT",
    "DBSIZE",
    -- "DEBUG OBJECT" (not yet supported)
    -- "DEBUG SEGFAULT" (not yet supported)
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
    -- MONITOR (not yet supported)
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
    -- PSUBSCRIBE (in turboredis.PUBSUB_COMMANDS)

    -- PUBSUB (divided into the subcommands below)
    "PUBSUB CHANNELS",
    "PUBSUB NUMSUB",
    "PUBSUB NUMPAT",

    "PTTL",
    "PUBLISH",
    -- PUNSUBSCRIBE (in turboredis.PUBSUB_COMMANDS)
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
    -- SELECT (custom handling)
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
    -- SUBSCRIBE (in turboredis.PUBSUB_COMMANDS)
    "SUNION",
    "SUNIONSTORE",
    "SYNC",
    "TIME",
    "TTL",
    "TYPE",
    -- UNSUBSCRIBE (in turboredis.PUBSUB_COMMANDS)
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

-------------------------------------------------------------------------------
---------------------------------------------------- Redis Protocol Helpers ---
-------------------------------------------------------------------------------

function turboredis.read_bulk_reply(iostream, len, callback, callback_arg)
    iostream:read_bytes(len, function (data)
        iostream:read_bytes(2, function ()
            if callback_arg then
                callback(callback_arg, data)
            else
                callback(data)
            end
        end)
    end)
end

function turboredis.read_multibulk_reply(iostream, num_replies, callback,
                                         callback_arg)
    local out = {}
    local len
    function loop()
        iostream:read_until("\r\n", function (data)
            function check()
                if #out == num_replies then
                    if callback_arg then
                        callback(callback_arg, out)
                    else
                        callback(out)
                    end
                else
                    loop()
                end
            end
            local firstchar = data:sub(1,1)
            if firstchar == "$" or firstchar == "*" then
                if len == -1 then
                    table.insert(out, nil)
                    check()
                else
                    len = tonumber(data:strip():sub(2))
                    if firstchar == "$" then
                        turboredis.read_bulk_reply(iostream, len,
                            function (data)
                                table.insert(out, data)
                                check()
                            end)
                    else
                        turboredis.read_multibulk_reply(iostream, len,
                            function(data)
                                table.insert(out, data)
                                check()
                            end)
                    end
                end
            elseif firstchar == ":" then
                table.insert(out, tonumber(data:strip():sub(2)))
                check()
            elseif firstchar == "+" then
                table.insert(out, true)
                check()
            elseif firstchar == "-" then
                table.insert(out, false)
                check()
            else
                table.insert(out, "Could not parse reply")
                check()
            end
        end)
    end
    loop()
end

function turboredis.read_reply(iostream, firstchar, callback, callback_arg)
    function done(res)
        if callback_arg then
            callback(callback_arg, res)
        else
            callback(res)
        end
    end
    if firstchar == "+" then -- status
        iostream:read_until("\r\n", function (data)
            data = data:strip()
            done({true, data})
        end)
    elseif firstchar == "-" then -- error
        iostream:read_until("\r\n", function (data)
            data = data:strip()
            done({false, data})
        end)
    elseif firstchar == ":" then -- integer
        iostream:read_until("\r\n", function (data)
            done({tonumber(data:strip())})
        end)
    elseif firstchar == "$" or firstchar == "*" then
        iostream:read_until("\r\n", function (data)
            local len = tonumber(data:strip())
            if len == -1 then
                done({nil})
            elseif len == 0 and firstchar == "*" then -- empty list or set
                done({{}})
            else
                if firstchar == "$" then
                    turboredis.read_bulk_reply(iostream, len, function (reply)
                        done({reply})
                    end)
                else
                    turboredis.read_multibulk_reply(iostream, len, function (reply)
                        done({reply})
                    end)
                end
            end
        end)
    else
        done({nil, "Could not parse reply from redis."})
    end
end


-------------------------------------------------------------------------------
------------------------------------------------------------------- Command ---
-------------------------------------------------------------------------------

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
        elseif self.cmd[1] == "INCRBYFLOAT" or 
               self.cmd[1] == "PTTL" then
            out = {tonumber(res[1])}
        elseif self.cmd[1] == "PUBSUB" then
            if self.cmd[2] == "NUMSUB" then
                out = {}
                for i in range(1, #res[1], 2) do
                    out[res[1][i]] = tonumber(res[1][i+1])
                end
                return {out}
            end
        elseif self.cmd[1] == "HGETALL" then
            out = {turboredis.from_kvlist(res[1])}
        else
            for _, c in ipairs({"EXISTS", "EXPIRE",
                                "EXPIREAT", "HEXISTS",
                                "HSETNX", "MSETNX",
                                "MOVE", "RENAMENX"}) do
                if self.cmd[1] == c then
                    out = {res[1] == 1}
                    return out
                end
            end
        end
    end
    return out
end

function turboredis.Command:_handle_reply(firstchar)
    turboredis.read_reply(self.iostream, firstchar, function (self, res)
        local a1, a2
        res = self:_format_res(res)
        if self.callback_arg then
            self.callback(self.callback_arg, unpack(res))
        else
            self.callback(unpack(res))
        end
    end, self)
end

function turboredis.Command:execute(callback, callback_arg)
    self.callback = callback
    self.callback_arg = callback_arg
    self.iostream:write(self.cmdstr, function()
        self.iostream:read_bytes(1, self._handle_reply, self)
    end)
end

function turboredis.Command:execute_noreply(callback, callback_arg)
    self.iostream:write(self.cmdstr, function()
        if callback_arg then
            callback(callback_arg, true)
        else
            callback(true)
        end
    end)
end


-------------------------------------------------------------------------------
----------------------------------------------------------- Base Connection ---
-------------------------------------------------------------------------------

turboredis.BaseConnection = class("BaseConnection")
function turboredis.BaseConnection:initialize(host, port, kwargs)
    kwargs = kwargs or {}
    self.host = host or "127.0.0.1"
    self.port = port or 6379
    self.family = 2
    self.ioloop = kwargs.io_loop or turbo.ioloop.instance()
    self.connect_timeout = kwargs.connect_timeout or 5
    self.authenticated = false
    self.selected = false
    self.pwd = nil
    self.dbid = nil
end

function turboredis.BaseConnection:_connect_done(args)
    self.connect_timeout_ref = nil
    self.connect_coctx:set_state(turbo.coctx.states.DEAD)
    self.connect_coctx:set_arguments(args)
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

function turboredis.BaseConnection:connect(timeout, callback, callback_arg)
    local timeout
    local connect_timeout_ref
    local ctx

    if not callback then
        ctx = turbo.coctx.CoroutineContext:new(self.ioloop)
        ctx:set_state(turbo.coctx.states.WORKING)
    end

    local connect_done = function(a1, a2)
        if callback then
            if callback_arg then
                callback(callback_arg, a1, a2)
            else
                callback(a1, a2)
            end
        else
            ctx:set_state(turbo.coctx.states.DEAD)
            ctx:set_arguments({a1, a2})
            ctx:finalize_context()
        end
    end

    function handle_connect()
        self.ioloop:remove_timeout(self.connect_timeout_ref)
        connect_done(true)
    end

    function handle_connect_timeout()
        connect_done(false, {err=-1, msg="Connect timeout"})
    end

    function handle_connect_error(err, strerror)
        self.ioloop:remove_timeout(self.connect_timeout_ref)
        connect_done(false, {err=err, msg=strerror})
    end

    self.ioloop = turbo.ioloop.instance()
    timeout = (timeout or self.connect_timeout) * 1000 +
        turbo.util.gettimeofday()
    connect_timeout_ref = self.ioloop:add_timeout(timeout,
                                                   handle_connect_timeout)
    self.sock, msg = turbo.socket.new_nonblock_socket(self.family,
                                                      turbo.socket.SOCK_STREAM,
                                                      0)
    self.iostream = turbo.iostream.IOStream:new(self.sock, self.ioloop)
    local rc, msg = self.iostream:connect(self.host,
                                          self.port,
                                          self.family,
                                          handle_connect,
                                          handle_connect_error,
                                          self)
    if rc ~= 0 then
        error("Connect failed")
        handle_connect_error(-1, "Connect failed")
        return -1 --wtf
    end

    if not callback then
        ctx:set_state(turbo.coctx.states.WAIT_COND)
        return ctx
    end
end

function turboredis.BaseConnection:run(cmd, callback, callback_arg)
    return turboredis.Command:new(cmd, self.iostream):execute(callback,
                                                               callback_arg)
end

function turboredis.BaseConnection:run_noreply(cmd, callback, callback_arg)
    return turboredis.Command:new(cmd, self.iostream):execute_noreply(callback,
        callback_arg)
end

function turboredis.BaseConnection:run_mod(cmd, mod, callback, callback_arg)
    turboredis.Command:new(cmd, self.iostream):execute(function (...)
        local args = mod(unpack({...}))
        if callback_arg then
            table.insert(args, 1, callback_arg)
            callback(unpack(args))
        else
            callback(unpack(args))
        end
    end)
end

function turboredis.BaseConnection:run_mod_dual(cmd, mod, callback, callback_arg)
    if callback then
        return self:run_mod(cmd, mod, callback, callback_arg)
    else
        return turbo.async.task(self.run_mod, self, cmd, mod)
    end
end

function turboredis.BaseConnection:run_dual(cmd, callback, callback_arg)
    if callback then
        return self:run(cmd, callback, callback_arg)
    else
        return turbo.async.task(self.run, self, cmd)
    end
end


-------------------------------------------------------------------------------
---------------------------------------------------------------- Connection ---
-------------------------------------------------------------------------------

turboredis.Connection = class("Connection", turboredis.BaseConnection)

for _, v in ipairs(turboredis.COMMANDS) do
    turboredis.Connection[v:lower():gsub(" ", "_")] = function (self, ...)
        local cmd = turboredis.flatten({v:split(" "), ...})
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
            return self:run(cmd, callback, callback_arg)
        else
            return turbo.async.task(self.run, self, cmd)
        end
    end
end

function turboredis.Connection:select(dbid, callback, callback_arg)
    self.selected = true
    self.dbid = dbid
    return self:run_dual({"SELECT", dbid}, callback, callback_arg)
end

function turboredis.Connection:config_get(key, callback, callback_arg)
    return self:run_mod_dual({"CONFIG", "GET", key}, function(v)
        return {v[key]}
    end)
end


-------------------------------------------------------------------------------
-------------------------------------------------------------------- PUBSUB ---
-------------------------------------------------------------------------------

turboredis.PUBSUB_COMMANDS = {
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "UNSUBSCRIBE"
}

turboredis.PubSubConnection = class("PubSubConnection", turboredis.BaseConnection)

function turboredis.PubSubConnection:read_msg(callback, callback_arg)
    self.iostream:read_until("\r\n", function (data)
        local data = data:strip()
        local prefix = data:sub(1, 1)
        local len = tonumber(data:strip():sub(2))
        assert(prefix == '*')
        turboredis.read_multibulk_reply(self.iostream, len, function (data)
            callback(callback_arg, data)
        end)
    end)
end

function turboredis.PubSubConnection:start(callback, callback_arg)
    self.callback = callback
    self.callback_arg = callback_arg
    turbo.ioloop.instance():add_callback(function ()
        while true do
            local msg = yield(turbo.async.task(self.read_msg, self))
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
                self.callback(callback_arg, res)
            else
                self.callback(res)
            end
        end
    end)
end

for _, v in ipairs(turboredis.PUBSUB_COMMANDS) do
    turboredis.PubSubConnection[v:lower():gsub(" ", "_")] = function (self, ...)
        local cmd = turboredis.flatten({v:split(" "), ...})
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
            return turbo.async.task(self.run_noreply, self, cmd)
        end
    end
end

return turboredis
