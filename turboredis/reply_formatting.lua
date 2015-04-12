local util = require("turboredis.util")

local rf = {}

function rf.parse_client_list(listtext)
    local lines = listtext:strip():split("\n")
    local entries = {}
    for i, line in ipairs(lines) do
        local entry = {}
        line = line .. " "
        for k,v in line:gmatch("([a-z]+)%=(.-)%s") do
            if k == "fd" or k == "age" or k == "idle" or k == "db" or
               k == "sub" or k == "psub" or k == "multi" or k == "qbuf" or
               k == "qbuf-free" or k == "obl" or k == "oll" or k == "omem" then
               v = tonumber(v)
            end
            entry[k] = v
        end
        entries[#entries+1] = entry
    end
    return entries
end

-- Format reply from Redis for convenience
--
--> NOTE: This is not consistent and needs some work
function rf.format_res(cmd, res)
    local out = res
    if cmd[1] == "CONFIG" then
        if cmd[2] == "GET" then
            out = {res[1][2]}
        end
    elseif cmd[1] == "CLIENT" then
        if cmd[2] == "LIST" then
            out = {rf.parse_client_list(res[1])}
        end
    elseif cmd[1] == "INCRBYFLOAT" or
           cmd[1] == "PTTL" then
        out = {tonumber(res[1])}
    elseif cmd[1] == "PUBSUB" then
        if cmd[2] == "NUMSUB" then
            out = {}
            for i in util.range(1, #res[1], 2) do
                out[res[1][i]] = tonumber(res[1][i+1])
            end
            return {out}
        end
    elseif cmd[1] == "HGETALL" then
        out = {util.from_kvlist(res)}
    else
        for _, c in ipairs({"EXISTS", "EXPIRE",
                            "EXPIREAT", "HEXISTS",
                            "HSETNX", "MSETNX",
                            "MOVE", "RENAMENX", "SETNX",
                            "SISMEMBER", "SMOVE"}) do
            if cmd[1] == c then
                out = {res[1] == 1}
                return out
            end
        end
    end
    return out
end

return rf
