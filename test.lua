--
-- A set of very basic tests for turboredis
--
--


function trace (event, line)
  local s = debug.getinfo(2).short_src
  print(s .. ":" .. line)
end

local turbo = require("turbo")
require("luaunit")
local turboredis = require("turboredis")
local yield = coroutine.yield
local ffi = require("ffi")
local os = require("os")
ffi.cdef([[
unsigned int sleep(unsigned int seconds);
]])

local usage = [[
test.lua [-f/--fast] [--redis-host REDIS_HOST] [--redis-port REDIS_PORT] [-h/--help]
         [-U/--include-unstable-tests] [-I/--include-unsupported]
]]

local options = {
    host="127.0.0.1",
    port=6379,
    fast=false,
    include_unsupported=false
}

local i = 1
while i <= #arg do
    if arg[i] == "-f" or arg[i] == "--fast" then
        options.fast = true
    elseif arg[i] == "--include-unsupported" or arg[i] == "-I" then
        options.include_unsupported = true
    elseif arg[i] == "--include-unstable-tests" or arg[i] == "-U" then
        options.unstable = true
    elseif arg[i] == "--redis-host" then
        options.host = arg[i+1]
        if options.host == nil then
            print("Invalid option supplied to --redis-port")
            os.exit(1)
        end
        i = i + 1
    elseif arg[i] == "--redis-port" then
        options.port = tonumber(arg[i+1])
        if options.port == nil then
            print("Invalid option supplied to --redis-port")
            os.exit(1)
        end
        i = i + 1
    elseif arg[i] == "-h" or arg[i] == "--help" then
        print(usage)
        os.exit()
    end
    i = i+1
end
arg = {}

if not options.include_unsupported then
    print("IMPORTANT: Ignoring tests for unsupported / potentially unsupported commands")
end


TestTurboRedis = {}

function TestTurboRedis:setUp()
    local r
    self.con = turboredis.Connection:new(options.host, options.port)
    r = yield(self.con:connect())
    assert(r)
    r = yield(self.con:flushall())
    assert(r)
end

function TestTurboRedis:tearDown()
end

function TestTurboRedis:t_flushdb()
    local r
    r = yield(self.con:flushdb())
    assert(r)
    r = yield(self.con:set("test1", "123"))
    assert(r)
    r = yield(self.con:set("test2", "123"))
    assert(r)
    r = yield(self.con:set("test3", "123"))
    assert(r)
    r = yield(self.con:set("test4", "123"))
    assert(r)
    r = yield(self.con:dbsize())
    assertEquals(r, 4)
    r = yield(self.con:flushdb())
    assert(r)
    r = yield(self.con:dbsize())
    assertEquals(r, 0)
end

-- GET: see test_basic_set_get

function TestTurboRedis:test_getbit()
    local r
    r = yield(self.con:getbit("test", 1))
    assertEquals(r, 0)
    r = yield(self.con:getbit("test", 2))
    assertEquals(r, 1)
end

function TestTurboRedis:test_getrange()
    local r
    r = yield(self.con:getrange("test", 1, 2))
    assertEquals(r, "23")
end

function TestTurboRedis:test_getset()
    local r
    r = yield(self.con:getset("test", "hello"))
    assertEquals(r, "123")
end

function TestTurboRedis:test_hash()
    local r
    r = yield(self.con:hset("myhash", "field1", "foo"))
    assert(r)
    r = yield(self.con:hget("myhash", "field1"))
    assertEquals(r, "foo")
    r = yield(self.con:hexists("myhash", "field1"))
    assert(r)
    r = yield(self.con:hexists("myhash", "doesnotexist"))
    assert(not r)
    r = yield(self.con:hgetall("myhash"))
    assert(r["field1"] == "foo")
    r = yield(self.con:hkeys("myhash"))
    assert(r[1] == "field1")
    r = yield(self.con:hvals("myhash"))
    assert(r[1] == "foo")
    r = yield(self.con:hlen("myhash"))
    assertEquals(r, 1)
    r = yield(self.con:hset("myhash", "counter", 0))
    assert(r)
    r = yield(self.con:hincrby("myhash", "counter", 1))
    assertEquals(r, 1)
    r = yield(self.con:hget("myhash", "counter"))
    assertEquals(r, "1")
    r = yield(self.con:hincrbyfloat("myhash", "counter", 0.1))
    assertEquals(r, "1.1")
    r = yield(self.con:hmset("myhash", "f1", "v1", "f2", "v2"))
    assert(r)
    r = yield(self.con:hmget("myhash", "f1", "f2"))
    assert(r[1] == "v1")
    assert(r[2] == "v2")
    r = yield(self.con:hsetnx("myhash", "f1", "123131231"))
    assertEquals(r, false)
    r = yield(self.con:hsetnx("myhash", "newfield", "lala"))
    assertEquals(r, true)
    r = yield(self.con:hdel("myhash", "newfield"))
    assert(r)
    r = yield(self.con:hexists("myhash", "newfield"))
    assert(not r)
end


function TestTurboRedis:test_incr()
    local r
    r = yield(self.con:incr("test"))
    assertEquals(r, 124)
    r = yield(self.con:get("test"))
    assertEquals(r, "124")
end


function TestTurboRedis:test_incrbyfloat()
    local r
    r = yield(self.con:incrbyfloat("test", 0.1))
    assertEquals(r, "123.1")
    r = yield(self.con:get("test"))
    assertEquals(r, "123.1")
end


function TestTurboRedis:test_keys()
    local r
    r = yield(self.con:keys("*"))
    assert(#r == 1)
    assert(r[1] == "test")
end


function TestTurboRedis:test_list()
    local r
    r = yield(self.con:rpush("mylist", "Hello!"))
    assert(r)
    r = yield(self.con:lset("mylist", 0, "Hello!"))
    assert(r)
    r = yield(self.con:lindex("mylist", 0))
    assertEquals(r, "Hello!")
    r = yield(self.con:llen("mylist"))
    assertEquals(r, 1)
    r = yield(self.con:lpop("mylist"))
    assertEquals(r, "Hello!")
    r = yield(self.con:llen("mylist"))
    assertEquals(r, 0)
    r = yield(self.con:lset("mylist", 0, "Hello!"))
    assert(not r)
    r = yield(self.con:rpush("mylist", "Hello", "World"))
    assertEquals(r, 2)
    r = yield(self.con:rpushx("mylist", "!"))
    assertEquals(r, 3)
    r = yield(self.con:rpushx("invalid", "abcdefgh"))
    assertEquals(r, 0)
    r = yield(self.con:lpushx("invalid", "abcdefgh"))
    assertEquals(r, 0)
    r = yield(self.con:lpop("mylist"))
    assertEquals(r, "Hello")
    r = yield(self.con:rpop("mylist"))
    assertEquals(r, "!")
    r = yield(self.con:lpush("mylist", "GoodBye"))
    assertEquals(r, 2)
    r = yield(self.con:lrem("mylist", -1, "World"))
    assertEquals(r, 1)
    for _, v in ipairs({1,2,3,4}) do -- 4,3,2,1,GoodBye
        r = yield(self.con:lpush("mylist", v))
    end
    assertEquals(r, 5)
    r = yield(self.con:lrange("mylist", 0, 1))
    assert(r[1] == "4")
    assert(r[2] == "3")
end

function TestTurboRedis:test_mget()
    local r
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:set("bar", "foo"))
    assert(r)
    r = yield(self.con:mget("foo", "bar"))
    assert(r[1] == "bar")
    assert(r[2] == "foo")
end

function TestTurboRedis:test_move()
    local r
    r = yield(self.con:set("to_be_moved", "123"))
    assert(r)
    r = yield(self.con:move("to_be_moved", "1"))
    assert(r)
    r = yield(self.con:get("to_be_moved"))
    assert(not r)
    r = yield(self.con:select(1))
    assert(r)
    r = yield(self.con:get("to_be_moved"))
    assert(r)
end

function TestTurboRedis:test_mset()
    local r
    r = yield(self.con:mset("key1", "val1", "key2", "val2"))
    assert(r)
    r = yield(self.con:get("key1"))
    assertEquals(r, "val1")
    r = yield(self.con:get("key2"))
    assertEquals(r, "val2")
end

function TestTurboRedis:test_msetnx()
    local r
    r = yield(self.con:msetnx("key1", "val1", "key2", "val2"))
    assert(r)
    r = yield(self.con:msetnx("key2", "val2", "key3", "val3"))
    assert(not r)
end

function TestTurboRedis:test_multi_exec_and_discard()
    local r
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("key1", "val1"))
    assert(r)
    r = yield(self.con:set("key2", "val2"))
    assert(r)
    r = yield(self.con:exec())
    assert(r)
    r = yield(self.con:get("key1"))
    assertEquals(r, "val1")
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("key3", "val3"))
    assert(r)
    r = yield(self.con:get("key3"))
    assert(r)
    r = yield(self.con:discard())
    assert(r)
    r = yield(self.con:get("key3"))
    assertEquals(r, nil)
end

-- TODO: OBJECT

function TestTurboRedis:test_persist()

end

function TestTurboRedis:test_select()
    local r
    r = yield(self.con:select(1))
    assert(r)
    r = yield(self.con:get("test"))
    assert(not r)
end



--- Test by command


function TestTurboRedis:test_append()
    local r
    r = yield(self.con:set("test", "123"))
    assert(r)
    r = yield(self.con:get("test"))
    assertEquals(r, "123")
    r = yield(self.con:append("test", "456"))
    assert(r)
    r = yield(self.con:get("test"))
    assertEquals(r, "123456")
end

-- REDIS_PASSWORD = nil
-- if REDIS_PASSWORD then
--    function TestTurboRedis:test_auth()
--        local r
--        r = yield(self.con.auth(REDIS_PASSWORD))
--        assert(r)
--    end
-- end
--

function TestTurboRedis:test_bgrewriteaof()
    -- TOOD: Write a test that works for this
end

if not options.fast and options.unstable then
    -- This occasionally fails due to a currently
    -- running background save operation
    function TestTurboRedis:test_bgsave()
        local r
        local time
        time = yield(self.con:lastsave())
        assert(time)
        r = yield(self.con:bgsave())
        assert(r)
        r = yield(self.con:lastsave())
        assert(time ~= r)
    end
end

function TestTurboRedis:test_bitcount()
    local r
    r = yield(self.con:set("test", "foobar"))
    assert(r)
    r = yield(self.con:bitcount("test", 0, 0))
    assertEquals(r, 4)
    r = yield(self.con:bitcount("test", 1, 1))
    assertEquals(r, 6)
    r = yield(self.con:bitcount("test", 1))
    assertEquals(r, false) -- Error reply
end

function TestTurboRedis:test_bitop()
    local r
    r = yield(self.con:set("t1", "aaabbb"))
    assert(r)
    r = yield(self.con:set("t2", "bbbccc"))
    assert(r)

    r = yield(self.con:bitop_and("dst", "t1", "t2"))
    assertEquals(r, 6)
    r = yield(self.con:get("dst"))
    assertEquals(r, "```bbb")

    r = yield(self.con:bitop_or("dst", "t1", "t2"))
    assertEquals(r, 6)
    r = yield(self.con:get("dst"))
    assertEquals(r, "cccccc")

    r = yield(self.con:bitop_xor("dst", "t1", "t2"))
    assertEquals(r, 6)
    r = yield(self.con:get("dst"))
    assertEquals(r, "\x03\x03\x03\x01\x01\x01")

    r = yield(self.con:bitop_xor("dst", "t1", "t2"))
    assertEquals(r, 6)
    r = yield(self.con:get("dst"))
    assertEquals(r, "\x03\x03\x03\x01\x01\x01")

    r = yield(self.con:bitop_not("dst", "t1"))
    assertEquals(r, 6)
    r = yield(self.con:get("dst"))
    assertEquals(r, "\x9e\x9e\x9e\x9d\x9d\x9d")
end

function TestTurboRedis:test_blpop()
    local r
    r = yield(self.con:rpush("foo", "bar"))
    assert(r)
    r = yield(self.con:blpop("foo", 1))
    assertEquals(r[1], "foo")
    assertEquals(r[2], "bar")
end

function TestTurboRedis:test_brpop()
    local r
    r = yield(self.con:rpush("foo", "bar"))
    r = yield(self.con:rpush("foo", "barbar"))
    assert(r)
    r = yield(self.con:brpop("foo", 1))
    assertEquals(r[1], "foo")
    assertEquals(r[2], "barbar")
end

function TestTurboRedis:test_brpoplpush()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assert(r)
    r = yield(self.con:rpush("foolist", "bar"))
    assert(r)
    r = yield(self.con:rpush("foolist", "foobar"))
    assert(r)
    r = yield(self.con:brpoplpush("foolist", "barlist", 1))
    assertEquals(r, "foobar")
    r = yield(self.con:lrange("barlist", 0, -1))
    assertEquals(r, {"foobar"})
end

function TestTurboRedis:test_client_kill()
    -- TODO: 1. Make another connection
    --       2. Kill it
    --       3. Verify that it is no longer working
end

function TestTurboRedis:test_client_list()
    local r
    r = yield(self.con:client_list())
    assert(r) -- TODO: Test properly
end

function TestTurboRedis:test_client_getname()
    local r
    r = yield(self.con:client_setname("foo"))
    assert(r)
    r = yield(self.con:client_getname())
    assertEquals(r, "foo")
end

function TestTurboRedis:test_client_setname()
    local r
    r = yield(self.con:client_setname("bar"))
    assert(r)
    r = yield(self.con:client_getname())
    assertEquals(r, "bar")
end

function TestTurboRedis:test_config_get()
    local r
    r = yield(self.con:config_get("port"))
    assertEquals(r, tostring(self.con.port))
end

function TestTurboRedis:test_config_set()
    local r
    local old_appendonly = yield(self.con:config_get("appendonly"))
    local new_appendonly = old_appendonly and "yes" or "no"
    r = yield(self.con:config_set("appendonly", new_appendonly))
    assert(r)
    r = yield(self.con:config_get("appendonly"))
    assertEquals(r, new_appendonly)
    r = yield(self.con:config_set("appendonly", old_appendonly))
    assert(r)
end

function TestTurboRedis:test_config_resetstats()
    -- TODO: How to test this?? Some string matching on the result
    -- from the INFO command maybe?
end

function TestTurboRedis:test_dbsize()
    local r
    r = yield(self.con:dbsize())
    assertEquals(r, 0)
    r = yield(self.con:set("abcdefg", "hijklmnop"))
    assert(r)
    r = yield(self.con:dbsize())
    assertEquals(r, 1)
end

if options.include_unsupported then
    function TestTurboRedis:test_debug_object()
        assert(false, "'DEBUG OBJECT' has no test yet.")
    end
end

if options.include_unsupported then
    function TestTurboRedis:test_debug_segfault()
        assert(false, "'DEBUG SEGAFAULT' has no test yet.")
    end
end

function TestTurboRedis:test_decr()
    local r
    r = yield(self.con:set("foo", 1))
    assert(r)
    r = yield(self.con:decr("foo"))
    assertEquals(r, 0)
    r = yield(self.con:get("foo"))
    assertEquals(r, "0")
end

function TestTurboRedis:test_decrby()
    local r
    r = yield(self.con:set("foo", 10))
    assert(r)
    r = yield(self.con:decrby("foo", 5))
    assertEquals(r, 5)
    r = yield(self.con:get("foo"))
    assertEquals(r, "5")
end

function TestTurboRedis:test_del()
    local r
    r = yield(self.con:set("foo", 1))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "1")
    r = yield(self.con:del("foo"))
    assert(r)
    r = yield(self.con:get("foo"))
    assert(not r)
end


function TestTurboRedis:test_discard()
    local r
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:discard())
    assert(r)
    r = yield(self.con:get("foo"))
    assert(not r)
end

function TestTurboRedis:test_dump()
    local r
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:dump("foo"))
    assertEquals(r, "\x00\x03bar\x06\x00pS!\xe0\x1b3\xc1\x84")
end

function TestTurboRedis:test_echo()
    local r
    r = yield(self.con:echo("Foo"))
    assertEquals(r, "Foo")
    r = yield(self.con:echo("Bar"))
    assertEquals(r, "Bar")
end

function TestTurboRedis:test_eval()
    local r
    r = yield(self.con:eval("return redis.call('set','foo','bar')", 0))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_evalsha()
    local r
    local ssha
    ssha = yield(self.con:script_load("return redis.call('set','foo','bar')"))
    assert(ssha == "2fa2b029f72572e803ff55a09b1282699aecae6a")
    -- TODO: Should argument count be implied?
    r = yield(self.con:evalsha(ssha, 0))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_exec()
    local r
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:set("bar", "foo"))
    assert(r)
    r = yield(self.con:set("fortytwo", 42))
    assert(r)
    r = yield(self.con:decr("fortytwo"))
    assert(r)
    r = yield(self.con:exec())
    assert(r[1] == true)
    assert(r[2] == true)
    assert(r[3] == true)
    assert(r[4] == 41)
end

function TestTurboRedis:test_exists()
    local r
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:set("bar", "foo"))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "bar")
    r = yield(self.con:exists("foo"))
    assert(r)
    r = yield(self.con:exists("abc"))
    assert(not r)
end

if not options.fast then
    function TestTurboRedis:test_expire()
        local r
        r = yield(self.con:set("foo", "bar"))
        assert(r)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
        r = yield(self.con:expire("foo", 3))
        ffi.C.sleep(5)
        r = yield(self.con:get("foo"))
        assert(not r)
    end
end

if not options.fast then
    function TestTurboRedis:test_expireat()
        local r
        local ts
        r = yield(self.con:set("foo", "bar"))
        assert(r)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
        r = yield(self.con:expireat("foo", os.time()+5))
        ffi.C.sleep(2)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
        ffi.C.sleep(3)
        r = yield(self.con:get("foo"))
        assert(not r)
    end
end

function TestTurboRedis:test_flushall()
    local r
    r = yield(self.con:select(0))
    assert(r)
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:select(1))
    assert(r)
    r = yield(self.con:set("bar", "foo"))
    assert(r)
    r = yield(self.con:flushall())
    assert(r)
    r = yield(self.con:select(0))
    assert(r)
    r = yield(self.con:get("foo"))
    assert(not r)
    r = yield(self.con:select(1))
    assert(r)
    r = yield(self.con:get("bar"))
    assert(not r)
end

function TestTurboRedis:test_flushdb()
    local r
    r = yield(self.con:flushdb())
    assert(r)
    r = yield(self.con:set("test1", "123"))
    assert(r)
    r = yield(self.con:set("test2", "123"))
    assert(r)
    r = yield(self.con:dbsize())
    assertEquals(r, 2)
    r = yield(self.con:flushdb())
    assert(r)
    r = yield(self.con:dbsize())
    assertEquals(r, 0)
end

function TestTurboRedis:test_get()
    -- FIXME: Arrogance
end

function TestTurboRedis:test_getbit()
    local r
    r = yield(self.con:setbit("foo", 7, 1))
    assert(r)
    r = yield(self.con:getbit("foo", 7))
    assertEquals(r, 1)
    r = yield(self.con:setbit("foo", 7, 0))
    assert(r)
    r = yield(self.con:getbit("foo", 7))
    assertEquals(r, 0)
end

function TestTurboRedis:test_getrange()
    local r
    r = yield(self.con:set("foo", "foobar"))
    assert(r)
    r = yield(self.con:getrange("foo", 0, 2))
    assertEquals(r, "foo")
    r = yield(self.con:getrange("foo", 3, 5))
    assertEquals(r, "bar")
    r = yield(self.con:getrange("foo", 3, 1000))
    assertEquals(r, "bar")
    r = yield(self.con:getrange("foo", -3, -1))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_getset()
    local r
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:getset("foo", "foo"))
    assertEquals(r, "bar")
    r = yield(self.con:get("foo"))
    assertEquals(r, "foo")
end

function TestTurboRedis:test_hdel()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hset("foohash", "bar", "foo"))
    assert(r)
    r = yield(self.con:hdel("foohash", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:hdel("foohash", "bar"))
    assertEquals(r, 1)
end

function TestTurboRedis:test_hexists()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hexists("foohash", "foo"))
    assert(r)
    r = yield(self.con:hdel("foohash", "foo"))
    assert(r)
    r = yield(self.con:hexists("foohash", "foo"))
    assert(not r)
end

function TestTurboRedis:test_hget()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_hgetall()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hset("foohash", "bar", "foo"))
    assert(r)
    r = yield(self.con:hgetall("foohash"))
    for i, v in ipairs(r) do
        if v == "foo" then
            assert(r[i+1] == "bar")
        elseif v == "bar" then
            assert(r[i+1] == "foo")
        end
    end
end

function TestTurboRedis:test_hincrby()
    local r
    r = yield(self.con:hset("foohash", "foo", 10))
    assert(r)
    r = yield(self.con:hincrby("foohash", "foo", 2))
    assertEquals(r, 12)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "12")
end

function TestTurboRedis:test_hincrybyfloat()
    local r
    r = yield(self.con:hset("foohash", "foo", "13.4"))
    assert(r)
    r = yield(self.con:hincrbyfloat("foohash", "foo", 0.3))
    assertEquals(r, "13.7")
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "13.7")
end

function TestTurboRedis:test_hkeys()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hset("foohash", "bar", "foo"))
    assert(r)
    r = yield(self.con:hkeys("foohash"))
    assertEquals({"foo", "bar"}, r)
end

function TestTurboRedis:test_hlen()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hlen("foohash"))
    assertEquals(r, 1)
    r = yield(self.con:hset("foohash", "bar", "foo"))
    assert(r)
    r = yield(self.con:hlen("foohash"))
    assertEquals(r, 2)
end

function TestTurboRedis:test_hmget()
    local r
    r = yield(self.con:hset("foohash", "foo", "123"))
    assert(r)
    r = yield(self.con:hset("foohash", "bar", "456"))
    assert(r)
    r = yield(self.con:hmget("foohash", "foo", "bar"))
    assertEquals(r, {"123", "456"})
    r = yield(self.con:hmget("foohash", "bar", "foo"))
    assertEquals(r, {"456", "123"})
end

function TestTurboRedis:test_hmset()
    local r
    r = yield(self.con:hmset("foohash", "foo", "abc", "bar", "123"))
    assert(r)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "abc")
    r = yield(self.con:hget("foohash", "bar"))
    assertEquals(r, "123")
end

function TestTurboRedis:test_hset()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_hsetnx()
    local r
    r = yield(self.con:hset("foohash", "foo", "bar"))
    assert(r)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "bar")
    r = yield(self.con:hsetnx("foohash", "foo", "test"))
    assert(not r)
    r = yield(self.con:hget("foohash", "foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_hvals()
    local r
    r = yield(self.con:hset("foohash", "foo", "abc"))
    assert(r)
    r = yield(self.con:hset("foohash", "bar", "123"))
    assert(r)
    r = yield(self.con:hvals("foohash"))
    assertEquals(r, {"abc", "123"})
end

function TestTurboRedis:test_incr()
    local r
    r = yield(self.con:set("foo", "41"))
    assert(r)
    r = yield(self.con:incr("foo"))
    assertEquals(r, 42)
    r = yield(self.con:get("foo"))
    assertEquals(r, "42")
end

function TestTurboRedis:test_incrby()
    local r
    r = yield(self.con:set("foo", 40))
    assert(r)
    r = yield(self.con:incrby("foo", 2))
    assertEquals(r,  42)
    r = yield(self.con:get("foo"))
    assertEquals(r,  "42")
end

function TestTurboRedis:test_incrbyfloat()
    local r
    r = yield(self.con:set("foo", 40.3))
    assert(r)
    r = yield(self.con:incrbyfloat("foo", 1.7))
    assertEquals(r, 42)
    r = yield(self.con:get("foo"))
    assertEquals(r, "42")
end

function TestTurboRedis:test_info()
    local r
    r = yield(self.con:info())
    assert(type(r) == "string")
end

function TestTurboRedis:test_keys()
    local r
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:set("bar", "foo"))
    assert(r)
    r = yield(self.con:set("hello", "world"))
    assert(r)
    r = yield(self.con:keys("*o"))
    assertItemsEquals(r, {"hello", "foo"})
    assertItemsEquals(r, {"hello", "foo"})
end

if not options.fast and options.unstable then
    -- This occasionally fails due to a currently
    -- running background save operation
    function TestTurboRedis:test_lastsave()
        local r
        local time
        time = yield(self.con:lastsave())
        assert(time)
        r = yield(self.con:bgsave())
        assert(r)
        ffi.C.sleep(10) -- We assume that this is enough
        r = yield(self.con:lastsave())
        assert(time ~= r)
    end
end

function TestTurboRedis:test_lindex()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r,  1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r,  2)
    r = yield(self.con:lindex("foolist", 0))
    assertEquals(r, "foo")
    r = yield(self.con:lindex("foolist", 1))
    assertEquals(r, "bar")
    r = yield(self.con:lindex("foolist", -1))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_linsert()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r,  1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r,  2)
    r = yield(self.con:linsert("foolist", "BEFORE", "bar", "Hello"))
    assertEquals(r,  3)
    r = yield(self.con:linsert("foolist", "AFTER", "Hello", "World"))
    assertEquals(r,  4)
    r = yield(self.con:lrange("foolist", 0, -1))
    assertEquals(r, {"foo", "Hello", "World", "bar"})
end

function TestTurboRedis:test_llen()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:llen("foolist"))
    assertEquals(r, 2)
end

function TestTurboRedis:test_lpop()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:lpop("foolist"))
    assertEquals(r, "foo")
    r = yield(self.con:lpop("foolist"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_lpush()
    local r
    r = yield(self.con:lpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:lpush("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:lrange("foolist", 0, -1))
    assertEquals(r, {"bar", "foo"})
end

function TestTurboRedis:test_lpushx()
    local r
    r = yield(self.con:lpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:lpushx("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:lpushx("barlist", "foo"))
    assertEquals(r, 0)
end

function TestTurboRedis:test_lrange()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:rpush("foolist", "hello"))
    assertEquals(r, 3)
    r = yield(self.con:rpush("foolist", "world"))
    assertEquals(r, 4)
    r = yield(self.con:lrange("foolist", 0, 0))
    assertEquals(r, {"foo"})
    r = yield(self.con:lrange("foolist", 0, 1))
    assertEquals(r, {"foo", "bar"})
    r = yield(self.con:lrange("foolist", 1, 2))
    assertEquals(r, {"bar", "hello"})
    r = yield(self.con:lrange("foolist", 0, -1))
    assertEquals(r, {"foo", "bar", "hello", "world"})
    r = yield(self.con:lrange("foolist", 0, -2))
    assertEquals(r, {"foo", "bar", "hello"})
end

function TestTurboRedis:test_lrem()
    local r
    r = yield(self.con:rpush("foolist", "foo"))
    assertEquals(r, 1)
    r = yield(self.con:rpush("foolist", "bar"))
    assertEquals(r, 2)
    r = yield(self.con:llen("foolist"))
    assertEquals(r, 2)
    r = yield(self.con:lrem("foolist", 0, "foo"))
    assertEquals(r, 1)
    r = yield(self.con:llen("foolist"))
    assertEquals(r, 1)
    -- FIXME: More tests if needed
end

if options.include_unsupported then
    function TestTurboRedis:test_monitor()
        assert(false, "'MONITOR' not currently supported and has no test.")
    end
end

function TestTurboRedis:test_move()
    local r
    r = yield(self.con:select(0))
    assert(r)
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:move("foo", 1))
    assert(r)
    r = yield(self.con:select(1))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "bar")
end

function TestTurboRedis:test_mset()
    local r
    r = yield(self.con:mset("foo", "bar", "hello", "world"))
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "bar")
    r = yield(self.con:get("hello"))
    assertEquals(r, "world")
end

function TestTurboRedis:test_msetnx()
    local r
    r = yield(self.con:mset("foo", "bar", "hello", "world"))
    assert(r)
    r = yield(self.con:msetnx("foo", "bar", "Hello", "world"))
    assert(not r)

end

function TestTurboRedis:test_multi()
    local r
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("foo", "bar"))
    assert(r)
    r = yield(self.con:set("foo", "Hello World!"))
    assert(r)
    r = yield(self.con:exec())
    assert(r)
    r = yield(self.con:get("foo"))
    assertEquals(r, "Hello World!")
end

if options.include_unsupported then
    function TestTurboRedis:test_object()
        assert(false, "'OBJECT' has no test.")
    end
end

if not options.fast then
    function TestTurboRedis:test_persist()
        local r
        r = yield(self.con:set("foo", "bar"))
        assert(r)
        r = yield(self.con:expire("foo", 5))
        assert(r)
        r = yield(self.con:persist("foo"))
        ffi.C.sleep(6)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
    end
end

if not options.fast then
    function TestTurboRedis:test_pexpire()
        local r
        r = yield(self.con:set("foo", "bar"))
        assert(r)
        r = yield(self.con:pexpire("foo", 2000))
        assert(r)
        ffi.C.sleep(3)
        r = yield(self.con:get("foo"))
        assert(not r)
    end
end

if not options.fast then
    function TestTurboRedis:test_pexpireat()
        local r
        r = yield(self.con:set("foo", "bar"))
        assert(r)
        r = yield(self.con:pexpireat("foo", (os.time()*1000) + 2000))
        assert(r)
        ffi.C.sleep(1)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
        ffi.C.sleep(2)
        r = yield(self.con:get("foo"))
        assertEquals(r, nil)
    end
end

function TestTurboRedis:test_ping()
    local r
    r = yield(self.con:ping())
    assert(r)
end

if not options.fast then
    function TestTurboRedis:test_psetex()
        local r
        r = yield(self.con:psetex("foo", 2000, "bar"))
        assert(r)
        r = yield(self.con:get("foo"))
        assertEquals(r, "bar")
        ffi.C.sleep(3)
        r = yield(self.con:get("foo"))
        assertEquals(r, nil)
    end
end

-------------------------------------------------------------------------------

TestTurboRedisPubSub = {}

function TestTurboRedisPubSub:setUp()
    _G.io_loop_instance = nil
    self.ioloop = turbo.ioloop.instance()
    self.con = turboredis.Connection:new(options.host, options.port)
    self.pcon = turboredis.PubSubConnection:new(options.host, options.port)
end

function TestTurboRedisPubSub:connect()
    local r
    r = yield(self.con:connect())
    assert(r)
    r = yield(self.pcon:connect())
    assert(r)
    r = yield(self.con:flushall())
    assert(r)
end

function TestTurboRedisPubSub:done()
    r = yield(self.pcon:unsubscribe())
    assert(r)
end

function TestTurboRedisPubSub:tearDown()
end

function TestTurboRedisPubSub:test_pubsub_channels()
    local io = self.ioloop
    io:add_callback(function ()
        local r
        self:connect()
        r = yield(self.con:pubsub_channels("*"))
        assertEquals(#r, 0)
        r = yield(self.pcon:subscribe("foo"))
        assert(r)
        self.pcon:start(function ()
            r = yield(self.con:pubsub_channels())
            assertEquals(#r, 1)
            self:done()
            io:close()
        end)
    end)
    io:wait(2)
end

function TestTurboRedisPubSub:test_pubsub_numpat()
    local io = self.ioloop
    io:add_callback(function ()
        local r
        self:connect()
        r = yield(self.con:pubsub_numpat())
        assertEquals(r, 0)
        r = yield(self.pcon:psubscribe("fooz*"))
        assert(r)
        self.pcon:start(function ()
            r = yield(self.con:pubsub_numpat())
            assertEquals(r, 1)
            self:done()
            io:close()
        end)
    end)
    io:wait(2)
end

function TestTurboRedisPubSub:test_pubsub_numsub()
    local io = self.ioloop
    io:add_callback(function ()
        local r
        self:connect()
        r = yield(self.con:pubsub_numsub("foo"))
        assertEquals(r.foo, 0)
        r = yield(self.pcon:subscribe("foo"))
        assert(r)
        self.pcon:start(function ()
            local r
            r = yield(self.con:pubsub_numsub("foo"))
            assertEquals(r.foo, 1)
            self:done()
            io:close()
        end)
    end)
    io:wait(2)
end

function runtests()
    LuaUnit:run("TestTurboRedis")
    turbo.ioloop.instance():close()
    LuaUnit:run("TestTurboRedisPubSub")
end

turbo.ioloop.instance():add_callback(runtests)
turbo.ioloop.instance():start()
