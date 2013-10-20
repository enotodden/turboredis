--
-- A set of very basic tests for turboredis
--

local turbo = require("turbo")
local LuaUnit = require("luaunit")
local turboredis = require("turboredis")
local yield = coroutine.yield
local ffi = require("ffi")
ffi.cdef([[
unsigned int sleep(unsigned int seconds);
]])

-- Use --fast or -f to not run tests that use sleep (expire etc.)
local run_slow_tests = true
if arg[1] == "--fast" or arg[1] == "-f" then
    print("Not running slow tests")
    run_slow_tests = false
end

TestTurboRedis = {}

function TestTurboRedis:setUp()
    local r
    self.con = turboredis.Connection:new()
    r = yield(self.con:connect())
    assert(r)
    r = yield(self.con:set("test", "123"))
    assert(r)
end

function TestTurboRedis:tearDown()
    r = yield(self.con:flushdb())
    assert(r)
end

function TestTurboRedis:test_basic_set_get()
    local r
    r = yield(self.con:get("test1"))
    assertEquals(r, nil)
    r = yield(self.con:set("test1", "123"))
    r = yield(self.con:get("test1"))
    assertEquals(r, "123")
    r = yield(self.con:set("abc")) --no value
    assert(not r)
end

function TestTurboRedis:test_append()
    local r
    r = yield(self.con:get("test"))
    assert(r == "123")
    r = yield(self.con:append("test", "456"))
    assert(r)
    r = yield(self.con:get("test"))
    assert(r == "123456")
end

function TestTurboRedis:test_bitcount()
    local r
    r = yield(self.con:set("test1", "hello"))
    assert(r)
    r = yield(self.con:bitcount("test1"))
    assert(r == 21)
    r = yield(self.con:bitcount("test1", 0, 0))
    assert(r == 3)
    r = yield(self.con:bitcount("test1", 1, 1))
    assert(r == 4)
end

function TestTurboRedis:test_bitop_and()
    local r
    r = yield(self.con:set("test1", "hello"))
    assert(r)
    r = yield(self.con:set("test2", "world"))
    assert(r)
    r = yield(self.con:bitop_and("test3", "test1", "test2"))
    assert(r == 5)
    r = yield(self.con:get("test3"))
    assert(r == "`e`ld")
end

function TestTurboRedis:test_getname_setname()
    local r
    r = yield(self.con:client_getname())
    assert(r == nil)
    r = yield(self.con:client_setname("turboREDIS"))
    assert(r)
    r = yield(self.con:client_getname())
    assert(r == "turboREDIS")
end

function TestTurboRedis:test_config()
    local r, ml
    r = yield(self.con:config_get("*"))
    assert(r)
    assert(r["port"] == tostring(self.con.port))
    r = yield(self.con:config_get("port"))
    assert(r)
    assert(r["port"] == tostring(self.con.port))
    r = yield(self.con:config_get("slowlog-max-len"))
    ml = r["slowlog-max-len"]
    r = yield(self.con:config_set("slowlog-max-len", ml * 2))
    assert(r)
    r = yield(self.con:config_get("slowlog-max-len"))
    assert(r["slowlog-max-len"] == tostring((ml * 2)))
    r = yield(self.con:config_set("slowlog-max-len", ml))
    assert(r)
    r = yield(self.con:config_get("slowlog-max-len"))
    assert(r["slowlog-max-len"] == tostring(ml))

end

function TestTurboRedis:test_dbsize()
    local r = yield(self.con:dbsize())
    assert(r == 1)
end

function TestTurboRedis:test_decr()
    local r
    r = yield(self.con:decr("test"))
    assert(r == 122)
    r = yield(self.con:get("test"))
    assert(r == "122")
    r = yield(self.con:set("invalid", "hello"))
    assert(r)
    r = yield(self.con:decr("invalid"))
    assert(not r)
end

function TestTurboRedis:test_decrby()
    local r
    r = yield(self.con:decrby("test", 2))
    assert(r == 121)
    r = yield(self.con:get("test"))
    assert(r == "121")
    r = yield(self.con:set("invalid", "hello"))
    assert(r)
    r = yield(self.con:decr("invalid"))
    assert(not r)
end

function TestTurboRedis:test_del()
    local r
    r = yield(self.con:get("test"))
    assert(r == "123")
    r = yield(self.con:del("test"))
    assert(r)
    r = yield(self.con:get("test"))
    assert(not r)
end

-- DISCARD: see test_multi_exec_and_discard

function TestTurboRedis:test_dump()
    local r
    r = yield(self.con:dump("invalid"))
    assert(r == nil)
end

function TestTurboRedis:test_echo()
    local r
    r = yield(self.con:echo("Hello!"))
    assert(r == "Hello!")
end

-- TODO: EVAL
-- TODO: EVALSHA
-- EXEC: see test_multi_exec_and_discard

function TestTurboRedis:test_exists()
    local r
    r = yield(self.con:exists("test"))
    assert(r == true)
    r = yield(self.con:del("test"))
    assert(r)
    r = yield(self.con:exists("test"))
    assert(not r)
end

if run_slow_tests then
    function TestTurboRedis:test_expire()
        local r
        r, d = yield(self.con:expire("test", 3))
        assert(r)
        ffi.C.sleep(5)
        r = yield(self.con:exists("test"))
        assert(not r)
    end
end
-- TODO: EXPIREAT

-- TODO: FLUSHALL

function TestTurboRedis:test_flushdb()
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
    assert(r == 4)
    r = yield(self.con:flushdb())
    assert(r)
    r = yield(self.con:dbsize())
    assert(r == 0)
end

-- GET: see test_basic_set_get

function TestTurboRedis:test_getbit()
    local r
    r = yield(self.con:getbit("test", 1))
    assert(r == 0)
    r = yield(self.con:getbit("test", 2))
    assert(r == 1)
end

function TestTurboRedis:test_getrange()
    local r
    r = yield(self.con:getrange("test", 1, 2))
    assert(r == "23")
end

function TestTurboRedis:test_getset()
    local r
    r = yield(self.con:getset("test", "hello"))
    assert(r == "123")
end

function TestTurboRedis:test_hash()
    local r
    r = yield(self.con:hset("myhash", "field1", "foo"))
    assert(r)
    r = yield(self.con:hget("myhash", "field1"))
    assert(r == "foo")
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
    assert(r == 1)
    r = yield(self.con:hset("myhash", "counter", 0))
    assert(r)
    r = yield(self.con:hincrby("myhash", "counter", 1))
    assert(r == 1)
    r = yield(self.con:hget("myhash", "counter"))
    assert(r == "1")
    r = yield(self.con:hincrbyfloat("myhash", "counter", 0.1))
    assert(r == "1.1")
    r = yield(self.con:hmset("myhash", "f1", "v1", "f2", "v2"))
    assert(r)
    r = yield(self.con:hmget("myhash", "f1", "f2"))
    assert(r[1] == "v1")
    assert(r[2] == "v2")
    r = yield(self.con:hsetnx("myhash", "f1", "123131231"))
    assert(r == false)
    r = yield(self.con:hsetnx("myhash", "newfield", "lala"))
    assert(r == true)
    r = yield(self.con:hdel("myhash", "newfield"))
    assert(r)
    r = yield(self.con:hexists("myhash", "newfield"))
    assert(not r)
end


function TestTurboRedis:test_incr()
    local r
    r = yield(self.con:incr("test"))
    assert(r == 124)
    r = yield(self.con:get("test"))
    assert(r == "124")
end


function TestTurboRedis:test_incrbyfloat()
    local r
    r = yield(self.con:incrbyfloat("test", 0.1))
    assert(r == "123.1")
    r = yield(self.con:get("test"))
    assert(r == "123.1")
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
    assert(r == "Hello!")
    r = yield(self.con:llen("mylist"))
    assert(r == 1)
    r = yield(self.con:lpop("mylist"))
    assert(r == "Hello!")
    r = yield(self.con:llen("mylist"))
    assert(r == 0)
    r = yield(self.con:lset("mylist", 0, "Hello!"))
    assert(not r)
    r = yield(self.con:rpush("mylist", "Hello", "World"))
    assert(r == 2)
    r = yield(self.con:rpushx("mylist", "!"))
    assert(r == 3)
    r = yield(self.con:rpushx("invalid", "abcdefgh"))
    assert(r == 0)
    r = yield(self.con:lpushx("invalid", "abcdefgh"))
    assert(r == 0)
    r = yield(self.con:lpop("mylist"))
    assert(r == "Hello")
    r = yield(self.con:rpop("mylist"))
    assert(r == "!")
    r = yield(self.con:lpush("mylist", "GoodBye"))
    assert(r == 2)
    r = yield(self.con:lrem("mylist", -1, "World"))
    assert(r == 1)
    for _, v in ipairs({1,2,3,4}) do -- 4,3,2,1,GoodBye
        r = yield(self.con:lpush("mylist", v))
    end
    assert(r == 5)
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
    r = yield(self.con:select("1"))
    assert(r)
    r = yield(self.con:get("to_be_moved"))
    assert(r)
end

function TestTurboRedis:test_mset()
    local r
    r = yield(self.con:mset("key1", "val1", "key2", "val2"))
    assert(r)
    r = yield(self.con:get("key1"))
    assert(r == "val1")
    r = yield(self.con:get("key2"))
    assert(r == "val2")
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
    assert(r == "val1")
    r = yield(self.con:multi())
    assert(r)
    r = yield(self.con:set("key3", "val3"))
    assert(r)
    r = yield(self.con:get("key3"))
    assert(r)
    r = yield(self.con:discard())
    assert(r)
    r = yield(self.con:get("key3"))
    assert(r == nil)
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


function runtests()
    LuaUnit:run()
    turbo.ioloop.instance():close()
end


turbo.ioloop.instance():add_callback(runtests)
turbo.ioloop.instance():start()

