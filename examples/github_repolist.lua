TURBO_SSL = true -- Enable SSL since we are querying a https:// url.
local turbo = require("turbo")
local turboredis = require("turboredis")
local yield = coroutine.yield
local GITHUB_URL = "https://api.github.com/users/%s/repos"

-- Create a new connection to Redis
local redis = turboredis.Connection:new({host="127.0.0.1", port=6379})

local GitHubRepoListHandler = class("GitHubRepoListHandler",
                                    turbo.web.RequestHandler)

function GitHubRepoListHandler:get(username)
    -- Set the async option to force Turbo to not finish up
    -- the request before we say so.
    self:set_async(true)

    local cache_key = "github_repolist_" + username

    -- Try to retrieve the response from cache.
    local r = yield(redis:get(cache_key))

    -- If the repo list was found in cache, write it to the client
    -- and return
    if r then
        local repolist = turbo.escape.json_decode(r)
        self:write_repolist(repolist)
        self:write("<br><b>cach hit<b>")
        -- Get the remaining time-to-live for the Redis key
        redis:ttl(cache_key, function (ttl)
            self:write(string.format("<br><b>ttl: %d", ttl)) 
            self:finish()
        end)
        return
    end

    -- If the repo list wasn't cached, fetch it from GitHub
    local res = yield(turbo.async.HTTPClient():fetch(
        string.format(GITHUB_URL, username), {verify=true}))

    local status_code = res.headers:get_status_code()
    if status_code ~= 200 then
        if status_code == 404 then -- Not found
            self:write(string.format("User '%s' not found", username))
            return
        else -- Other error
            error(turbo.web.HTTPError:new(500, res.error.message))
        end
    end

    local repolist = turbo.escape.json_decode(res.body)
    self:write_repolist(repolist)
    self:write("<br><b>cache miss<b>")
    self:finish()

    -- Set the key github_repolist_<USERNAME> to the response
    -- body string and set it to expire in 10 seconds
    r = yield(redis:setex(cache_key, 10, res.body))
    if not r then
        -- We should never get here.
        turbo.log.warning("Could not cache response body from github")
    end
end

-- Writes a repo list to client as an HTML unordered list with the repo
-- name and what github consideres the primary language used in the repo.
function GitHubRepoListHandler:write_repolist(repolist)
    self:write("<h1>Repositories</h1><ul>")
    for i, repo in ipairs(repolist) do
        self:write(string.format([[<li><a href="%s">%s(%s)</a></li>]],
            repo.url, repo.name, repo.language))
    end
    self:write("</ul>")
end

local app = turbo.web.Application({
    {"^/(.-)$", GitHubRepoListHandler}
}):listen(8888)

turbo.ioloop.instance():add_callback(function ()
    yield(redis:connect())
end)
turbo.ioloop.instance():start()

turbo.ioloop.instance():start()
