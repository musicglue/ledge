local h_util = require "ledge.header_util"
local http_headers = require "resty.http_headers"

local pairs = pairs
local ipairs = ipairs
local setmetatable = setmetatable
local rawset = rawset
local rawget = rawget
local tonumber = tonumber
local tbl_concat = table.concat
local str_lower = string.lower
local str_gsub = string.gsub
local ngx_re_gmatch = ngx.re.gmatch
local ngx_re_match = ngx.re.match
local ngx_parse_http_time = ngx.parse_http_time
local ngx_http_time = ngx.http_time
local ngx_time = ngx.time
local ngx_req_get_headers = ngx.req.get_headers


local _M = {
    _VERSION = '0.3'
}

local mt = {
    __index = _M,
}

local NOCACHE_HEADERS = {
    ["Pragma"] = { "no-cache" },
    ["Cache-Control"] = {
        "no-cache",
        "no-store",
        "private",
    }
}


function _M.new()
    local body = ""
    local header = http_headers.new()
    local status = nil

    return setmetatable({   status = nil,
                            body = body,
                            header = header,
                            remaining_ttl = 0,
                            has_esi = false,
    }, mt)
end


function _M.is_cacheable(self)
    return true
end


function _M.ttl(self)
    return 86400
end


function _M.has_expired(self)
    if self.remaining_ttl <= 0 then
        return true
    end

    local cc = ngx_req_get_headers()["Cache-Control"]
    if self.remaining_ttl - (h_util.get_numeric_header_token(cc, "min-fresh") or 0) <= 0 then
        return true
    end
end


-- The amount of additional stale time allowed for this response considering
-- the current requests 'min-fresh'.
function _M.stale_ttl(self)
    -- Check response for headers that prevent serving stale
    local cc = self.header["Cache-Control"]
    if h_util.header_has_directive(cc, "revalidate") or
        h_util.header_has_directive(cc, "s-maxage") then
        return 0
    end

    local min_fresh = h_util.get_numeric_header_token(
        ngx_req_get_headers()["Cache-Control"], "min-fresh"
    ) or 0

    return self.remaining_ttl - min_fresh
end


-- Reduce the cache lifetime and Last-Modified of this response to match
-- the newest / shortest in a given table of responses. Useful for esi:include.
function _M.minimise_lifetime(self, responses)
    for _,res in ipairs(responses) do
        local ttl = res:ttl()
        if ttl < self:ttl() then
            self.header["Cache-Control"] = "max-age="..ttl
            if self.header["Expires"] then
                self.header["Expires"] = ngx_http_time(ngx_time() + ttl)
            end
        end

        if res.header["Age"] and self.header["Age"] and
            (tonumber(res.header["Age"]) < tonumber(self.header["Age"])) then
            self.header["Age"] = res.header["Age"]
        end

        if res.header["Last-Modified"] and self.header["Last-Modified"] then
            local res_lm = ngx_parse_http_time(res.header["Last-Modified"])
            if res_lm > ngx_parse_http_time(self.header["Last-Modified"]) then
                self.header["Last-Modified"] = res.header["Last-Modified"]
            end
        end
    end
end

return _M
