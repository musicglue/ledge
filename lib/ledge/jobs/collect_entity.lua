local pairs, unpack = pairs, unpack
local tbl_insert = table.insert


local _M = {
    _VERSION = '0.01',
}


-- Cleans up expired items and keeps track of memory usage.
function _M.perform(job)
    local redis = job.redis
    if not redis then
        return nil, "job-error", "no redis connection provided"
    end

    local ok = redis:multi()

    local res, err
    res, err = redis:zrem(job.data.body_key_chain.keys, job.data.cache_key_chain.key)
    if not res then return nil, "redis-error-zrem-1", err end
    res, err = redis:zrem(job.data.cache_key_chain.bodies, job.data.body_key_chain.body)
    if not res then return nil, "redis-error-zrem-2", err end

    local res, err = redis:exec()
    if not res then
        return nil, "redis-error-transaction", err
    end

    redis:watch(job.data.body_key_chain.keys)

    local num_keys = redis:zcard(job.data.body_key_chain.keys)
    local expired_keys = redis:zrangebyscore(job.data.body_key_chain.keys, 0, ngx.time())

    redis:multi()

    if num_keys == 0 then
        job.data.body_key_chain.root = nil
        local del_keys = {}
        for k, v in pairs(job.data.body_key_chain) do
            if k ~= "root" then
                tbl_insert(del_keys, v)
            end
        end
        res, err = redis:del(unpack(del_keys))
        if not res then return nil, "redis-error-del", err end
    elseif expired_keys then
        redis:zrem(job.data.body_key_chain.keys, unpack(expired_keys))
    end

    local res, err = redis:exec()
    if res then
        return true, nil
    else
        return nil, "redis-error", err
    end
    
    --[[
    local del_keys = {}
    for _, key in pairs(job.data.entity_keys) do
        tbl_insert(del_keys, key)
    end

    local res, err = redis:del(unpack(del_keys))
    ]]--

    -- Decrement the integer value of a key by the given number, only if the key exists,
    -- and only if the current value is a positive integer.
    -- Params: key, decrement
    -- Return: (integer): the value of the key after the operation.
    local POSDECRBYX = [[
        local value = redis.call("GET", KEYS[1])
        if value and tonumber(value) > 0 then
            return redis.call("DECRBY", KEYS[1], ARGV[1])
        else
            return 0
        end
        ]]

    --res, err = redis:eval(POSDECRBYX, 1, job.data.cache_key_chain.memused, job.data.size)
--    res, err = redis:zrem(job.data.cache_key_chain.entities, job.data.entity_keys.main)

  --  res, err = redis:exec()

end


return _M

