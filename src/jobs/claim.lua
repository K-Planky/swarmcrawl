-- ARGV: job, usize maximum, worker identity.
if data.state ~= 'running' then return {'idle'} end
local url = redis.call('LINDEX', frontier, 0)
if not url then return {'idle'} end -- Owned work can still publish new children.
if string.sub(url, 1, #data.base) ~= data.base or
   redis.call('SISMEMBER', seen, url) ~= 1 or redis.call('HEXISTS', flight, url) ~= 0 then
    return {'invalid'}
end
redis.call('LPOP', frontier)
redis.call('HSET', flight, url, ARGV[3])
return {'claimed', data.base, url}
