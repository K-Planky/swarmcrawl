-- Allocate a namespace-local process identity, not a heartbeat or recovery lease.
local key = KEYS[1]
local kind = redis.call('TYPE', key).ok
if kind ~= 'none' and kind ~= 'string' then return {'invalid'} end
local previous = redis.call('GET', key) or '0'
if not string.match(previous, '^%d+$') or
   (#previous > 1 and string.sub(previous, 1, 1) == '0') or
   #previous > 19 then return {'invalid'} end
local maximum = '9223372036854775807'
if #previous == 19 and previous > maximum then return {'invalid'} end
if previous == maximum then return {'exhausted'} end
redis.call('INCR', key)
-- Read the string instead of converting Lua's potentially rounded INCR reply.
return {'worker', 'node-' .. redis.call('GET', key)}
