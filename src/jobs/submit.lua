-- Redis serializes this whole transition. Only this script creates submission
-- identities; workers must not initialize jobs themselves. No expiry is used.
local submissions, sequence, active = KEYS[1], KEYS[2], KEYS[3]
local prefix, base = ARGV[1], ARGV[2]

local function key_type(key)
    return redis.call('TYPE', key).ok
end
local function compatible(key, expected)
    local kind = key_type(key)
    return kind == 'none' or kind == expected
end
local function job_keys(id)
    local root = prefix .. ':job:' .. id
    return {root .. ':meta', root .. ':seen', root .. ':frontier',
            root .. ':in-flight', root .. ':stats', root .. ':extensions'}
end
local function valid_id(id)
    return id and string.match(id, '^[1-9]%d*$') and
        (#id < 19 or (#id == 19 and id <= '9223372036854775807'))
end

-- Lua script errors do not roll back earlier writes. Validate shared key types
-- before changing anything; malformed/orphan state is an error, not a reset.
if not compatible(submissions, 'hash') or not compatible(sequence, 'string') or
   not compatible(active, 'set') then
    return {'invalid'}
end
local existing = redis.call('HGET', submissions, base)
if existing then
    if not valid_id(existing) then return {'invalid'} end
    local meta = job_keys(existing)[1]
    if key_type(meta) ~= 'hash' or redis.call('HGET', meta, 'schema') ~= '1' or
       redis.call('HGET', meta, 'base') ~= base then
        return {'invalid'}
    end
    return {'existing', existing}
end

local previous = redis.call('GET', sequence)
if previous then
    if previous ~= '0' and not valid_id(previous) then return {'invalid'} end
    if previous == '9223372036854775807' then return {'exhausted'} end
end
-- INCR's Lua number would lose precision above 2^53. Ignore that reply and GET
-- the exact decimal string instead. An orphan-key error below can leave an ID
-- gap, but never a submission identity or a partly initialized runnable job.
redis.call('INCR', sequence)
local id = redis.call('GET', sequence)
local keys = job_keys(id)
for _, key in ipairs(keys) do
    if redis.call('EXISTS', key) ~= 0 then return {'invalid'} end
end

redis.call('HSET', keys[1], 'schema', '1', 'base', base, 'state', 'running',
           'processed', '0', 'failure', '')
redis.call('SADD', keys[2], base)
redis.call('RPUSH', keys[3], base)
redis.call('HSET', keys[5], 'num_files', '0', 'num_exts', '0', 'total_word_count', '0')
redis.call('HSET', submissions, base, id)
redis.call('SADD', active, id)
return {'created', id}
