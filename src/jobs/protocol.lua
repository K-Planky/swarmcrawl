-- Shared preflight for worker transitions. EVAL errors do not roll back, so all
-- schema/type/ownership/arithmetic checks must precede the first mutation.
local meta, seen, frontier, flight, totals, extensions, active = unpack(KEYS)
local job, usize_max = ARGV[1], ARGV[2]
local u64_max = '18446744073709551615'

local function at_most(a, b)
    return #a < #b or (#a == #b and a <= b)
end
local function unsigned(value, maximum)
    return value and (value == '0' or string.match(value, '^[1-9]%d*$')) and
        at_most(value, maximum)
end
-- Only single digits use Lua numbers. Whole counters remain decimal strings;
-- neither Lua floating arithmetic nor signed Redis HINCRBY can hold u64 totals.
local function add(a, b, maximum)
    local i, j, carry, digits = #a, #b, 0, {}
    while i > 0 or j > 0 or carry > 0 do
        local x = i > 0 and tonumber(string.sub(a, i, i)) or 0
        local y = j > 0 and tonumber(string.sub(b, j, j)) or 0
        local sum = x + y + carry
        table.insert(digits, 1, tostring(sum % 10))
        carry = math.floor(sum / 10)
        i, j = i - 1, j - 1
    end
    local result = table.concat(digits)
    if not at_most(result, maximum) then return nil end
    return result
end
local function cardinal(command, key)
    local n = redis.call(command, key)
    -- Collection sizes above Lua's exact integer range are rejected, not rounded.
    if n > 9007199254740991 then return nil end
    return string.format('%.0f', n)
end
local function compatible(key, expected)
    local kind = redis.call('TYPE', key).ok
    return kind == 'none' or kind == expected
end
local function load_job()
    local kinds = {'hash', 'set', 'list', 'hash', 'hash', 'hash', 'set'}
    for i, key in ipairs(KEYS) do
        if not compatible(key, kinds[i]) then return nil, 'invalid' end
    end
    if redis.call('EXISTS', meta) == 0 then
        for i = 2, 6 do
            if redis.call('EXISTS', KEYS[i]) ~= 0 then return nil, 'invalid' end
        end
        return nil, 'unknown'
    end
    local base = redis.call('HGET', meta, 'base')
    local state = redis.call('HGET', meta, 'state')
    local failure = redis.call('HGET', meta, 'failure')
    local processed = redis.call('HGET', meta, 'processed')
    if redis.call('HLEN', meta) ~= 5 or redis.call('HGET', meta, 'schema') ~= '1' or
       not base or base == '' or not unsigned(processed, u64_max) then
        return nil, 'invalid'
    end
    if not ((state == 'running' or state == 'done') and failure == '' or
            state == 'failed' and (failure == 'fetch' or failure == 'statistics' or
                                  failure == 'protocol')) then
        return nil, 'invalid'
    end
    if redis.call('SISMEMBER', active, job) ~= (state == 'running' and 1 or 0) then
        return nil, 'invalid'
    end
    local files = redis.call('HGET', totals, 'num_files')
    local exts = redis.call('HGET', totals, 'num_exts')
    local words = redis.call('HGET', totals, 'total_word_count')
    if redis.call('HLEN', totals) ~= 3 or not unsigned(files, usize_max) or
       not unsigned(exts, usize_max) or not unsigned(words, u64_max) or
       not at_most(files, processed) or files == '0' and words ~= '0' then
        return nil, 'invalid'
    end
    local sum = '0'
    local counts = redis.call('HGETALL', extensions)
    for i = 1, #counts, 2 do
        local name, count = counts[i], counts[i + 1]
        if name == '' or not unsigned(count, usize_max) or count == '0' then
            return nil, 'invalid'
        end
        sum = add(sum, count, usize_max)
        if not sum then return nil, 'invalid' end
    end
    if sum ~= files or cardinal('HLEN', extensions) ~= exts then return nil, 'invalid' end
    local discovered = cardinal('SCARD', seen)
    local queued = cardinal('LLEN', frontier)
    local owned = cardinal('HLEN', flight)
    if not discovered or not queued or not owned then return nil, 'invalid' end
    local accounted = add(processed, queued, u64_max)
    accounted = accounted and add(accounted, owned, u64_max)
    if discovered == '0' or accounted ~= discovered or
       state == 'running' and queued == '0' and owned == '0' or
       state == 'done' and (queued ~= '0' or owned ~= '0') then
        return nil, 'invalid'
    end
    return {base = base, state = state, failure = failure, processed = processed,
            files = files, exts = exts, words = words}
end
local function fail_job(reason)
    -- Freeze owners/frontier/partial totals for diagnosis; already-owned work may
    -- drain its network task but cannot publish any more links or contributions.
    redis.call('HSET', meta, 'state', 'failed', 'failure', reason)
    redis.call('SREM', active, job)
    return {'failed', reason}
end
local data, error = load_job()
if not data then return {error} end
