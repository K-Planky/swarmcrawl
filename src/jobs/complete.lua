-- ARGV: job, usize maximum, worker, parent URL, base, outcome, word count,
-- extension, then zero or more canonical in-scope child URLs.
local worker, parent, base, outcome, words, extension = unpack(ARGV, 3, 8)
if data.base ~= base then return {'invalid'} end
local owner = redis.call('HGET', flight, parent)
if not owner then
    if redis.call('SISMEMBER', seen, parent) == 1 then return {'already'} end
    return {'ownership'}
end
if owner ~= worker then return {'ownership'} end
if redis.call('SISMEMBER', seen, parent) ~= 1 then return {'invalid'} end
-- Abort freezes diagnostics; late outcomes drain without failing the node.
if data.state == 'aborted' then return {'aborted'} end
if data.state == 'failed' then return {'failed', data.failure} end
if data.state ~= 'running' then return {'invalid'} end
if outcome == 'fetch' or outcome == 'statistics' or outcome == 'protocol' then
    return fail_job(outcome)
end
if outcome ~= 'file' and outcome ~= 'no-file' then return {'invalid'} end
local processed = add(data.processed, '1', u64_max)
local files, exts, total_words, extension_count = data.files, data.exts, data.words, nil
if outcome == 'file' then
    if extension == '' or not unsigned(words, u64_max) then return {'invalid'} end
    local previous = redis.call('HGET', extensions, extension)
    files = add(files, '1', usize_max)
    total_words = add(total_words, words, u64_max)
    extension_count = add(previous or '0', '1', usize_max)
    if not previous then exts = add(exts, '1', usize_max) end
end
if not processed or not files or not exts or not total_words or
   outcome == 'file' and not extension_count then
    return fail_job('statistics')
end
-- Preflight each child's collection consistency before making any writes.
for i = 9, #ARGV do
    if string.sub(ARGV[i], 1, #base) ~= base or
       (redis.call('HEXISTS', flight, ARGV[i]) == 1 and
        redis.call('SISMEMBER', seen, ARGV[i]) ~= 1) then return {'invalid'} end
end
for i = 9, #ARGV do
    if redis.call('SADD', seen, ARGV[i]) == 1 then redis.call('RPUSH', frontier, ARGV[i]) end
end
if outcome == 'file' then
    redis.call('HSET', totals, 'num_files', files, 'num_exts', exts,
               'total_word_count', total_words)
    redis.call('HSET', extensions, extension, extension_count)
end
redis.call('HDEL', flight, parent)
redis.call('HSET', meta, 'processed', processed)
if redis.call('LLEN', frontier) == 0 and redis.call('HLEN', flight) == 0 then
    redis.call('HSET', meta, 'state', 'done')
    redis.call('SREM', active, job)
    return {'published', 'done'}
end
return {'published', 'running'}
