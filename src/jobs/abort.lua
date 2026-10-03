-- Runs after shared protocol preflight, atomically with claims/publication.
-- Keep outstanding frontier/owners and partial totals frozen for diagnostics,
-- just as for failure. An abort is not completion and cannot be resumed.
if data.state ~= 'running' then return {'unchanged'} end
redis.call('HSET', meta, 'state', 'aborted')
redis.call('SREM', active, job)
return {'aborted'}
