local threadId = ARGV[1]
local domain = ARGV[2]
local crawler_engine = ARGV[3]


------------------------------------------------------------------------
-- Remove crawler_thread from IN_PROCESS
------------------------------------------------------------------------



redis.call('HDEL','INPROCESS_CRAWLER_THREAD',threadId)




redis.call('hincrby','INPROCESS_THREADS_BY_CRAWLER_ENGINE_CNT',crawler_engine, -1)

------------------------------------------------------------------------
-- Decrease HITS counter for Domain. Useful for rate-limiting
------------------------------------------------------------------------
local current_domain_hits = tonumber(redis.call('zincrby','INPROCESS_DOMAINS_HITS', -1, domain))
local domain_threads_in_queue = tonumber(redis.call('hget','INQUEUE_DOMAINS_CNT', domain))

if current_domain_hits == 0 and domain_threads_in_queue == 0 then
    
    -- Set to -1 if there's no more queued items. Will prevent retrieval of threads from this domain.
    redis.call('zadd','INPROCESS_DOMAINS_HITS', -1, domain)
end

------------------------------------------------------------------------
------------------------------------------------------------------------
-- De-Cache TTL of the threadId. Will timeout to 120 seconds and be put back in queue
------------------------------------------------------------------------

redis.call('zrem','INPROCESS_THREADID_TTL', threadId)




------------------------------------------------------------------------
-- Decrease autoscaling capacity requirements
------------------------------------------------------------------------
local score  = tonumber(redis.call('hget','CRAWLING_CAPACITY_FORECAST', crawler_engine))
if score and score > 0 then
    redis.call('hincrby','CRAWLING_CAPACITY_FORECAST', crawler_engine, -1)
end




