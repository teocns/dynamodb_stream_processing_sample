local threadId = ARGV[1]
local domain = ARGV[2]
local crawler_engine = ARGV[3]
local crawler_thread = ARGV[4]
local RATE_LIMIT_PER_DOMAIN = tonumber(ARGV[5])
local time = tonumber(ARGV[6])



------------------------------------------------------------------------
-- Store crawler thread
------------------------------------------------------------------------
redis.call('hset','INQUEUE_CRAWLER_THREAD', threadId, crawler_thread)

redis.call('hincrby','INQUEUE_DOMAINS_CNT', domain, 1)


local score = redis.call('zscore','INPROCESS_DOMAINS_HITS', domain)

if score == nil or score == false or tonumber(score) == -1 then
    redis.call('zadd','INPROCESS_DOMAINS_HITS', 0, domain) 
end





-- For reporting: increase count for crawler_thread
redis.call('hincrby','INQUEUE_THREADS_BY_CRAWLER_ENGINE_CNT',crawler_engine, 1)




------------------------------------------------------------------------
-- Autoscaling pursposes, writes into CRAWLING_CAPACITY_FORECAST
------------------------------------------------------------------------

local threads_for_engine_and_domain = tonumber(
    redis.call('lpush','INQUEUE_CRAWLER_ENGINE_#' .. crawler_engine .. '_DOMAIN_#' .. domain, threadId)
)            

if threads_for_engine_and_domain <= RATE_LIMIT_PER_DOMAIN then
    -- Increase capacity requirements demand after taking in account the domain rate limiter
    redis.call('hincrby','CRAWLING_CAPACITY_FORECAST', crawler_engine, 1)
end
------------------------------------------------------------------------