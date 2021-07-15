local crawler_engine = ARGV[1]
local time = ARGV[2]
-- Find domain with lowest hit rate ZRANGEBYSCORE INPROCESS_DOMAINS_HITS -1 -1 WITHSCORES
local domain_hits_table = redis.call('ZRANGEBYSCORE', 'INPROCESS_DOMAINS_HITS', '0', '4','LIMIT', 0, 1)
if next(domain_hits_table) then
    
    local lowest_hit_domain = table.remove(domain_hits_table,1)
    
    
    local threadId = redis.call('LPOP', 'INQUEUE_CRAWLER_ENGINE_#'  .. crawler_engine .. '_DOMAIN_#' .. lowest_hit_domain  )
    
    if type(threadId) == 'string' then
        ---------------------------------------------------------
        -- Retrieve crawler thread
        ---------------------------------------------------------    
        local crawler_thread =  redis.call('HGET', 'INQUEUE_CRAWLER_THREAD',threadId)

        redis.call('HDEL','INQUEUE_CRAWLER_THREAD',threadId)

        ---------------------------------------------------------
        -- Move crawler_thread status from to IN_PROCESS and remove from IN_QUEUE
        ---------------------------------------------------------
        redis.call('HSET','INPROCESS_CRAWLER_THREAD',threadId, crawler_thread)

        redis.call('hincrby','INQUEUE_THREADS_BY_CRAWLER_ENGINE_CNT',crawler_engine, -1)
        
        redis.call('hincrby','INPROCESS_THREADS_BY_CRAWLER_ENGINE_CNT',crawler_engine, 1)

        -------------------------------------------------------------
        -- Increase HITS counter for Domain. Useful for rate-limiting
        -------------------------------------------------------------
        redis.call('zincrby','INPROCESS_DOMAINS_HITS', 1, lowest_hit_domain)

        redis.call('hincrby','INQUEUE_DOMAINS_CNT', lowest_hit_domain,-1)
        
        ----------------------------------------------------------------------------------
        -- Cache TTL of the threadId. Will timeout to 120 seconds and be put back in queue
        ----------------------------------------------------------------------------------
        
        redis.call('zadd','INPROCESS_THREADID_TTL', time, threadId)
        
       
        return crawler_thread
    end
end



