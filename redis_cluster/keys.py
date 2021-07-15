
CRAWLER_THREADS_LIST = "CRAWLER_THREADS"  # : [threadId]


# {
#    "SCRAPER": [threadId],
#    "SPIDER": [threadId]
# }

CRAWLER_THREADS_PK_CRAWLER_ENGINE = "CRAWLER_THREADS_PK_CRAWLER_ENGINE"


# {
#    "www.google.com": [threadId],
#    "www.foobar.com": [threadId]
# }

CRAWLER_THREADS_PK_DOMAIN = "CRAWLER_THREADS_PK_DOMAIN"


# {
#    "www.google.com": [threadId],
#    "www.foobar.com": [threadId]
# }

CRAWLER_THREAD_DOMAIN = "CRAWLER_THREAD_DOMAIN"


# {
#    "threadId": DOMAIN,
# }

INPROCESS_THREADID_DOMAIN = "THREADID_DOMAIN"


# {
#    "#threadId": [threadId],
#    "#threadId": [threadId]
# }

CRAWLER_THREADS_IN_PROGRESS = "CRAWLER_THREADS_IN_PROGRESS"


# used with Z ranges

INPROCESS_DOMAINS_HITS = "INPROCESS_DOMAINS_HITS"


# Score list
# {
#   "threadId": "timestamp"
# }
INPROCESS_THREADID_TTL = "THREADS_IN_PROCESS_TTL"


# To be called at every retrieval and put
# LPUSH / LPOP
READY_CRWLER_ENGINE_THREADID = "READY_DOMAIN_THREADID"


# {
# 			"SCRAPER#DOMAIN": [
# 				{threadId}
# 			],
# 			"SPIDER#DOMAIN": [
# 				threadId
# 			]
# 	}

CRAWLER_ENGINE_DOMAIN_THREADID = "CRAWLER_ENGINE_DOMAIN_THREADID"


# Ready crawler threads count, grouped by their crawler engine
READY_THREADS_CRAWLER_ENGINES_COUNTER = "READY_THREADS_CRAWLER_ENGINES_COUNTER"


# {
#    INQUEUE_DOMAINS_THREADSCNT: {
#           "foo.com": 13
#    }
# }
INQUEUE_DOMAINS_THREADSCNT = "INQUEUE_DOMAINS_THREADSCNT"


# After domain rate-limit implied
# {
#   "spider": 0,
#   "scraper": 1
# }
CRAWLING_CAPACITY_FORECAST = ""
