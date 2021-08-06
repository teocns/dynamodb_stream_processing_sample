from functions import update_tracked_url_after_completion
# from redis_cluster.functions import crawler_thread_in_queue
from functions import generate_main_thread_for_crawler_process
from mysql_db.send_completed_process import send as send_completed_process_to_mysql
from config import MAX_CRAWLER_PROCESS_WITHOUT_JOBS
#from batch_write_items_model import BatchWriteItemsModel
from typing import List


def on_crawler_process_updated(cp, old_cp):
    done_threads = cp.get('threads_done_cnt')

    

    links = cp.get('links')
    duplicates = cp.get('duplicates')

    # Duplicates won't get scraped, and we add +1 for the scrape(LINKS) action
    real_threads_count = links - duplicates + 1
    print('Checking if should update')
    print('real_threads_count: %s, done_threads: %s' % (str(real_threads_count), str(done_threads)))
    if done_threads >= real_threads_count:
        update_tracked_url_after_completion(cp)
        
        #send_completed_process_to_mysql(cp)
        # Update status to completed

        # batch_write_items_models = {
        #     'tracked_urls': BatchWriteItemsModel('tracked_urls')
        # }

        # crawler_process_index = cp.get('crawler_process_index')
        # url_id = cp.get('url_id')
        # total_scraped_jobs = cp.get('total_scraped_jobs')

        # tracked_urls_update = {
        #     'crawler_engine': cp.get('crawler_engine'),
        #     'total_scraped_jobs': total_scraped_jobs,
        #     # 'crawler_processes_done_cnt': crawler_process_index + 1
        # }

        # if total_scraped_jobs == 0:
        #     # 0 jobs scraped. Should change either crawler engine or ban URL
        #     if crawler_process_index >= MAX_CRAWLER_PROCESS_WITHOUT_JOBS:
        #         # If URL has been crawled more than threshold, it likely already tried another crawler engine so just proceed to ban the mf
        #         # BAN URL
        #         ban_tracked_url(url_id)
        #     else:
        #         tracked_urls_update['crawler_engine'] = 'SPIDER' if cp.get(
        #             'crawler_engine') == "SCRAPER" else 'SCRAPER'
        #         # Retry with SPIDER

        #     # Get domains statistics for process

        # return batch_write_items_models


def on_crawler_process_inserted(cp):
    # Generate crawler thread for scraping links
    generate_main_thread_for_crawler_process(
        crawler_process=cp
    )

    # # Store in redis
    # crawler_thread_in_queue(crawler_thread)

    pass
