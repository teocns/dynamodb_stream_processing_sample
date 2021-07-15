from redis_cluster.functions import crawler_thread_in_queue
from db_functions import ban_tracked_url, generate_main_thread_for_crawler_process
from config import MAX_CRAWLER_PROCESS_WITHOUT_JOBS
from batch_write_items_model import BatchWriteItemsModel
from typing import List


def on_crawler_process_updated(cp, old_cp) -> List[BatchWriteItemsModel]:
    total_threads = cp.get('crawler_threads_cnt')
    done_threads = cp.get('crawler_threads_done_cnt')

    if done_threads >= total_threads:
        # Update status to completed

        batch_write_items_models = {
            'tracked_urls': BatchWriteItemsModel('tracked_urls')
        }

        crawler_process_index = cp.get('crawler_process_index')
        url_id = cp.get('url_id')
        total_scraped_jobs = cp.get('total_scraped_jobs')

        tracked_urls_update = {
            'crawler_engine': cp.get('crawler_engine'),
            'total_scraped_jobs': total_scraped_jobs,
            # 'crawler_processes_done_cnt': crawler_process_index + 1
        }

        if total_scraped_jobs == 0:
            # 0 jobs scraped. Should change either crawler engine or ban URL
            if crawler_process_index >= MAX_CRAWLER_PROCESS_WITHOUT_JOBS:
                # If URL has been crawled more than threshold, it likely already tried another crawler engine so just proceed to ban the mf
                # BAN URL
                ban_tracked_url(url_id)
            else:
                tracked_urls_update['crawler_engine'] = 'SPIDER' if cp.get(
                    'crawler_engine') == "SCRAPER" else 'SCRAPER'
                # Retry with SPIDER

            # Get domains statistics for process

        return batch_write_items_models


def on_crawler_process_inserted(cp) -> List[BatchWriteItemsModel]:
    # Generate crawler thread for scraping links
    generate_main_thread_for_crawler_process(
        crawler_process=cp
    )

    # # Store in redis
    # crawler_thread_in_queue(crawler_thread)

    pass
