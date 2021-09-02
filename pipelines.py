from functions import update_tracked_url_after_completion
# from redis_cluster.functions import crawler_thread_in_queue
from functions import generate_main_thread_for_crawler_process
from mysql_db.send_completed_process import send as send_completed_process_to_mysql
from config import MAX_CRAWLER_PROCESS_WITHOUT_JOBS
#from batch_write_items_model import BatchWriteItemsModel
from typing import List


def on_crawler_process_updated(cp, old_cp):
    done_threads = cp.get('threads_done_cnt',0) or 0

    

    links = cp.get('links',0) or 0
    duplicates = cp.get('duplicates',0) or 0

    # Duplicates won't get scraped, and we add +1 for the scrape(LINKS) action
    real_threads_count = links - duplicates + 1
    
    if done_threads >= real_threads_count:
        update_tracked_url_after_completion(cp)
        


def on_crawler_process_inserted(cp):
    # Generate crawler thread for scraping links
    generate_main_thread_for_crawler_process(
        crawler_process=cp
    )
