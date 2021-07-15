from mysql_db.send_completed_process import do


while input() != None:
    do(
        {
            "url_id": 753655,
            "crawler_engine": "SPIDER",
            "scraped_jobs": 10,
            "duplicates": 0,
            "scraped_links": 10,
            "bytes_transferred": 2000
        }
    )
