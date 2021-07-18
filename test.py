

from mysql_db.send_completed_process import send


send(
        {
            "url_id": 753655,
            "crawler_engine": "SPIDER",
            "jobs": 10,
            "duplicates": 0,
            "links": 10,
            "bytes": 2000
        }
    )
