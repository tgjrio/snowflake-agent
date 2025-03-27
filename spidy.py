
from spider import Spider
import os 
from datetime import datetime
import json
import logging
import pprint
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SPIDER_CLOUD_KEY="sk-ce5e6935-7e74-49f8-affc-a90c55dad44a"
client = Spider(api_key=SPIDER_CLOUD_KEY)

crawler_params = {
        'proxy_enabled': True,
        'store_data': False,
        'metadata': False,
        'request': 'empty',
        'return_page_links': True,
        'return_json_data': True,
        'readability': True,
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://docs.snowflake.com",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br"
        }
    }

url = 'https://docs.snowflake.com/en/user-guide/snowflake-horizon'


result = client.scrape_url(url, params=crawler_params)

pprint(result, indent=2)