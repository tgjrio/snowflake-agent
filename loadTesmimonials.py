import os
import json
import logging
from dotenv import load_dotenv
from spider import Spider
import math

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

api_key = os.getenv("SPIDER_CLOUD_KEY")
if not api_key:
    print("SPIDER_CLOUD_KEY not set in environment variables.")
    exit(1)

# Initialize Spider client
client = Spider(api_key=api_key)

# Scrape parameters (maximize content capture)
crawler_params = {
    "proxy_enabled": True,
    "anti_bot": True,
    "return_json_data": True,
    'return_page_links': True,
    'request': 'smart',
    "return_format" : "raw",
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.snowflake.com/en/customers",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br"
    },
}

# Initial scrape to get totalMatches
initial_url = "https://www.snowflake.com/en/customers/all-customers/"
try:
    initial_response = client.scrape_url(initial_url, params=crawler_params)
    with open("response.json", "w", encoding="utf-8") as f:
        json.dump(initial_response, f, indent=4)
    logger.info("Initial scrape successful")
except Exception as e:
    print(f"Initial scrape failed: {e}")
    print("Check your API key and ensure you have sufficient credits.")
    exit(1)

# Extract totalMatches from json_data
json_data = initial_response[0].get("json_data", {})
try:
    total_matches = (
        json_data
        .get("other_scripts", [{}])[0]
        .get("rootModel", {})
        .get(":children", {})
        .get("/content/snowflake-site/global/en/customers/all-customers", {})
        .get(":items", {})
        .get("root", {})
        .get(":items", {})
        .get("responsivegrid", {})
        .get(":items", {})
        .get("container", {})
        .get(":items", {})
        .get("filterable_resources", {})
        .get("initialHits", {})
        .get("totalMatches", 0)
    )
except (KeyError, IndexError) as e:
    logger.error(f"Error navigating json_data: {e}")
    total_matches = 0

# Calculate total pages (12 articles per page)
articles_per_page = 12
total_pages = math.ceil(total_matches / articles_per_page) if total_matches > 0 else 1
logger.info(f"Total matches: {total_matches}, Total pages: {total_pages}")

# Generate paginated URLs
base_url = "https://www.snowflake.com/en/customers/all-customers/"
paginated_urls = [
    f"{base_url}?page={page}&pageSize={articles_per_page}&offset={page * articles_per_page}"
    for page in range(total_pages)
]

# Batch URLs and scrape, storing links as list of arrays
batch_size = 5
all_links_arrays = []  # List of arrays, one per batch

for i in range(0, len(paginated_urls), batch_size):
    batch_urls = paginated_urls[i:i + batch_size]
    concatenated_batch = ",".join(batch_urls)
    try:
        response_list = client.scrape_url(concatenated_batch, params=crawler_params)
        logger.info(f"Scraped batch {i // batch_size + 1} of {math.ceil(len(paginated_urls) / batch_size)}")
        batch_links = []
        for response in response_list:
            links = response.get("links", [])
            batch_links.extend(links)
        all_links_arrays.append(batch_links)  # Add batch links as an array
        logger.info(f"Batch links count: {len(batch_links)}, URLs: {batch_urls}")
    except Exception as e:
        logger.error(f"Batch scrape failed for URLs {concatenated_batch}: {e}")
        continue

# Flatten all links for filtering and deduplication
all_links_flat = [link for batch in all_links_arrays for link in batch]
# unique_links = list(set(all_links_flat))  # Deduplicate

# # Filter links for video and case-study
# filtered_links = [
#     link for link in all_links_flat 
#     if "all-customers/case-study/" in link or "all-customers/video/" in link
# ]

# Save all links arrays
with open("all_links_arrays.json", "w", encoding="utf-8") as f:
    json.dump(all_links_arrays, f, indent=4)

# Save filtered links
with open("filtered_paginated_customer_links.json", "w", encoding="utf-8") as f:
    json.dump(all_links_flat, f, indent=4)

logger.info(f"Total unique links: {len(all_links_flat)}, Filtered links: {len(all_links_flat)}")
logger.info("All links arrays saved to all_links_arrays.json")
logger.info("Filtered links saved to filtered_paginated_customer_links.json")
print("Scraping completed.")