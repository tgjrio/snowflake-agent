import os
import json
from dotenv import load_dotenv
from spider import Spider  # Assuming this is the correct import for Spider client

# Load environment variables
load_dotenv()

# Get API key from environment
api_key = os.getenv("SPIDER_CLOUD_KEY")
if not api_key:
    print("SPIDER_CLOUD_KEY not set in environment variables.")
    exit(1)

# Initialize Spider client
client = Spider(api_key=api_key)

# Scrape parameters
crawler_params = {
    "request": "empty",  # Capture full HTML content
    "proxy_enabled": True,
    "anti_bot": True,
    "return_json_data": True,
    # "return_page_links": True,
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://docs.snowflake.com/",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br"
    },
}

# URL to scrape
url = "https://docs.snowflake.com/en/user-guide/data-load-s3"

# Perform the scrape
try:
    response = client.scrape_url(url, params=crawler_params)
    print("Scrape successful")
except Exception as e:
    print(f"Scrape failed: {e}")
    print("Check your API key and ensure you have sufficient credits.")
    exit(1)

# Save the response locally
output_file = "bama_video_response.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(response, f, indent=4)

print(f"Response saved to {output_file}")