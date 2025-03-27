"""
Web Scraper for Snowflake Documentation Sections
===============================================
This script scrapes multiple sections of the Snowflake documentation site,
extracts navigation data from the 'tree' structure, and uploads the results
to a Snowflake database using Snowpark.
Key features:
- Scrapes multiple URLs using Spider API
- Extracts hierarchical page data into flat lists
- Stores navigation data and Markdown content in Snowflake tables
"""

from spider import Spider
import logging
from dotenv import load_dotenv
import os
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load environment variables (e.g., API key and Snowflake credentials)
load_dotenv() #DD

# Configure logging for tracking execution
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Snowflake connection parameters

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", 'SZNCRNI-SNOWFLAKEAGENT')
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", 'SERVICE_ACCOUNT_SCRAPY')
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", 'SNOWFLAKE_DOCUMENTATION')
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", 'STAGING')


# Load the private key from your PEM file
with open("private_key.p8", "rb") as key_file:
    private_key_obj = serialization.load_pem_private_key(
        key_file.read(),
        password=b'zrgtg3!',
        backend=default_backend()
    )

# Convert the private key to DER format (binary)
private_key_der = private_key_obj.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

sf_connection_params = {
    "account": SNOWFLAKE_ACCOUNT,
    "user": SNOWFLAKE_USER,
    "private_key": private_key_der,
    "database": SNOWFLAKE_DATABASE,
    "schema": SNOWFLAKE_SCHEMA,
}

# Create a Snowflake session using Snowpark
session = Session.builder.configs(sf_connection_params).create()

# Constants
BASE_URL = "https://docs.snowflake.com"

def extract_tree_data(tree_data, base_url=BASE_URL):
    """
    Extract navigation data from the 'tree' object into a flat list of dictionaries.
    Args:
        tree_data: The 'tree' object from the JSON response
        base_url: Base URL for concatenating hrefs
    Returns:
        List of dicts with id, url, label, type, depth, and parent_url
    """
    def process_node(node, result_list):
        if not isinstance(node, dict):
            return 
        
        if all(key in node for key in ['id', 'href', 'label', 'type', 'depth', 'parentRef']):
            entry = {
                'id': node['id'],
                'url': base_url + node['href'],
                'label': node['label'],
                'type': node['type'],
                'depth': node['depth'],
                'parent_url': base_url + node['parentRef']
            }
            result_list.append(entry)
        
        if 'children' in node and isinstance(node['children'], list):
            for child in node['children']:
                process_node(child, result_list)
    
    result = []
    if 'children' in tree_data and isinstance(tree_data['children'], list):
        for child in tree_data['children']:
            process_node(child, result)
    return result

def setup_snowflake_tables(session):
    # Log the current database and schema
    current_db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    current_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    logger.info(f"Current database: {current_db}, Current schema: {current_schema}")

    # Explicitly set the database and schema
    session.sql("USE DATABASE SNOWFLAKE_DOCUMENTATION").collect()
    session.sql("USE SCHEMA STAGING").collect()
    logger.info("Set context to SNOWFLAKE_DOCUMENTATION.STAGING")

    # Drop the table to start fresh (optional, for debugging)
    session.sql("DROP TABLE IF EXISTS TEST_SETUP").collect()
    logger.info("Dropped TEST_SETUP table if it existed")

    # Create the table
    session.sql("""
        CREATE TABLE IF NOT EXISTS TEST_SETUP (
            ID VARCHAR,
            URL VARCHAR,
            LABEL VARCHAR,
            TYPE VARCHAR,
            DEPTH INTEGER,
            PARENT_URL VARCHAR,
            SECTION VARCHAR
        )
    """).collect()

    # Verify the table exists
    result = session.sql("SHOW TABLES LIKE 'TEST_SETUP' IN SNOWFLAKE_DOCUMENTATION.STAGING").collect()
    if result:
        logger.info(f"Table TEST_SETUP created successfully: {result}")
    else:
        logger.error("Table TEST_SETUP was not created!")
    
    logger.info("Ensured TEST_SETUP table exists")

def upload_to_snowflake(session, tree_data, section_name):
    """
    Upload extracted tree data and Markdown content to Snowflake.
    Args:
        session: Snowpark session object
        tree_data: List of dictionaries containing tree data
        markdown: Markdown content string (or None)
        section_name: Identifier for the section (e.g., 'guides')
    """
    # Add section identifier to tree data
    for row in tree_data:
        row['section'] = section_name
    
    # Upload tree data
    if tree_data:
        df = session.create_dataframe(tree_data)
        df.write.mode("append").save_as_table("TEST_SETUP")
        logger.info(f"Uploaded {len(tree_data)} rows to TEST_SETUP for section: {section_name}")
    

def scrape_and_process(url, api_key, section_name):
    """
    Scrape a Snowflake documentation section and upload data to Snowflake.
    Args:
        url: URL to scrape
        api_key: Spider API key
        section_name: Identifier for the section (e.g., 'guides')
    """
    # Initialize Spider client
    client = Spider(api_key=api_key)
    
    # Scrape parameters
    crawler_params = {
        'proxy_enabled': True,
        'store_data': False,
        'metadata': True,
        'request': 'markdown',
        'return_page_links': True,
        'return_json_data': True,
        'readability': True,
    }
    
    try:
        # Perform the scrape
        logger.info(f"Scraping {url}")
        result_list = client.scrape_url(url, params=crawler_params)
        if not result_list or not isinstance(result_list, list):
            raise ValueError("Unexpected response format: Expected a list")
        result = result_list[0]
        
        # Extract tree data
        tree_location = (
            result.get('json_data', {}).get('other_scripts', [{}])[0]
            .get('props', {}).get('pageProps', {}).get('tree')
        )
        tree_data = extract_tree_data(tree_location) if tree_location else []
        
        # Upload to Snowflake
        upload_to_snowflake(session, tree_data, section_name)
        
        logger.info(f"Completed processing {url}")
    
    except Exception as e:
        logger.error(f"Error during scraping or uploading {url}: {str(e)}")
        raise

if __name__ == "__main__":
    # Entry point: Scrape multiple Snowflake documentation sections
    api_key = os.getenv("SPIDER_CLOUD_KEY")
    if not api_key:
        raise ValueError("SPIDER_CLOUD_KEY not found in environment variables")
    
    # Ensure Snowflake tables exist
    setup_snowflake_tables(session)
    
    # Define sections to scrape with their identifiers
    sections = [
        {"url": "https://docs.snowflake.com/en/guides", "basename": "guides"},
        {"url": "https://docs.snowflake.com/en/developer", "basename": "developer"},
        {"url": "https://docs.snowflake.com/en/reference", "basename": "reference"},
        {"url": "https://docs.snowflake.com/en/release-notes/overview", "basename": "releases"},
    ]
    
    # Iterate through sections and scrape each one
    for section in sections:
        scrape_and_process(section["url"], api_key, section["basename"])
    
    # Close the Snowpark session
    session.close()
    print("Scraping and uploading completed for all sections.")