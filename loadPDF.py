import asyncio
from pyppeteer import launch
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def url_to_pdf(url, output_path):
    try:
        logger.info(f"Starting PDF conversion for {url}")
        browser = await launch(headless=True, args=['--no-sandbox'])
        page = await browser.newPage()
        await page.goto(url, {'waitUntil': 'networkidle2'})
        await page.pdf({'path': output_path})
        await browser.close()
        logger.info(f"PDF saved to {output_path}")
    except Exception as e:
        logger.error(f"Error converting {url} to PDF: {str(e)}")

asyncio.run(url_to_pdf('https://docs.snowflake.com/en/user-guide/data-load-overview', 'output.pdf'))