import asyncio
import random
import logging
import aiohttp
import pandas as pd
import json
import re
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


logging.basicConfig(level=logging.INFO)

FALLBACK_URL = "https://www.glamira.com/catalog/product/view/id/{}"

MAX_RETRIES = 5
CONCURRENT_REQUESTS = 200
TIMEOUT = aiohttp.ClientTimeout(total=10)

async def fetch_product(session, url):
    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            async with session.get(url, headers=headers) as response:
                status = response.status
                if status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    react_data = None
                    # Find script has react_data
                    scripts = soup.find_all("script", {"type": "text/javascript"})
                    for script in scripts:
                        if script.string and "var react_data" in script.string:
                            match = re.search(
                                r"var\s+react_data\s*=\s*(\{.*?\});",
                                script.string,
                                re.DOTALL
                            )
                            if match:
                                json_str = match.group(1)
                                try:
                                    react_data = json.loads(json_str)
                                except Exception as e:
                                    logging.warning(f"JSON parse error {url} {e}")
                                break
                    if not react_data:
                        logging.warning(f"react_data not found {url}")
                        return None
                    return react_data

                elif status in [403, 404]:
                    logging.warning(f"Skip {url} status={status}")
                    return None

                elif status == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    sleep = retry_after + random.uniform(1, 3)
                    logging.warning(f"Rate limited {url}, sleeping {sleep:.2f}s")
                    await asyncio.sleep(sleep)

                elif status >= 500:
                    sleep = (2**attempt) + random.uniform(0, 2)
                    logging.warning(f"Server error {status} retry {url}")
                    await asyncio.sleep(sleep)

        except asyncio.TimeoutError:
            sleep = (2**attempt) + random.uniform(0, 2)
            await asyncio.sleep(sleep)

        except aiohttp.ClientError as e:
            logging.warning(f"Request error {url} {e}")
            sleep = (2**attempt) + random.uniform(0, 2)
            await asyncio.sleep(sleep)
    return None

async def worker(session, semaphore, product_id, urls, failed_products):
    async with semaphore:
        # Try original URLs
        for url in urls:
            info = await fetch_product(session, url)
            if info:
                return {
                    "product_id": product_id,
                    **info
                }

        # Try fallback URL
        fallback_url = FALLBACK_URL.format(product_id)
        logging.info(f"Trying fallback URL for product {product_id}")
        info = await fetch_product(session, fallback_url)
        if info:
            return {
                "product_id": product_id,
                **info
            }
        
        # If still failed → log product_id
        failed_products.append(product_id)
        logging.warning(f"Product failed completely: {product_id}")
        return None

async def crawl_product(products):
    failed_products = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36"}
    async with aiohttp.ClientSession(
        timeout=TIMEOUT,
        connector=connector,
        headers=headers
    ) as session:
        tasks = [
            worker(session, semaphore, pid, urls, failed_products)
            for pid, urls in products.items()
        ]
        results = []
        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Crawling products"
        ):
            try:
                result = await future
                if result:
                    results.append(result)
            except Exception as e:
                logging.warning(f"Worker failed: {e}")
    return results, failed_products

def load_urls(json_path):
    logging.info(f"Loading urls from {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)
    
def save_results_json(results, path):
    logging.info(f"Saving results to {path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logging.info("Saved JSON successfully")

def save_failed_products(failed_products, path):
    logging.info(f"Saving failed product_ids to {path}")
    with open(path, "w") as f:
        for pid in failed_products:
            f.write(str(pid) + "\n")

def collect_product(urls_path, output_path, error_product_id_path):
    products = load_urls(urls_path)
    logging.info(f"{len(products)} products loaded")
    logging.info("Start crawling pipeline")
    results, failed_products = asyncio.run(crawl_product(products))
    logging.info("Collect finished")
    save_results_json(results, output_path)
    logging.info(f"Saved results to {output_path}")
    if failed_products:
        save_failed_products(failed_products, error_product_id_path)
        logging.info(f"{len(failed_products)} products failed")