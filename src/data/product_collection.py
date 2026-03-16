import asyncio
import random
import logging
import aiohttp
import json
import re
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


logging.basicConfig(level=logging.INFO)

FALLBACK_URL = "https://www.glamira.com/catalog/product/view/id/{}"

MAX_RETRIES = 5
CONCURRENT_REQUESTS = 200
TIMEOUT = aiohttp.ClientTimeout(total=10)

BROWSER_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.118 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.60 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.2535.51",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36 Edg/124.0.2478.80",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]

PROXY_LIST = [
    "http://xugdfcgc:dan7w0ydvmmh@31.59.20.176:6754",
    "http://xugdfcgc:dan7w0ydvmmh@23.95.150.145:6114",
    "http://xugdfcgc:dan7w0ydvmmh@198.23.239.134:6540",
    "http://xugdfcgc:dan7w0ydvmmh@45.38.107.97:6014",
    "http://xugdfcgc:dan7w0ydvmmh@107.172.163.27:6543",
    "http://xugdfcgc:dan7w0ydvmmh@198.105.121.200:6462",
    "http://xugdfcgc:dan7w0ydvmmh@64.137.96.74:6641",
    "http://xugdfcgc:dan7w0ydvmmh@216.10.27.159:6837",
    "http://xugdfcgc:dan7w0ydvmmh@142.111.67.146:5611",
    "http://xugdfcgc:dan7w0ydvmmh@191.96.254.138:6185"
]


def get_random_client():
    browser = random.choice(BROWSER_LIST)
    proxy = random.choice(PROXY_LIST)
    headers = {
        "User-Agent": browser
    }
    return proxy, headers

async def fetch_product(session, url):
    for attempt in range(MAX_RETRIES):
        proxy, headers = get_random_client()
        try:
            async with session.get(
                url,
                proxy=proxy,
                headers=headers
            ) as response:
                status = response.status
                if status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    react_data = None
                    scripts = soup.find_all(
                        "script",
                        {"type": "text/javascript"}
                    )
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
                                    logging.warning(
                                        f"JSON parse error {url} {e}"
                                    )
                            break
                    if not react_data:
                        logging.warning(f"react_data not found {url}")
                        return None
                    return react_data

                # 403  
                elif status == 403:
                    sleep = 30 + random.uniform(2, 5)
                    logging.warning(f"403 blocked {url} retry {attempt+1} sleep {sleep:.2f}")
                    await asyncio.sleep(sleep)

                # rate limit
                elif status == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    sleep = retry_after + random.uniform(1, 3)
                    logging.warning(f"Rate limited {url}, sleeping {sleep:.2f}s")
                    await asyncio.sleep(sleep)
                # server error 
                elif status >= 500:
                    sleep = (2 ** attempt) + random.uniform(0, 2)
                    logging.warning(f"Server error {status} retry {url}")
                    await asyncio.sleep(sleep)
                # 404
                elif status == 404:
                    logging.warning(f"Skip {url} status=404")
                    return None
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
        for url in urls:
            info = await fetch_product(session, url)
            if info:
                return {
                    "product_id": product_id,
                    **info
                }
        # fallback url
        fallback_url = FALLBACK_URL.format(product_id)
        logging.info(f"Trying fallback URL for product {product_id}")
        info = await fetch_product(session, fallback_url)
        if info:
            return {
                "product_id": product_id,
                **info
            }
        # failed
        failed_products.add((product_id, urls))
        logging.warning(f"Product failed completely: {product_id}")
        return None

async def crawl_product(products, output_path):
    failed_products = set()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(timeout=TIMEOUT,connector=connector) as session:
        tasks = [
            worker(session, semaphore, pid, urls, failed_products)
            for pid, urls in products.items()
        ]
        with open(output_path, "w", encoding="utf-8") as f:
            for future in tqdm(
                asyncio.as_completed(tasks),
                total=len(tasks),
                desc="Crawling products"
            ):
                try:
                    result = await future
                    if result:
                        f.write(json.dumps(result,ensure_ascii=False) + "\n")
                except Exception as e:logging.warning(f"Worker failed: {e}")
    return list(failed_products)

def load_urls(jsonl_path):
    logging.info(f"Loading urls from {jsonl_path}")
    products = {}
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line.strip())
            pid = record["product_id"]
            urls = record["urls"]
            products[pid] = urls
    logging.info(f"Loaded {len(products)} products")
    return products

def save_error_products(failed_products, path):

    logging.info(f"Saving failed products to {path}")
    with open(path, "w", encoding="utf-8") as f:
        for pid, urls in failed_products:
            record = {
                "product_id": pid,
                "urls": urls
            }
            f.write(json.dumps(record) + "\n")

def collect_product(urls_path, output_path, failed_path):
    products = load_urls(urls_path)
    logging.info(f"{len(products)} products loaded")
    logging.info("Start crawling pipeline")
    failed_products = asyncio.run(crawl_product(products, output_path))
    logging.info("Collect finished")
    if failed_products:
        save_error_products(failed_products,failed_path)
        logging.info(f"{len(failed_products)} products failed completely")