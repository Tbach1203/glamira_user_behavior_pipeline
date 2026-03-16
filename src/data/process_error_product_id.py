import asyncio
import random
import logging
import aiohttp
import json
import re
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)



FALLBACK_URL = "https://www.glamira.com/catalog/product/view/id/{}"

CONCURRENT_REQUESTS = 20
MAX_RETRIES = 5
TIMEOUT = aiohttp.ClientTimeout(total=20)

file_lock = asyncio.Lock()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/127.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36"
]

cookies = {
    "session_id": "pgs%3D16%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.glamira.com%2F"
}

PROXIES = [
    "http://200.174.198.32:8888",
    "http://217.76.245.80:999",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    }
def get_proxy():
    return random.choice(PROXIES)

def load_failed_products(path):
    with open(path, "r") as f:
        return [line.strip() for line in f if line.strip()]

async def remove_id_from_file(error_path, product_id):
    async with file_lock:
        with open(error_path, "r") as f:
            lines = f.readlines()

        with open(error_path, "w") as f:
            for line in lines:
                if line.strip() != str(product_id):
                    f.write(line)


async def fetch_product(session, url, product_id):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                headers=get_headers(),
                cookies=cookies, proxy=get_proxy()
            ) as response:
                status = response.status
                if status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    react_data = None
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
                                    logging.warning(f"{product_id} JSON parse error {e}")
                                break
                    if not react_data:
                        logging.warning(f"{product_id} react_data not found")
                        return None
                    logging.info(f"{product_id} SUCCESS")
                    return react_data

                # 403 → retry
                elif status == 403:
                    sleep = (2 ** attempt) + random.uniform(1, 3)
                    logging.warning(
                        f"{product_id} 403 BLOCKED → retry {attempt+1}/{MAX_RETRIES} sleep {sleep:.2f}s"
                    )
                    await asyncio.sleep(sleep)

                # 404 → skip
                elif status == 404:
                    logging.warning(f"{product_id} 404 NOT FOUND")
                    return None

                # Rate limit
                elif status == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    sleep = retry_after + random.uniform(1, 3)
                    logging.warning(f"{product_id} Rate limited sleep {sleep:.2f}s")
                    await asyncio.sleep(sleep)

                # Server error
                elif status >= 500:
                    sleep = (2 ** attempt) + random.uniform(0, 2)
                    logging.warning(f"{product_id} Server error {status} retry")
                    await asyncio.sleep(sleep)

        except asyncio.TimeoutError:
            sleep = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"{product_id} Timeout retry")
            await asyncio.sleep(sleep)

        except aiohttp.ClientError as e:
            sleep = (2 ** attempt) + random.uniform(0, 2)
            logging.warning(f"{product_id} Request error {e}")
            await asyncio.sleep(sleep)
    logging.error(f"{product_id} FAILED AFTER RETRIES")
    return None

async def worker(session, semaphore, product_id, output_file, error_path):
    async with semaphore:
        url = FALLBACK_URL.format(product_id)
        info = await fetch_product(session, url, product_id)
        if info:
            record = {
                "product_id": product_id,
                **info
            }
            async with file_lock:
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            await remove_id_from_file(error_path, product_id)
            return True
        return False

async def crawl_failed_products(product_ids, output_file, error_path):
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(
        timeout=TIMEOUT,
        connector=connector
    ) as session:
        tasks = [
            worker(session, semaphore, pid, output_file, error_path)
            for pid in product_ids
        ]
        success = 0
        for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Retrying failed products"
        ):
            result = await future
            if result:
                success += 1
    logging.info(f"Recovered {success}/{len(product_ids)} products")

def retry_failed_products(error_path, output_path):
    product_ids = load_failed_products(error_path)
    logging.info(f"{len(product_ids)} failed products loaded")
    asyncio.run(
        crawl_failed_products(product_ids, output_path, error_path)
    )