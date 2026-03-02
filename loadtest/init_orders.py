import asyncio
import json
import logging
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S',
)
logger = logging.getLogger(__name__)

NUMBER_OF_ITEMS = 100_000
ITEM_STARTING_STOCK = 1_000_000
ITEM_PRICE = 1
NUMBER_OF_USERS = 100_000
USER_STARTING_CREDIT = 1_000_000
NUMBER_OF_ORDERS = 100_000

URLS_PATH = Path(__file__).resolve().parent / "urls.json"
with open(URLS_PATH) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


async def populate_databases():
    # Large timeout because batch initialization may take minutes with big datasets.
    timeout = aiohttp.ClientTimeout(total=3600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info("Batch creating users ...")
        url = f"{PAYMENT_URL}/payment/batch_init/{NUMBER_OF_USERS}/{USER_STARTING_CREDIT}"
        async with session.post(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to seed users: {resp.status} {await resp.text()}")
            await resp.json()

        logger.info("Batch creating items ...")
        url = f"{STOCK_URL}/stock/batch_init/{NUMBER_OF_ITEMS}/{ITEM_STARTING_STOCK}/{ITEM_PRICE}"
        async with session.post(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to seed items: {resp.status} {await resp.text()}")
            await resp.json()

        logger.info("Batch creating orders ...")
        url = f"{ORDER_URL}/orders/batch_init/{NUMBER_OF_ORDERS}/{NUMBER_OF_ITEMS}/{NUMBER_OF_USERS}/{ITEM_PRICE}"
        async with session.post(url) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to seed orders: {resp.status} {await resp.text()}")
            await resp.json()

    logger.info("Finished populating users/items/orders")


if __name__ == "__main__":
    asyncio.run(populate_databases())
